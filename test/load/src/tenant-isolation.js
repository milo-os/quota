// tenant-isolation.js
//
// Verifies that one hot tenant does not block other ("cold") tenants. This is
// the direct test of row-level locking in the quota allocator: if there is
// any global contention point, cold-tenant latency will degrade in lockstep
// with the hot tenant. With proper per-bucket locking, the hot tenant may
// see high contention but cold tenants should barely notice.
//
// Scenario layout:
//   - 1 "hot" tenant receiving HOT_VUS VUs (~80% of traffic by default)
//   - (TENANT_COUNT - 1) "cold" tenants split across COLD_VUS VUs (~20%)
//   - Both scenarios run concurrently for DURATION seconds
//
// Each iteration creates a QuotaClaim and then deletes it. Claim/delete
// latency is tracked separately for hot and cold tenants via distinct
// metrics. k6 thresholds enforce the isolation contract.
//
// Isolation contract (thresholds):
//   - Cold-tenant p50 create latency < 200ms (the "didn't notice" bar)
//   - Cold-tenant p99 create latency < 500ms (tail stays bounded)
//   - Hot-tenant failures < 1% (hot still succeeds, just slower)
//   - Cold-tenant failures < 0.1% (cold must be effectively unaffected)
//
// Configuration:
//   TENANT_COUNT - Total tenants, including the 1 hot tenant (default 20)
//   HOT_VUS      - VUs hammering the hot tenant (default 40)
//   COLD_VUS     - VUs spread across cold tenants (default 10)
//   DURATION     - Duration of each scenario (default 60s)
//   GRANT_AMOUNT - Allowance per bucket (default 100000 — never runs out)
//
// Usage:
//   k6 run -e TENANT_COUNT=50 -e HOT_VUS=80 -e COLD_VUS=20 tenant-isolation.js

import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import {
  createQuotaDefinition,
  createQuotaGrant,
  createQuotaClaim,
  deleteQuotaClaim,
  ensureNamespace,
  namespaceFor,
  consumerNameFor,
} from './lib/quota-client.js';

const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '20');
const HOT_VUS = parseInt(__ENV.HOT_VUS || '40');
const COLD_VUS = parseInt(__ENV.COLD_VUS || '10');
const DURATION = __ENV.DURATION || '60s';
const GRANT_AMOUNT = parseInt(__ENV.GRANT_AMOUNT || '100000');

const HOT_TENANT_IDX = 0; // By convention, tenant 0 is "hot".

// Hot tenant metrics.
const hotClaimCreateDuration = new Trend('hot_claim_create_duration', true);
const hotClaimDeleteDuration = new Trend('hot_claim_delete_duration', true);
const hotClaimsCreated = new Counter('hot_claims_created');
const hotFailureRate = new Rate('hot_failure_rate');

// Cold tenant metrics — the ones the thresholds apply to.
const coldClaimCreateDuration = new Trend('cold_claim_create_duration', true);
const coldClaimDeleteDuration = new Trend('cold_claim_delete_duration', true);
const coldClaimsCreated = new Counter('cold_claims_created');
const coldFailureRate = new Rate('cold_failure_rate');

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],

  scenarios: {
    hot_tenant: {
      executor: 'constant-vus',
      vus: HOT_VUS,
      duration: DURATION,
      exec: 'hotTenant',
      tags: { scenario: 'hot_tenant' },
    },
    cold_tenants: {
      executor: 'constant-vus',
      vus: COLD_VUS,
      duration: DURATION,
      exec: 'coldTenant',
      tags: { scenario: 'cold_tenants' },
    },
  },

  thresholds: {
    // The isolation contract. Cold tenants must be essentially unaffected
    // by the hot-tenant contention on the allocator. If these thresholds
    // fail, there is a global contention point (advisory lock, shared
    // mutex, bottlenecked transaction, etc.) that bypasses row-level
    // bucket isolation.
    'cold_claim_create_duration': ['p(50)<200', 'p(99)<500'],
    'cold_failure_rate': ['rate<0.001'],
    'hot_failure_rate': ['rate<0.01'],
    'http_req_failed': ['rate<0.01'],
  },
};

function resourceType() {
  return 'perf.test/iso-resource';
}

function definitionName() {
  return 'iso-perf-def';
}

function claimingKind() {
  return 'IsolationKind';
}

export function setup() {
  console.log(`Provisioning ${TENANT_COUNT} tenants (1 hot + ${TENANT_COUNT - 1} cold)`);
  const start = Date.now();
  let errors = 0;

  const defRes = createQuotaDefinition(definitionName(), resourceType(), claimingKind());
  if (defRes.status !== 201 && defRes.status !== 409) {
    errors++;
    console.error(`setup: definition failed: ${defRes.status} ${defRes.body}`);
  }

  for (let t = 0; t < TENANT_COUNT; t++) {
    const ns = namespaceFor(t);
    const consumer = consumerNameFor(t);
    const nsRes = ensureNamespace(ns);
    if (nsRes.status !== 201 && nsRes.status !== 409) {
      errors++;
      console.error(`setup: namespace ${ns} failed: ${nsRes.status} ${nsRes.body}`);
      continue;
    }
    const grantName = `${consumer}-grant-iso`;
    const grantRes = createQuotaGrant(ns, grantName, consumer, resourceType(), GRANT_AMOUNT);
    if (grantRes.status !== 201 && grantRes.status !== 409) {
      errors++;
      if (errors < 5) {
        console.error(`setup: grant ${ns}/${grantName} failed: ${grantRes.status} ${grantRes.body}`);
      }
    }
  }

  console.log(`setup: complete in ${(Date.now() - start) / 1000}s (${errors} errors)`);
  return { errors };
}

// doClaim creates and deletes a QuotaClaim against the given tenant. Returns
// true on successful create. Records into hot or cold metric trends depending
// on the `isHot` flag.
function doClaim(tenantIdx, isHot) {
  const ns = namespaceFor(tenantIdx);
  const consumer = consumerNameFor(tenantIdx);
  const claimName = `iso-claim-${isHot ? 'h' : 'c'}-${__VU}-${__ITER}`;

  const createRes = createQuotaClaim(ns, claimName, consumer, resourceType(), 1, claimName, claimingKind());
  const createMs = createRes.timings.duration;
  if (isHot) {
    hotClaimCreateDuration.add(createMs);
  } else {
    coldClaimCreateDuration.add(createMs);
  }

  const ok = check(createRes, {
    'claim created': (r) => r.status === 201,
  });

  if (isHot) {
    hotFailureRate.add(ok ? 0 : 1);
  } else {
    coldFailureRate.add(ok ? 0 : 1);
  }

  if (ok) {
    if (isHot) {
      hotClaimsCreated.add(1);
    } else {
      coldClaimsCreated.add(1);
    }
    const delRes = deleteQuotaClaim(ns, claimName);
    const delMs = delRes.timings.duration;
    if (isHot) {
      hotClaimDeleteDuration.add(delMs);
    } else {
      coldClaimDeleteDuration.add(delMs);
    }
  } else if (__ITER < 3) {
    console.error(`${isHot ? 'hot' : 'cold'} claim error tenant=${tenantIdx} status=${createRes.status}: ${createRes.body}`);
  }

  return ok;
}

export function hotTenant() {
  doClaim(HOT_TENANT_IDX, true);
}

export function coldTenant() {
  // Pick a random cold tenant (any tenant index > 0). Every VU iteration hits
  // a potentially different tenant, so the cold workload is spread across all
  // non-hot buckets.
  if (TENANT_COUNT <= 1) {
    return;
  }
  const coldIdx = 1 + Math.floor(Math.random() * (TENANT_COUNT - 1));
  doClaim(coldIdx, false);
}
