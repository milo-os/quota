// deny-path.js
//
// Capacity-exhaustion workload. Provisions tenants with small grants so their
// buckets fill quickly, then hammers them with claims that each request a
// significant fraction of the allowance. Roughly half the claims are denied,
// half granted (as prior claims are released). Crucially, grant-path and
// deny-path latency are measured separately so we can answer: "is the deny
// path actually fast?"
//
// The round-2 allocator design skips the row-lock window for the deny case
// (no contention, just a read-and-reject), so deny p95 should be noticeably
// lower than grant p95. This script is how we verify that claim.
//
// Setup phase:
//   - Provisions TENANT_COUNT tenants with GRANT_AMOUNT=100 (small). Each
//     grant uses a single bucket so exhaustion is per-tenant.
//
// Test phase:
//   - Every VU runs a continuous create/release loop. With CLAIM_AMOUNT=10
//     and GRANT_AMOUNT=100, each tenant's bucket holds at most 10 concurrent
//     claims. Once saturated, new create attempts are denied until an older
//     claim is released.
//   - Each iteration records latency into grant_duration or deny_duration
//     depending on the claim outcome.
//
// Metrics captured:
//   - claims_granted Counter, claims_denied Counter
//   - grant_duration Trend, deny_duration Trend (separate distributions)
//   - grant_rate Rate (granted / total)
//   - http_errors Counter
//
// Thresholds:
//   - total ops/sec > 30 (combined grants + denies)
//   - deny p95 < 200ms (deny path skips row-lock contention; must stay fast)
//   - grant p95 < 800ms
//   - http_req_failed < 1% (note: 403 is not an http_req_failed)
//
// Configuration:
//   TENANT_COUNT - Number of tenants (default 50)
//   VUS          - Concurrent virtual users (default 30)
//   DURATION     - Test duration (default 60s)
//   GRANT_AMOUNT - Allowance per tenant bucket (default 100)
//   CLAIM_AMOUNT - Amount requested per claim (default 10)
//   QUOTA_API_URL - Apiserver URL (default localhost:8001)
//
// Usage:
//   k6 run -e VUS=60 -e GRANT_AMOUNT=50 deny-path.js

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

const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '50');
const VUS = parseInt(__ENV.VUS || '30');
const DURATION = __ENV.DURATION || '60s';
const GRANT_AMOUNT = parseInt(__ENV.GRANT_AMOUNT || '100');
const CLAIM_AMOUNT = parseInt(__ENV.CLAIM_AMOUNT || '10');

const claimsGranted = new Counter('claims_granted');
const claimsDenied = new Counter('claims_denied');
const httpErrors = new Counter('http_errors');
const grantRate = new Rate('grant_rate');
const grantDuration = new Trend('grant_duration', true);
const denyDuration = new Trend('deny_duration', true);
const claimDeleteDuration = new Trend('quota_claim_delete_duration', true);

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],

  scenarios: {
    deny_path: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      tags: { scenario: 'deny_path' },
    },
  },

  thresholds: {
    // Combined grant+deny throughput. The deny path is supposed to be cheap,
    // so total ops/sec should comfortably clear this bar.
    'claims_granted': ['rate>10'],
    'claims_denied': ['rate>10'],
    // The key assertion: the deny path must be FAST. No row lock contention,
    // just a plan-and-reject. p95 below 200ms validates the round-2 design.
    'deny_duration': ['p(95)<200', 'p(99)<500'],
    'grant_duration': ['p(95)<800', 'p(99)<2500'],
    'http_req_failed': ['rate<0.01'],
  },
};

const RESOURCE_TYPE = 'perf.test/deny-resource';
const DEFINITION_NAME = 'deny-perf-def';
const CLAIMING_KIND = 'DenyPathKind';

export function setup() {
  console.log(`Provisioning ${TENANT_COUNT} tenants with GRANT_AMOUNT=${GRANT_AMOUNT}, CLAIM_AMOUNT=${CLAIM_AMOUNT}`);
  console.log(`Max concurrent granted claims per tenant: ${Math.floor(GRANT_AMOUNT / CLAIM_AMOUNT)}`);
  const start = Date.now();
  let errors = 0;

  const res = createQuotaDefinition(DEFINITION_NAME, RESOURCE_TYPE, CLAIMING_KIND);
  if (res.status !== 201 && res.status !== 409) {
    errors++;
    console.error(`setup: definition failed: ${res.status} ${res.body}`);
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
    const grantName = `${consumer}-deny-grant`;
    const gRes = createQuotaGrant(ns, grantName, consumer, RESOURCE_TYPE, GRANT_AMOUNT);
    if (gRes.status !== 201 && gRes.status !== 409) {
      errors++;
      if (errors < 5) {
        console.error(`setup: grant ${ns}/${grantName} failed: ${gRes.status} ${gRes.body}`);
      }
    }
    if ((t + 1) % 50 === 0) {
      console.log(`setup: provisioned ${t + 1}/${TENANT_COUNT} tenants`);
    }
  }

  const elapsed = (Date.now() - start) / 1000;
  console.log(`setup: complete in ${elapsed}s (${errors} errors)`);
  return { provisionedAt: Date.now(), errors };
}

export default function () {
  const tenantIdx = Math.floor(Math.random() * TENANT_COUNT);
  const ns = namespaceFor(tenantIdx);
  const consumer = consumerNameFor(tenantIdx);
  const tenantTag = String(tenantIdx);

  const claimName = `deny-claim-${__VU}-${__ITER}`;

  const createRes = createQuotaClaim(ns, claimName, consumer, RESOURCE_TYPE, CLAIM_AMOUNT, claimName, CLAIMING_KIND);
  const createMs = createRes.timings.duration;

  if (createRes.status === 201) {
    claimsGranted.add(1);
    grantRate.add(1);
    grantDuration.add(createMs, { tenant: tenantTag });

    // Release the claim so another VU can either grant or deny.
    const delRes = deleteQuotaClaim(ns, claimName);
    claimDeleteDuration.add(delRes.timings.duration, { tenant: tenantTag });
    if (delRes.status !== 200 && delRes.status !== 202 && delRes.status !== 404) {
      httpErrors.add(1);
    }
  } else if (createRes.status === 403) {
    // Denied due to capacity exhaustion — the measured-latency case of
    // interest. No delete needed since nothing was allocated.
    claimsDenied.add(1);
    grantRate.add(0);
    denyDuration.add(createMs, { tenant: tenantTag });
  } else {
    httpErrors.add(1);
    grantRate.add(0);
    if (__ITER < 5) {
      console.error(`claim error tenant=${tenantIdx} status=${createRes.status}: ${createRes.body}`);
    }
  }

  check(createRes, {
    'claim processed': (r) => r.status === 201 || r.status === 403,
  });
}
