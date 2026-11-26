// multi-tenant-scaling.js
//
// Proves the postgres backend scales linearly with tenant and VU count once
// the global advisory lock is removed from the allocator. Unlike
// claim-throughput.js (which runs at a tenant pool too small to expose
// per-bucket row-level locking wins), this test provisions many tenants and
// spreads VUs across them randomly so that the dominant cost is per-bucket
// row-level CAS — not any global serialization point.
//
// Setup phase:
//   - Provisions TENANT_COUNT tenants, each with DEFINITIONS_PER_TENANT shared
//     QuotaDefinitions (one by default) and a high-allowance QuotaGrant per
//     (tenant, resource) pair so claims never run out of capacity.
//
// Test phase:
//   - Each VU picks a random tenant per iteration (no tenant stickiness),
//     creates a QuotaClaim, asserts it was Granted, then deletes it.
//
// Metrics captured:
//   - claims/sec (claims_created counter)
//   - p50/p95/p99 create latency (quota_claim_create_duration)
//   - p50/p95/p99 delete latency (quota_claim_delete_duration)
//   - per-tenant claim count distribution (per_tenant_claims, tagged by tenant)
//   - HTTP error counts (http_errors, http_req_failed)
//   - per-tenant max latency (per_tenant_max_latency_ms, tagged by tenant)
//
// Thresholds (postgres no-lock target; global-lock baseline is ~24/s):
//   - claims_created rate > 100/s
//   - http_req_failed < 1%
//   - quota_claim_create_duration p95 < 500ms, p99 < 2000ms
//
// Configuration:
//   TENANT_COUNT           - Number of tenants to provision/use (default 100)
//   VUS                    - Concurrent virtual users (default 50)
//   DURATION               - Steady-state test duration (default 60s)
//   DEFINITIONS_PER_TENANT - QuotaDefinitions each tenant uses (default 1)
//   GRANT_AMOUNT           - Allowance per (tenant, def) bucket (default 10000)
//   QUOTA_API_URL          - Apiserver URL (default localhost:8001)
//
// Usage:
//   k6 run -e TENANT_COUNT=200 -e VUS=100 -e DURATION=2m multi-tenant-scaling.js

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

const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '100');
const VUS = parseInt(__ENV.VUS || '50');
const DURATION = __ENV.DURATION || '60s';
const DEFINITIONS_PER_TENANT = parseInt(__ENV.DEFINITIONS_PER_TENANT || '1');
const GRANT_AMOUNT = parseInt(__ENV.GRANT_AMOUNT || '10000');

// Core throughput + latency metrics.
const claimsCreated = new Counter('claims_created');
const claimsDenied = new Counter('claims_denied');
const httpErrors = new Counter('http_errors');
const claimSuccessRate = new Rate('claim_success_rate');
const claimCreateDuration = new Trend('quota_claim_create_duration', true);
const claimDeleteDuration = new Trend('quota_claim_delete_duration', true);

// Distribution + fairness metrics. Tagged so they can be broken out per tenant
// in the k6 summary to detect hot or starved tenants.
const perTenantClaims = new Counter('per_tenant_claims');
const perTenantMaxLatency = new Trend('per_tenant_max_latency_ms', true);

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],

  scenarios: {
    multi_tenant: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      tags: { scenario: 'multi_tenant_scaling' },
    },
  },

  thresholds: {
    // Postgres-with-no-lock target. Global-lock baseline is ~24/s, so any
    // value above ~100/s demonstrates the scaling win. Raise this as the
    // allocator improves.
    'claims_created': ['rate>100'],
    'http_req_failed': ['rate<0.01'],
    'quota_claim_create_duration': ['p(95)<500', 'p(99)<2000'],
    'claim_success_rate': ['rate>0.99'],
  },
};

function resourceTypeFor(defIndex) {
  return `perf.test/mt-resource-${defIndex}`;
}

function definitionNameFor(defIndex) {
  return `mt-perf-def-${defIndex}`;
}

function claimingKindFor(defIndex) {
  return `MultiTenantKind${defIndex}`;
}

// setup() runs once before any VU iterations. Provision tenants, definitions,
// and grants here so the hot path measures only the claim workload.
export function setup() {
  console.log(`Provisioning ${TENANT_COUNT} tenants × ${DEFINITIONS_PER_TENANT} defs = ${TENANT_COUNT * DEFINITIONS_PER_TENANT} grants`);
  const start = Date.now();
  let errors = 0;

  // 1. Shared QuotaDefinitions.
  for (let d = 0; d < DEFINITIONS_PER_TENANT; d++) {
    const res = createQuotaDefinition(definitionNameFor(d), resourceTypeFor(d), claimingKindFor(d));
    if (res.status !== 201 && res.status !== 409) {
      errors++;
      console.error(`setup: definition ${d} failed: ${res.status} ${res.body}`);
    }
  }

  // 2. Per-tenant namespace + grants.
  for (let t = 0; t < TENANT_COUNT; t++) {
    const ns = namespaceFor(t);
    const consumer = consumerNameFor(t);
    const nsRes = ensureNamespace(ns);
    if (nsRes.status !== 201 && nsRes.status !== 409) {
      errors++;
      console.error(`setup: namespace ${ns} failed: ${nsRes.status} ${nsRes.body}`);
      continue;
    }
    for (let d = 0; d < DEFINITIONS_PER_TENANT; d++) {
      const grantName = `${consumer}-grant-${d}`;
      const res = createQuotaGrant(ns, grantName, consumer, resourceTypeFor(d), GRANT_AMOUNT);
      if (res.status !== 201 && res.status !== 409) {
        errors++;
        if (errors < 5) {
          console.error(`setup: grant ${ns}/${grantName} failed: ${res.status} ${res.body}`);
        }
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
  // Random tenant per iteration — no VU stickiness. This is the key to
  // exercising per-bucket row-level locking: each successive iteration may
  // touch a completely different bucket row, so a correctly-sharded allocator
  // should see roughly zero contention.
  const tenantIdx = Math.floor(Math.random() * TENANT_COUNT);
  const defIdx = Math.floor(Math.random() * DEFINITIONS_PER_TENANT);
  const ns = namespaceFor(tenantIdx);
  const consumer = consumerNameFor(tenantIdx);
  const resourceType = resourceTypeFor(defIdx);
  const kind = claimingKindFor(defIdx);
  const tenantTag = String(tenantIdx);

  // Claim name unique per VU+iteration to avoid collisions.
  const claimName = `mt-claim-${__VU}-${__ITER}-${defIdx}`;

  const createRes = createQuotaClaim(ns, claimName, consumer, resourceType, 1, claimName, kind);
  const createMs = createRes.timings.duration;
  claimCreateDuration.add(createMs, { tenant: tenantTag });
  perTenantMaxLatency.add(createMs, { tenant: tenantTag });

  const created = check(createRes, {
    'claim created': (r) => r.status === 201,
  });

  if (created) {
    claimsCreated.add(1);
    claimSuccessRate.add(1);
    perTenantClaims.add(1, { tenant: tenantTag });
  } else if (createRes.status === 403) {
    claimsDenied.add(1);
    claimSuccessRate.add(0);
  } else {
    httpErrors.add(1);
    claimSuccessRate.add(0);
    if (__ITER < 5) {
      console.error(`claim error tenant=${tenantIdx} status=${createRes.status}: ${createRes.body}`);
    }
  }

  if (created) {
    const delRes = deleteQuotaClaim(ns, claimName);
    claimDeleteDuration.add(delRes.timings.duration, { tenant: tenantTag });
    if (delRes.status !== 200 && delRes.status !== 202 && delRes.status !== 404) {
      httpErrors.add(1);
    }
  }
}
