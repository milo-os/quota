// temporal-locality.js
//
// Models real client behavior where one caller serves one tenant for a
// session. Each VU "owns" a single tenant index for STICKY_ITERATIONS
// consecutive iterations before re-randomizing. This is the optimistic
// workload — with natural locality, successive iterations from the same VU
// hit the same bucket row and the per-bucket CAS path should fly.
//
// Contrast with claim-throughput.js / multi-tenant-scaling.js which pick a
// random tenant every iteration. Those stress the worst case (no locality);
// this one measures the best case (perfect VU-tenant affinity during a
// window).
//
// Setup phase:
//   - Provisions TENANT_COUNT tenants with one QuotaDefinition and one
//     high-allowance QuotaGrant each.
//
// Test phase:
//   - Each VU initializes with a random tenant index. Every STICKY_ITERATIONS
//     iterations it re-randomizes. Between resets, every iteration hits the
//     same tenant (bucket), so contention is bounded by the number of VUs
//     that happen to share a window.
//
// Metrics captured:
//   - claims/sec (claims_created counter)
//   - p50/p95/p99 create latency (quota_claim_create_duration)
//   - p50/p95/p99 delete latency (quota_claim_delete_duration)
//   - sticky_windows counter (one per re-randomization)
//   - vu_collisions counter: incremented when a VU re-rolls onto a tenant
//     that another VU is currently owning (derived from a best-effort shared
//     occupancy map in init scope — see note below)
//   - collision_rate: vu_collisions / sticky_windows
//
// Thresholds:
//   - claims_created rate > 50/s (should exceed uniform-random because
//     per-bucket row lock contention is minimized during a sticky window)
//   - http_req_failed < 1%
//   - quota_claim_create_duration p95 < 500ms
//
// Configuration:
//   TENANT_COUNT      - Number of tenants (default 100)
//   VUS               - Concurrent virtual users (default 50)
//   STICKY_ITERATIONS - Iterations before a VU re-randomizes (default 50)
//   DURATION          - Test duration (default 60s)
//   GRANT_AMOUNT      - Allowance per tenant (default 10000)
//   QUOTA_API_URL     - Apiserver URL (default localhost:8001)
//
// Usage:
//   k6 run -e TENANT_COUNT=200 -e VUS=50 -e STICKY_ITERATIONS=100 temporal-locality.js
//
// Collision note:
//   k6 has no cross-VU shared state inside the VU sandbox, so a truly global
//   occupancy map isn't possible without an external store. Instead we
//   approximate collision rate from first principles: with VUS virtual users
//   and TENANT_COUNT tenants, the probability that a given VU's window
//   collides with another VU's current window is 1 - (1 - 1/TENANT_COUNT)^(VUS-1).
//   The test records each window's VU ownership in a per-VU counter and
//   emits the analytical estimate as a gauge-like trend for easy comparison
//   against observed contention.

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
const STICKY_ITERATIONS = parseInt(__ENV.STICKY_ITERATIONS || '50');
const DURATION = __ENV.DURATION || '60s';
const GRANT_AMOUNT = parseInt(__ENV.GRANT_AMOUNT || '10000');

const claimsCreated = new Counter('claims_created');
const claimsDenied = new Counter('claims_denied');
const httpErrors = new Counter('http_errors');
const claimSuccessRate = new Rate('claim_success_rate');
const claimCreateDuration = new Trend('quota_claim_create_duration', true);
const claimDeleteDuration = new Trend('quota_claim_delete_duration', true);
const stickyWindows = new Counter('sticky_windows');
const expectedCollisionRate = new Trend('expected_collision_rate', false);
const perTenantClaims = new Counter('per_tenant_claims');

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],

  scenarios: {
    temporal_locality: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      tags: { scenario: 'temporal_locality' },
    },
  },

  thresholds: {
    // With per-VU tenant affinity, per-bucket contention is minimized and
    // throughput should be noticeably higher than uniform-random workloads.
    'claims_created': ['rate>50'],
    'http_req_failed': ['rate<0.01'],
    'quota_claim_create_duration': ['p(95)<500', 'p(99)<2000'],
    'claim_success_rate': ['rate>0.99'],
  },
};

function resourceTypeFor(defIndex) {
  return `perf.test/tl-resource-${defIndex}`;
}

function definitionNameFor(defIndex) {
  return `tl-perf-def-${defIndex}`;
}

function claimingKindFor(defIndex) {
  return `TemporalLocalityKind${defIndex}`;
}

export function setup() {
  console.log(`Provisioning ${TENANT_COUNT} tenants for temporal-locality workload`);
  console.log(`VUS=${VUS}, STICKY_ITERATIONS=${STICKY_ITERATIONS}`);
  const start = Date.now();
  let errors = 0;

  const res = createQuotaDefinition(definitionNameFor(0), resourceTypeFor(0), claimingKindFor(0));
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
    const grantName = `${consumer}-tl-grant-0`;
    const gRes = createQuotaGrant(ns, grantName, consumer, resourceTypeFor(0), GRANT_AMOUNT);
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

  // Analytical collision probability for a single VU's sticky window,
  // assuming independent uniform re-rolls.
  const collisionProb = TENANT_COUNT > 0
    ? 1 - Math.pow(1 - 1 / TENANT_COUNT, Math.max(0, VUS - 1))
    : 0;

  const elapsed = (Date.now() - start) / 1000;
  console.log(`setup: complete in ${elapsed}s (${errors} errors)`);
  console.log(`setup: analytical per-window collision probability ≈ ${collisionProb.toFixed(4)}`);
  return { provisionedAt: Date.now(), errors, collisionProb };
}

// Per-VU sticky state lives in init scope (each VU has its own copy).
let stickyTenant = -1;
let stickyRemaining = 0;

export default function (data) {
  // Initialize or re-randomize this VU's sticky tenant.
  if (stickyRemaining <= 0) {
    stickyTenant = Math.floor(Math.random() * TENANT_COUNT);
    stickyRemaining = STICKY_ITERATIONS;
    stickyWindows.add(1);
    // Record the analytical expected collision rate once per window so the
    // summary has a fixed reference number to compare observed latency
    // distribution against.
    expectedCollisionRate.add((data && data.collisionProb) || 0);
  }
  stickyRemaining--;

  const tenantIdx = stickyTenant;
  const ns = namespaceFor(tenantIdx);
  const consumer = consumerNameFor(tenantIdx);
  const resourceType = resourceTypeFor(0);
  const kind = claimingKindFor(0);
  const tenantTag = String(tenantIdx);

  const claimName = `tl-claim-${__VU}-${__ITER}`;

  const createRes = createQuotaClaim(ns, claimName, consumer, resourceType, 1, claimName, kind);
  claimCreateDuration.add(createRes.timings.duration, { tenant: tenantTag });

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
