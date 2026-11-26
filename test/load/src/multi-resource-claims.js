// multi-resource-claims.js
//
// Exercises the multi-bucket plan/commit path in the allocator. Each
// QuotaClaim's Spec.Requests[] references 2-3 resource types, so a single
// claim must atomically decrement multiple bucket rows. The existing
// throughput scripts only claim from a single bucket and therefore never hit
// this code path — this script is the one that will catch deadlocks if the
// multi-bucket lock ordering is wrong.
//
// Setup phase:
//   - Provisions RESOURCE_COUNT shared QuotaDefinitions (default 3)
//   - Provisions TENANT_COUNT tenants, each with one QuotaGrant per resource
//     type with a high allowance so capacity is never the bottleneck
//
// Test phase:
//   - Each VU picks a random tenant per iteration, selects RESOURCES_PER_CLAIM
//     resource types from the tenant's set, and submits a single claim that
//     requests amount=1 from each. The claim is deleted at the end of the
//     iteration to recycle capacity.
//
// Metrics captured:
//   - claims/sec (claims_created counter)
//   - p50/p95/p99 create latency (quota_claim_create_duration)
//   - p50/p95/p99 delete latency (quota_claim_delete_duration)
//   - error rate (http_errors, http_req_failed)
//   - per-bucket-count breakdown tag on latency (requests tag)
//
// Thresholds (multi-bucket path is slower than single-bucket):
//   - claims_created rate > 20/s
//   - http_req_failed < 1%
//   - quota_claim_create_duration p95 < 800ms
//
// Configuration:
//   TENANT_COUNT        - Number of tenants to provision/use (default 50)
//   RESOURCE_COUNT      - Resource types per tenant (default 3)
//   RESOURCES_PER_CLAIM - Bucket count each claim spans (default 2)
//   VUS                 - Concurrent virtual users (default 20)
//   DURATION            - Steady-state test duration (default 60s)
//   GRANT_AMOUNT        - Allowance per (tenant, resource) bucket (default 10000)
//   QUOTA_API_URL       - Apiserver URL (default localhost:8001)
//
// Usage:
//   k6 run -e TENANT_COUNT=100 -e RESOURCES_PER_CLAIM=3 multi-resource-claims.js

import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import {
  createQuotaDefinition,
  createQuotaGrant,
  createMultiResourceQuotaClaim,
  deleteQuotaClaim,
  ensureNamespace,
  namespaceFor,
  consumerNameFor,
} from './lib/quota-client.js';

const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '50');
const RESOURCE_COUNT = parseInt(__ENV.RESOURCE_COUNT || '3');
const RESOURCES_PER_CLAIM = parseInt(__ENV.RESOURCES_PER_CLAIM || '2');
const VUS = parseInt(__ENV.VUS || '20');
const DURATION = __ENV.DURATION || '60s';
const GRANT_AMOUNT = parseInt(__ENV.GRANT_AMOUNT || '10000');

if (RESOURCES_PER_CLAIM > RESOURCE_COUNT) {
  throw new Error(`RESOURCES_PER_CLAIM (${RESOURCES_PER_CLAIM}) cannot exceed RESOURCE_COUNT (${RESOURCE_COUNT})`);
}

const claimsCreated = new Counter('claims_created');
const claimsDenied = new Counter('claims_denied');
const httpErrors = new Counter('http_errors');
const claimSuccessRate = new Rate('claim_success_rate');
const claimCreateDuration = new Trend('quota_claim_create_duration', true);
const claimDeleteDuration = new Trend('quota_claim_delete_duration', true);

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],

  scenarios: {
    multi_resource: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      tags: { scenario: 'multi_resource_claims' },
    },
  },

  thresholds: {
    // Multi-bucket plan/commit adds overhead per request, so thresholds are
    // looser than single-bucket tests. Tighten as the allocator improves.
    'claims_created': ['rate>20'],
    'http_req_failed': ['rate<0.01'],
    'quota_claim_create_duration': ['p(95)<800', 'p(99)<2500'],
    'claim_success_rate': ['rate>0.99'],
  },
};

// All definitions share a single claiming kind so one QuotaClaim can reference
// multiple resource types in Spec.Requests[] without violating the per-definition
// claimingResources validation.
const SHARED_CLAIMING_KIND = 'MultiResourceClaimer';

function resourceTypeFor(defIndex) {
  return `perf.test/mr-resource-${defIndex}`;
}

function definitionNameFor(defIndex) {
  return `mr-perf-def-${defIndex}`;
}

// Fisher-Yates partial shuffle: returns `n` distinct indices from [0, max).
function pickDistinct(max, n) {
  const pool = [];
  for (let i = 0; i < max; i++) {
    pool.push(i);
  }
  for (let i = 0; i < n; i++) {
    const j = i + Math.floor(Math.random() * (max - i));
    const tmp = pool[i];
    pool[i] = pool[j];
    pool[j] = tmp;
  }
  return pool.slice(0, n);
}

export function setup() {
  console.log(`Provisioning ${TENANT_COUNT} tenants × ${RESOURCE_COUNT} resource types = ${TENANT_COUNT * RESOURCE_COUNT} grants`);
  console.log(`Each claim will request ${RESOURCES_PER_CLAIM} resource types atomically`);
  const start = Date.now();
  let errors = 0;

  // 1. Shared QuotaDefinitions (one per resource type). Every definition uses
  // the same claiming kind so a single QuotaClaim may request multiple resource
  // types atomically.
  for (let d = 0; d < RESOURCE_COUNT; d++) {
    const res = createQuotaDefinition(definitionNameFor(d), resourceTypeFor(d), SHARED_CLAIMING_KIND);
    if (res.status !== 201 && res.status !== 409) {
      errors++;
      console.error(`setup: definition ${d} failed: ${res.status} ${res.body}`);
    }
  }

  // 2. Per-tenant namespace + grants for every resource type.
  for (let t = 0; t < TENANT_COUNT; t++) {
    const ns = namespaceFor(t);
    const consumer = consumerNameFor(t);
    const nsRes = ensureNamespace(ns);
    if (nsRes.status !== 201 && nsRes.status !== 409) {
      errors++;
      console.error(`setup: namespace ${ns} failed: ${nsRes.status} ${nsRes.body}`);
      continue;
    }
    for (let d = 0; d < RESOURCE_COUNT; d++) {
      const grantName = `${consumer}-mr-grant-${d}`;
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
  const tenantIdx = Math.floor(Math.random() * TENANT_COUNT);
  const ns = namespaceFor(tenantIdx);
  const consumer = consumerNameFor(tenantIdx);

  // Pick RESOURCES_PER_CLAIM distinct resource types from the tenant's set.
  // Every definition lists SHARED_CLAIMING_KIND as an allowed claimer, so one
  // claim with resourceRef.kind=SHARED_CLAIMING_KIND can legally request
  // multiple resource types.
  const defIndices = pickDistinct(RESOURCE_COUNT, RESOURCES_PER_CLAIM);
  const requests = defIndices.map((d) => ({
    resourceType: resourceTypeFor(d),
    amount: 1,
  }));
  const bucketCountTag = String(RESOURCES_PER_CLAIM);

  // Claim name unique per VU+iteration.
  const claimName = `mr-claim-${__VU}-${__ITER}`;

  const createRes = createMultiResourceQuotaClaim(ns, claimName, consumer, requests, claimName, SHARED_CLAIMING_KIND);
  const createMs = createRes.timings.duration;
  claimCreateDuration.add(createMs, { requests: bucketCountTag });

  const created = check(createRes, {
    'claim created': (r) => r.status === 201,
  });

  if (created) {
    claimsCreated.add(1);
    claimSuccessRate.add(1);
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
    claimDeleteDuration.add(delRes.timings.duration, { requests: bucketCountTag });
    if (delRes.status !== 200 && delRes.status !== 202 && delRes.status !== 404) {
      httpErrors.add(1);
    }
  }
}
