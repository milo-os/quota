// claim-throughput.js
//
// Measures the hot path of the quota service: QuotaClaim creation throughput
// and latency under sustained load. Each VU acts as a tenant claiming quota
// for a randomly selected resource type. Claims are deleted at the end of
// each iteration so the test can run for an arbitrary duration without
// exhausting allowances.
//
// This is the workload to watch when planning capacity. It answers:
//   - How many claims/sec can the apiserver sustain?
//   - What's the p95/p99 latency at this throughput?
//   - How does this scale with tenant count?
//
// Run setup-tenants.js first to provision tenants and grants.
//
// Configuration:
//   TENANT_COUNT     - Pool of tenants to draw from (must match setup, default 50)
//   DEFINITION_COUNT - Number of resource types (must match setup, default 5)
//   VUS              - Concurrent virtual users (default 20)
//   DURATION         - Test duration (default 2m)
//   QUOTA_API_URL    - Apiserver URL (default localhost:8001)
//
// Usage:
//   k6 run -e VUS=50 -e DURATION=5m claim-throughput.js

import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import {
  createQuotaClaim,
  deleteQuotaClaim,
  namespaceFor,
  consumerNameFor,
} from './lib/quota-client.js';

const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '50');
const DEFINITION_COUNT = parseInt(__ENV.DEFINITION_COUNT || '5');
const VUS = parseInt(__ENV.VUS || '20');
const DURATION = __ENV.DURATION || '2m';

const claimSuccessRate = new Rate('claim_success_rate');
const claimCreateLatency = new Trend('claim_create_latency_ms', true);
const claimDeleteLatency = new Trend('claim_delete_latency_ms', true);
const claimsCreated = new Counter('claims_created');
const claimsDenied = new Counter('claims_denied');
const claimErrors = new Counter('claim_errors');

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],

  scenarios: {
    steady_throughput: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      tags: { scenario: 'steady' },
    },
  },

  thresholds: {
    'claim_success_rate': ['rate>0.95'],
    'claim_create_latency_ms': ['p(95)<500', 'p(99)<2000'],
    'http_req_failed': ['rate<0.05'],
  },
};

function resourceTypeFor(defIndex) {
  return `perf.test/resource-${defIndex}`;
}

function claimingKindFor(defIndex) {
  return `TestKind${defIndex}`;
}

export default function () {
  // Each iteration picks a random tenant and resource type.
  const tenantIdx = Math.floor(Math.random() * TENANT_COUNT);
  const defIdx = Math.floor(Math.random() * DEFINITION_COUNT);
  const ns = namespaceFor(tenantIdx);
  const consumer = consumerNameFor(tenantIdx);
  const resourceType = resourceTypeFor(defIdx);
  const kind = claimingKindFor(defIdx);

  // Unique claim name per VU+iteration so we don't collide.
  const claimName = `claim-${__VU}-${__ITER}-${defIdx}`;

  const createRes = createQuotaClaim(ns, claimName, consumer, resourceType, 1, claimName, kind);
  claimCreateLatency.add(createRes.timings.duration);

  const created = check(createRes, {
    'claim created': (r) => r.status === 201,
  });

  if (created) {
    claimsCreated.add(1);
    claimSuccessRate.add(1);
  } else if (createRes.status === 403) {
    // Quota denied - expected at very high concurrency
    claimsDenied.add(1);
    claimSuccessRate.add(0);
  } else {
    claimErrors.add(1);
    claimSuccessRate.add(0);
    if (__ITER < 5) {
      console.error(`Claim error ${createRes.status}: ${createRes.body}`);
    }
  }

  // Delete the claim immediately to free quota for the next iteration.
  if (created) {
    const delRes = deleteQuotaClaim(ns, claimName);
    claimDeleteLatency.add(delRes.timings.duration);
    if (delRes.status !== 200 && delRes.status !== 202 && delRes.status !== 404) {
      claimErrors.add(1);
    }
  }
}
