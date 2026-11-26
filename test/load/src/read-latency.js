// read-latency.js
//
// Measures read-path latency under tenant scale: how fast can the apiserver
// serve list/get requests when there are many objects in the system?
//
// Mix of operations:
//   - List all definitions cluster-wide
//   - List all grants in a single tenant namespace
//   - List all grants cluster-wide (the expensive one)
//   - Get a specific grant
//   - List buckets cluster-wide
//
// This is the workload to watch when planning the controller-manager and UI.
// Both depend on listing/watching grants and buckets.
//
// Run setup-tenants.js first to populate the system.
//
// Configuration:
//   TENANT_COUNT - Pool of tenants (default 50, must match setup)
//   VUS          - Concurrent readers (default 10)
//   DURATION     - Test duration (default 2m)
//
// Usage:
//   k6 run -e VUS=20 -e DURATION=5m read-latency.js

import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import {
  listQuotaDefinitions,
  listQuotaGrants,
  listQuotaBuckets,
  getQuotaGrant,
  namespaceFor,
} from './lib/quota-client.js';

const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '50');
const DEFINITION_COUNT = parseInt(__ENV.DEFINITION_COUNT || '5');
const VUS = parseInt(__ENV.VUS || '10');
const DURATION = __ENV.DURATION || '2m';

const readSuccessRate = new Rate('read_success_rate');
const definitionListLatency = new Trend('definition_list_ms', true);
const tenantGrantListLatency = new Trend('tenant_grant_list_ms', true);
const clusterGrantListLatency = new Trend('cluster_grant_list_ms', true);
const grantGetLatency = new Trend('grant_get_ms', true);
const bucketListLatency = new Trend('bucket_list_ms', true);
const readErrors = new Counter('read_errors');

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],

  scenarios: {
    mixed_reads: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      tags: { scenario: 'reads' },
    },
  },

  thresholds: {
    'read_success_rate': ['rate>0.99'],
    'definition_list_ms': ['p(95)<200'],
    'tenant_grant_list_ms': ['p(95)<200'],
    'cluster_grant_list_ms': ['p(95)<2000'],
    'grant_get_ms': ['p(95)<100'],
    'http_req_failed': ['rate<0.01'],
  },
};

function consumerNameFor(t) {
  return `tenant-${t}`;
}

export default function () {
  const tenantIdx = Math.floor(Math.random() * TENANT_COUNT);
  const defIdx = Math.floor(Math.random() * DEFINITION_COUNT);
  const ns = namespaceFor(tenantIdx);
  const grantName = `${consumerNameFor(tenantIdx)}-grant-${defIdx}`;

  // Weight reads to mirror realistic traffic:
  //   60% per-tenant grant list (most common - controllers reconciling)
  //   20% specific grant get
  //   10% cluster-wide grant list (admin/UI)
  //   5%  definition list
  //   5%  bucket list
  const r = Math.random();

  if (r < 0.60) {
    const res = listQuotaGrants(ns);
    tenantGrantListLatency.add(res.timings.duration);
    recordResult(res);
  } else if (r < 0.80) {
    const res = getQuotaGrant(ns, grantName);
    grantGetLatency.add(res.timings.duration);
    // 404 is fine - some grants might not exist
    recordResult(res, [200, 404]);
  } else if (r < 0.90) {
    const res = listQuotaGrants(null);
    clusterGrantListLatency.add(res.timings.duration);
    recordResult(res);
  } else if (r < 0.95) {
    const res = listQuotaDefinitions();
    definitionListLatency.add(res.timings.duration);
    recordResult(res);
  } else {
    const res = listQuotaBuckets(null);
    bucketListLatency.add(res.timings.duration);
    recordResult(res);
  }
}

function recordResult(res, allowedStatuses) {
  const allowed = allowedStatuses || [200];
  if (allowed.includes(res.status)) {
    readSuccessRate.add(1);
  } else {
    readSuccessRate.add(0);
    readErrors.add(1);
  }
}
