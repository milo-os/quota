// setup-tenants.js
//
// Provisions a baseline tenant population for performance testing:
//   - N namespaces (one per tenant)
//   - M shared QuotaDefinitions (resource types)
//   - One QuotaGrant per tenant per resource type with a configurable allowance
//
// This script does NOT exercise the hot path. Run it once before the
// throughput / latency tests to populate the cluster, then keep the resulting
// tenants in place for the steady-state tests.
//
// Configuration:
//   TENANT_COUNT       - Number of tenants to create (default 50)
//   DEFINITION_COUNT   - Number of resource types to create (default 5)
//   GRANT_AMOUNT       - Allowance per (tenant, resource) pair (default 100)
//   QUOTA_API_URL      - Base URL of the apiserver (default localhost:8001)
//
// Usage:
//   k6 run -e TENANT_COUNT=100 -e DEFINITION_COUNT=10 setup-tenants.js

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import {
  createQuotaDefinition,
  createQuotaGrant,
  ensureNamespace,
  namespaceFor,
  consumerNameFor,
} from './lib/quota-client.js';

const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '50');
const DEFINITION_COUNT = parseInt(__ENV.DEFINITION_COUNT || '5');
const GRANT_AMOUNT = parseInt(__ENV.GRANT_AMOUNT || '100');

const definitionsCreated = new Counter('definitions_created');
const namespacesCreated = new Counter('namespaces_created');
const grantsCreated = new Counter('grants_created');
const setupErrors = new Counter('setup_errors');
const setupDuration = new Trend('setup_duration_ms');

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  // Single VU running once - this is a setup script, not a load test.
  scenarios: {
    setup: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      maxDuration: '30m',
    },
  },
  thresholds: {
    'setup_errors': ['count<10'],
  },
};

function resourceTypeFor(defIndex) {
  return `perf.test/resource-${defIndex}`;
}

function definitionNameFor(defIndex) {
  return `perf-def-${defIndex}`;
}

function claimingKindFor(defIndex) {
  return `TestKind${defIndex}`;
}

export default function () {
  console.log(`Provisioning ${TENANT_COUNT} tenants × ${DEFINITION_COUNT} definitions = ${TENANT_COUNT * DEFINITION_COUNT} grants`);

  const start = Date.now();

  // 1. Create the shared QuotaDefinitions.
  for (let d = 0; d < DEFINITION_COUNT; d++) {
    const name = definitionNameFor(d);
    const res = createQuotaDefinition(name, resourceTypeFor(d), claimingKindFor(d));
    if (check(res, { 'definition created or exists': (r) => r.status === 201 || r.status === 409 })) {
      definitionsCreated.add(1);
    } else {
      setupErrors.add(1);
      console.error(`Failed to create definition ${name}: ${res.status} ${res.body}`);
    }
  }

  // 2. Create namespaces and grants per tenant.
  for (let t = 0; t < TENANT_COUNT; t++) {
    const ns = namespaceFor(t);
    const consumer = consumerNameFor(t);

    const nsRes = ensureNamespace(ns);
    if (check(nsRes, { 'namespace created or exists': (r) => r.status === 201 || r.status === 409 })) {
      namespacesCreated.add(1);
    } else {
      setupErrors.add(1);
      console.error(`Failed to create namespace ${ns}: ${nsRes.status} ${nsRes.body}`);
      continue;
    }

    for (let d = 0; d < DEFINITION_COUNT; d++) {
      const grantName = `${consumer}-grant-${d}`;
      const res = createQuotaGrant(ns, grantName, consumer, resourceTypeFor(d), GRANT_AMOUNT);
      if (check(res, { 'grant created or exists': (r) => r.status === 201 || r.status === 409 })) {
        grantsCreated.add(1);
      } else {
        setupErrors.add(1);
        if (t < 3) {
          // Only log the first few failures to avoid log spam
          console.error(`Failed to create grant ${ns}/${grantName}: ${res.status} ${res.body}`);
        }
      }
    }

    if ((t + 1) % 10 === 0) {
      console.log(`Provisioned tenant ${t + 1}/${TENANT_COUNT}`);
    }
  }

  setupDuration.add(Date.now() - start);
  console.log(`Setup complete in ${(Date.now() - start) / 1000}s`);
}
