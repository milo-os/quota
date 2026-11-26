// tenant-scale.js
//
// The "growing tenants" test. Walks through several tenant population sizes
// and measures claim throughput and read latency at each level. Use this to
// see how the system degrades (or doesn't) as the tenant count grows.
//
// At each step:
//   1. Provision additional tenants up to the next target count
//   2. Run a fixed-duration claim throughput burst against the new population
//   3. Run a fixed-duration read mix against the new population
//   4. Record latency stats tagged with the current tenant count
//
// Output is structured so each measurement carries its tenant count as a tag.
// Pipe the JSON output through a script (or load into Grafana) to plot
// latency vs tenant count.
//
// Configuration:
//   TENANT_STEPS     - Comma-separated tenant counts to walk through (default "10,50,100,250,500")
//   DEFINITION_COUNT - Definitions per step (default 5)
//   GRANT_AMOUNT     - Allowance per (tenant, resource) (default 1000)
//   STEP_VUS         - VUs per measurement burst (default 20)
//   STEP_DURATION    - Duration of each measurement burst (default 30s)
//
// Usage:
//   k6 run -e TENANT_STEPS=10,100,500 tenant-scale.js
//
// Note: this is structured as a single shared-iterations scenario rather than
// the per-step scenarios you'd want for production benchmarking. The intent
// is to validate the system behaves at scale; for clean per-step measurements
// run claim-throughput.js + read-latency.js after setup-tenants.js with
// different TENANT_COUNT values.

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import {
  createQuotaDefinition,
  createQuotaGrant,
  createQuotaClaim,
  deleteQuotaClaim,
  listQuotaGrants,
  ensureNamespace,
  namespaceFor,
  consumerNameFor,
} from './lib/quota-client.js';

const TENANT_STEPS = (__ENV.TENANT_STEPS || '10,50,100,250,500')
  .split(',')
  .map((s) => parseInt(s.trim()))
  .filter((n) => !isNaN(n) && n > 0)
  .sort((a, b) => a - b);
const DEFINITION_COUNT = parseInt(__ENV.DEFINITION_COUNT || '5');
const GRANT_AMOUNT = parseInt(__ENV.GRANT_AMOUNT || '1000');
const STEP_VUS = parseInt(__ENV.STEP_VUS || '20');
const STEP_OPS = parseInt(__ENV.STEP_OPS || '500');

// Per-step metrics keyed by tenant count via tags.
const provisionDuration = new Trend('provision_duration_ms', true);
const claimLatency = new Trend('claim_create_at_scale_ms', true);
const readLatency = new Trend('grant_list_at_scale_ms', true);
const stepClaimsCreated = new Counter('step_claims_created');
const stepReadsCompleted = new Counter('step_reads_completed');

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    walk: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      maxDuration: '60m',
    },
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

let provisionedTenants = 0;

function provisionUpTo(targetTenants) {
  const start = Date.now();

  // Definitions only need to be created once.
  if (provisionedTenants === 0) {
    for (let d = 0; d < DEFINITION_COUNT; d++) {
      createQuotaDefinition(definitionNameFor(d), resourceTypeFor(d), claimingKindFor(d));
    }
  }

  for (let t = provisionedTenants; t < targetTenants; t++) {
    const ns = namespaceFor(t);
    const consumer = consumerNameFor(t);
    ensureNamespace(ns);
    for (let d = 0; d < DEFINITION_COUNT; d++) {
      createQuotaGrant(ns, `${consumer}-grant-${d}`, consumer, resourceTypeFor(d), GRANT_AMOUNT);
    }
  }

  provisionedTenants = targetTenants;
  const duration = Date.now() - start;
  provisionDuration.add(duration, { tenants: String(targetTenants) });
  return duration;
}

function runClaimBurst(tenantCount) {
  let created = 0;
  let denied = 0;
  let errs = 0;
  for (let i = 0; i < STEP_OPS; i++) {
    const tenantIdx = Math.floor(Math.random() * tenantCount);
    const defIdx = Math.floor(Math.random() * DEFINITION_COUNT);
    const ns = namespaceFor(tenantIdx);
    const claimName = `burst-${tenantCount}-${i}-${defIdx}`;
    const consumer = consumerNameFor(tenantIdx);
    const res = createQuotaClaim(ns, claimName, consumer, resourceTypeFor(defIdx), 1, claimName, claimingKindFor(defIdx));
    claimLatency.add(res.timings.duration, { tenants: String(tenantCount) });
    if (res.status === 201) {
      created++;
      stepClaimsCreated.add(1, { tenants: String(tenantCount) });
      // Delete to free quota
      deleteQuotaClaim(ns, claimName);
    } else if (res.status === 403) {
      denied++;
    } else {
      errs++;
      if (errs < 3) {
        console.error(`step ${tenantCount}: claim error ${res.status}: ${res.body}`);
      }
    }
  }
  return { created, denied, errs };
}

function runReadBurst(tenantCount) {
  let ok = 0;
  for (let i = 0; i < STEP_OPS; i++) {
    const tenantIdx = Math.floor(Math.random() * tenantCount);
    const ns = namespaceFor(tenantIdx);
    const res = listQuotaGrants(ns);
    readLatency.add(res.timings.duration, { tenants: String(tenantCount) });
    if (res.status === 200) {
      ok++;
      stepReadsCompleted.add(1, { tenants: String(tenantCount) });
    }
  }
  return ok;
}

export default function () {
  console.log(`Walking tenant scale: ${TENANT_STEPS.join(', ')}`);
  console.log(`Per step: ${STEP_OPS} claims + ${STEP_OPS} reads`);

  for (const target of TENANT_STEPS) {
    console.log(`\n=== Step: ${target} tenants ===`);
    const provDur = provisionUpTo(target);
    console.log(`Provisioned to ${target} tenants in ${provDur}ms`);

    const claimResult = runClaimBurst(target);
    console.log(`Claims at ${target} tenants: ${claimResult.created} created, ${claimResult.denied} denied, ${claimResult.errs} errors`);

    const readsOk = runReadBurst(target);
    console.log(`Reads at ${target} tenants: ${readsOk}/${STEP_OPS} successful`);
  }

  console.log('\nWalk complete.');
}
