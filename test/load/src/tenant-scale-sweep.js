// tenant-scale-sweep.js
//
// Runs the same claim-throughput workload at multiple tenant counts to plot a
// scaling curve. This is the direct demonstration that the quota service
// supports multi-tenant scaling: postgres (with row-level bucket locking)
// should hold claims/sec roughly constant — or growing — as tenant count
// increases, while etcd's single-Raft writer holds flat regardless.
//
// For each tenant count in TENANT_COUNTS:
//   1. Provision tenants up to that count (incremental — tenants from prior
//      steps are reused, not torn down).
//   2. Run a fixed-duration claim burst at a fixed VU count.
//   3. Capture claims_created and average/p95/p99 latency for that step.
//   4. Record the result, tagged with the tenant count.
//
// On exit, handleSummary emits two artifacts:
//   - A JSON document with the per-step results, written to
//     /tmp/tenant-scale-sweep.json (or TENANT_SCALE_SWEEP_OUT if set).
//   - A CSV version alongside the JSON, same base name with .csv extension.
//   - Also emitted to stdout so operators scraping the k6 log can see it.
//
// Both outputs are keyed by tenant count and include: claims_created,
// duration_seconds, claims_per_sec, p50_ms, p95_ms, p99_ms, errors.
//
// Configuration:
//   TENANT_COUNTS            - Comma-separated tenant counts (default "10,50,100,200,500")
//   STEP_DURATION_MS         - Milliseconds per step for the claim burst (default 30000)
//   STEP_VUS                 - Logical "VUs" the single runner emulates (default 20)
//                              Note: k6 executors can't easily restart with
//                              different VU counts mid-run, so this script
//                              uses a single-runner iteration loop with
//                              STEP_VUS parallel HTTP requests via
//                              http.batch is NOT used (claims must be
//                              serialized per VU to get realistic latency).
//                              Instead this script runs in a single VU and
//                              simulates concurrency by firing STEP_VUS
//                              concurrent requests per batch step; since k6
//                              VUs are goroutines, we keep it simple by
//                              running serially within the iteration loop
//                              and using the claims/sec counter. Use
//                              multi-tenant-scaling.js for precise VU
//                              scaling.
//   DEFINITIONS_PER_TENANT   - Definitions per tenant (default 1)
//   GRANT_AMOUNT             - Allowance per bucket (default 100000)
//   TENANT_SCALE_SWEEP_OUT   - Output JSON path (default /tmp/tenant-scale-sweep.json)
//
// Usage:
//   k6 run -e TENANT_COUNTS=10,100,500 tenant-scale-sweep.js

import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import {
  createQuotaDefinition,
  createQuotaGrant,
  createQuotaClaim,
  deleteQuotaClaim,
  ensureNamespace,
  namespaceFor,
  consumerNameFor,
} from './lib/quota-client.js';

const TENANT_COUNTS = (__ENV.TENANT_COUNTS || '10,50,100,200,500')
  .split(',')
  .map((s) => parseInt(s.trim()))
  .filter((n) => !isNaN(n) && n > 0)
  .sort((a, b) => a - b);
const STEP_DURATION_MS = parseInt(__ENV.STEP_DURATION_MS || '30000');
const STEP_VUS = parseInt(__ENV.STEP_VUS || '20');
const DEFINITIONS_PER_TENANT = parseInt(__ENV.DEFINITIONS_PER_TENANT || '1');
const GRANT_AMOUNT = parseInt(__ENV.GRANT_AMOUNT || '100000');
const OUT_PATH = __ENV.TENANT_SCALE_SWEEP_OUT || '/tmp/tenant-scale-sweep.json';

const sweepClaimLatency = new Trend('sweep_claim_create_ms', true);
const sweepClaimsCreated = new Counter('sweep_claims_created');
const sweepErrors = new Counter('sweep_errors');

export const options = {
  insecureSkipTLSVerify: __ENV.K6_INSECURE_SKIP_TLS_VERIFY !== 'false',
  // Every report shows p50 (med), p95 and p99; K6_SUMMARY_TREND_STATS overrides.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    sweep: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      maxDuration: '4h',
    },
  },
};

function resourceTypeFor(defIndex) {
  return `perf.test/sweep-resource-${defIndex}`;
}

function definitionNameFor(defIndex) {
  return `sweep-perf-def-${defIndex}`;
}

function claimingKindFor(defIndex) {
  return `SweepKind${defIndex}`;
}

let provisioned = 0;

function provisionUpTo(target) {
  const start = Date.now();
  if (provisioned === 0) {
    for (let d = 0; d < DEFINITIONS_PER_TENANT; d++) {
      createQuotaDefinition(definitionNameFor(d), resourceTypeFor(d), claimingKindFor(d));
    }
  }
  for (let t = provisioned; t < target; t++) {
    const ns = namespaceFor(t);
    const consumer = consumerNameFor(t);
    ensureNamespace(ns);
    for (let d = 0; d < DEFINITIONS_PER_TENANT; d++) {
      createQuotaGrant(ns, `${consumer}-grant-${d}`, consumer, resourceTypeFor(d), GRANT_AMOUNT);
    }
  }
  provisioned = target;
  return Date.now() - start;
}

// Run a fixed-duration claim burst against the current tenant population.
// Returns a result object for inclusion in the summary. Latencies are
// collected in a local array so we can compute percentiles for the per-step
// summary without relying on k6's runtime metrics (which are whole-run only).
function runStep(tenantCount) {
  const deadline = Date.now() + STEP_DURATION_MS;
  const latencies = [];
  let created = 0;
  let errors = 0;
  let denied = 0;

  while (Date.now() < deadline) {
    const tenantIdx = Math.floor(Math.random() * tenantCount);
    const defIdx = Math.floor(Math.random() * DEFINITIONS_PER_TENANT);
    const ns = namespaceFor(tenantIdx);
    const consumer = consumerNameFor(tenantIdx);
    const claimName = `sweep-${tenantCount}-${created}-${defIdx}`;

    const createRes = createQuotaClaim(
      ns,
      claimName,
      consumer,
      resourceTypeFor(defIdx),
      1,
      claimName,
      claimingKindFor(defIdx),
    );
    const ms = createRes.timings.duration;
    sweepClaimLatency.add(ms, { tenants: String(tenantCount) });
    latencies.push(ms);

    if (createRes.status === 201) {
      created++;
      sweepClaimsCreated.add(1, { tenants: String(tenantCount) });
      const delRes = deleteQuotaClaim(ns, claimName);
      if (delRes.status !== 200 && delRes.status !== 202 && delRes.status !== 404) {
        errors++;
        sweepErrors.add(1, { tenants: String(tenantCount) });
      }
    } else if (createRes.status === 403) {
      denied++;
    } else {
      errors++;
      sweepErrors.add(1, { tenants: String(tenantCount) });
      if (errors < 3) {
        console.error(`sweep ${tenantCount}: claim error ${createRes.status}: ${createRes.body}`);
      }
    }
  }

  // Percentile computation over the captured per-step latencies. Small
  // enough (O(100k) at most) that a sort is fine and avoids bringing in a
  // stats dependency.
  latencies.sort((a, b) => a - b);
  const pick = (p) => {
    if (latencies.length === 0) return 0;
    const idx = Math.min(latencies.length - 1, Math.floor(latencies.length * p));
    return latencies[idx];
  };

  const durationSec = STEP_DURATION_MS / 1000;
  return {
    tenants: tenantCount,
    duration_seconds: durationSec,
    claims_created: created,
    claims_per_sec: +(created / durationSec).toFixed(2),
    errors: errors,
    denied: denied,
    step_vus: STEP_VUS, // Informational; the runner uses a single-VU loop.
    p50_ms: +pick(0.5).toFixed(2),
    p95_ms: +pick(0.95).toFixed(2),
    p99_ms: +pick(0.99).toFixed(2),
  };
}

// Results are collected at global scope so handleSummary can serialize them
// after all iterations complete. k6 does not persist state between setup()
// and handleSummary, so we use a module-level var populated by default().
let sweepResults = [];

export default function () {
  console.log(`Tenant scale sweep: ${TENANT_COUNTS.join(', ')}`);
  console.log(`Per step: ${STEP_DURATION_MS}ms burst, STEP_VUS=${STEP_VUS}`);

  for (const target of TENANT_COUNTS) {
    console.log(`\n=== Step: ${target} tenants ===`);
    const provDur = provisionUpTo(target);
    console.log(`Provisioned to ${target} tenants in ${provDur}ms`);

    const result = runStep(target);
    sweepResults.push(result);

    console.log(
      `  tenants=${target} claims/sec=${result.claims_per_sec} ` +
      `p50=${result.p50_ms}ms p95=${result.p95_ms}ms p99=${result.p99_ms}ms ` +
      `errors=${result.errors}`,
    );
  }

  console.log('\nSweep complete.');
}

// handleSummary runs once after the test completes. Use it to emit both a
// JSON and a CSV artifact of the per-step sweep results. k6 writes files via
// the object keys returned here; any path prefix is honored.
export function handleSummary(data) {
  const jsonPayload = {
    generated_at: new Date().toISOString(),
    config: {
      tenant_counts: TENANT_COUNTS,
      step_duration_ms: STEP_DURATION_MS,
      step_vus: STEP_VUS,
      definitions_per_tenant: DEFINITIONS_PER_TENANT,
      grant_amount: GRANT_AMOUNT,
    },
    results: sweepResults,
  };

  const csvHeader = 'tenants,claims_created,duration_seconds,claims_per_sec,p50_ms,p95_ms,p99_ms,errors,denied\n';
  const csvRows = sweepResults
    .map((r) =>
      [
        r.tenants,
        r.claims_created,
        r.duration_seconds,
        r.claims_per_sec,
        r.p50_ms,
        r.p95_ms,
        r.p99_ms,
        r.errors,
        r.denied,
      ].join(','),
    )
    .join('\n');
  const csvPayload = csvHeader + csvRows + '\n';

  const csvPath = OUT_PATH.replace(/\.json$/i, '.csv');

  const stdoutLines = ['\n--- Tenant Scale Sweep Summary ---'];
  stdoutLines.push(csvHeader.trimEnd());
  for (const r of sweepResults) {
    stdoutLines.push(
      `${r.tenants},${r.claims_created},${r.duration_seconds},${r.claims_per_sec},${r.p50_ms},${r.p95_ms},${r.p99_ms},${r.errors},${r.denied}`,
    );
  }
  stdoutLines.push('--- end summary ---\n');

  const out = {};
  out[OUT_PATH] = JSON.stringify(jsonPayload, null, 2);
  out[csvPath] = csvPayload;
  out['stdout'] = stdoutLines.join('\n');
  return out;
}
