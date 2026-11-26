// quota-performance.jsonnet
//
// Performance drill-down dashboard for the quota apiserver. The purpose of
// this dashboard is to surface bottlenecks across every layer of the quota
// system: the aggregated apiserver, the transactional claim hot path, the
// storage backend (etcd or Postgres), the policy engine workqueue, and the
// Go runtime.
//
// Metric sources
// --------------
//   Custom (from internal/metrics/metrics.go and internal/quota/engine/policy.go)
//     quota_claim_total{result}
//     quota_claim_duration_seconds{result}
//     quota_grant_total{operation}
//     quota_definition_total{operation}
//     quota_policy_informer_events_total{type}
//     quota_policy_load_total
//     quota_policies_active
//
//   Automatic (k8s.io/apiserver aggregated apiserver)
//     apiserver_request_total, apiserver_request_duration_seconds,
//     apiserver_current_inflight_requests, apiserver_flowcontrol_rejected_requests_total,
//     storage_operation_duration_seconds, storage_operation_errors_total,
//     etcd_request_duration_seconds, etcd_db_total_size_in_bytes,
//     etcd_server_has_leader, etcd_server_leader_changes_seen_total,
//     workqueue_depth, workqueue_adds_total, workqueue_queue_duration_seconds,
//     workqueue_work_duration_seconds, workqueue_retries_total,
//     workqueue_unfinished_work_seconds, workqueue_longest_running_processor_seconds,
//     go_goroutines, go_gc_duration_seconds, process_cpu_seconds_total,
//     process_resident_memory_bytes, process_open_fds

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.4.0/main.libsonnet';
local config = import '../config.libsonnet';

local dashboard = g.dashboard;
local variable = dashboard.variable;
local row = g.panel.row;
local ts = g.panel.timeSeries;
local stat = g.panel.stat;
local heatmap = g.panel.heatmap;
local prom = g.query.prometheus;
local util = g.util;

// -----------------------------------------------------------------------------
// Variables
// -----------------------------------------------------------------------------
// The `datasource` variable lets the dashboard target any Prometheus instance,
// and the `job` variable is populated dynamically from label_values so the
// dashboard works regardless of how prometheus-operator actually names the job
// label for the quota ServiceMonitor. The regex filters to jobs matching
// "quota" to auto-select the right one on first load.

local datasourceVar =
  variable.datasource.new('datasource', 'prometheus')
  + variable.datasource.generalOptions.withLabel('Data source')
  + variable.datasource.withRegex('');

local jobVar =
  variable.query.new('job')
  + variable.query.generalOptions.withLabel('Job')
  + variable.query.withDatasourceFromVariable(datasourceVar)
  + variable.query.queryTypes.withLabelValues('job', 'apiserver_request_total')
  + variable.query.withRefresh('time')
  + variable.query.withRegex('/.*quota.*/')
  + variable.query.selectionOptions.withIncludeAll(false);

local instanceVar =
  variable.query.new('instance')
  + variable.query.generalOptions.withLabel('Instance')
  + variable.query.withDatasourceFromVariable(datasourceVar)
  + variable.query.queryTypes.withLabelValues('instance', 'apiserver_request_total{job="$job"}')
  + variable.query.withRefresh('time')
  + variable.query.selectionOptions.withIncludeAll(true, '.*')
  + variable.query.selectionOptions.withMulti(true);

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------
local ds = '$datasource';
// Selector for metrics exposed by the quota apiserver itself (filtered via
// the $job template variable populated from apiserver_request_total labels).
local jobSel = 'job="$job", instance=~"$instance"';
// Selector for etcd *server-side* metrics, which are scraped from the etcd
// pods via the quota-etcd ServiceMonitor and therefore carry a different job
// label. Pinning by namespace avoids cross-cluster collisions if the same
// Prometheus scrapes multiple etcd fleets.
local etcdSel = 'namespace="quota-system", job=~".*quota-etcd.*|quota-etcd"';

// Build an array of prometheus targets from [{expr, legend}, ...].
local targets(items) = [
  prom.new(ds, item.expr) + prom.withLegendFormat(item.legend)
  for item in items
];

// Time-series panel with sensible defaults for a performance dashboard:
// table legend with calcs, multi-mode tooltip, no points, light fill.
local tsPanel(title, items, unit='short', stack=false, description='') =
  ts.new(title)
  + (if description != '' then ts.panelOptions.withDescription(description) else {})
  + ts.queryOptions.withTargets(targets(items))
  + ts.standardOptions.withUnit(unit)
  + ts.options.legend.withDisplayMode('table')
  + ts.options.legend.withPlacement('bottom')
  + ts.options.legend.withCalcs(['lastNotNull', 'max', 'mean'])
  + ts.options.tooltip.withMode('multi')
  + ts.options.tooltip.withSort('desc')
  + ts.fieldConfig.defaults.custom.withFillOpacity(if stack then 30 else 10)
  + ts.fieldConfig.defaults.custom.withShowPoints('never')
  + ts.fieldConfig.defaults.custom.withLineWidth(1)
  + (if stack
     then ts.fieldConfig.defaults.custom.stacking.withMode('normal')
     else {});

// Single-value stat panel with area sparkline backing the current value.
local statPanel(title, expr, unit='short', thresholds=null, description='') =
  stat.new(title)
  + (if description != '' then stat.panelOptions.withDescription(description) else {})
  + stat.queryOptions.withTargets([prom.new(ds, expr)])
  + stat.standardOptions.withUnit(unit)
  + stat.options.withColorMode('value')
  + stat.options.withGraphMode('area')
  + stat.options.withJustifyMode('auto')
  + stat.options.reduceOptions.withCalcs(['lastNotNull'])
  + (if thresholds != null
     then stat.standardOptions.thresholds.withMode('absolute')
          + stat.standardOptions.thresholds.withSteps(thresholds)
     else {});

// Heatmap fed from native Prometheus histogram buckets (format='heatmap'
// requires bucket_rate summations on the server side).
local heatPanel(title, expr, unit='s', description='') =
  heatmap.new(title)
  + (if description != '' then heatmap.panelOptions.withDescription(description) else {})
  + heatmap.queryOptions.withTargets([
      prom.new(ds, expr)
      + prom.withFormat('heatmap')
      + prom.withLegendFormat('{{le}}'),
    ])
  + heatmap.options.withCalculate(false)
  + heatmap.options.yAxis.withUnit(unit)
  + heatmap.options.color.withScheme('Spectral')
  + heatmap.options.color.withMode('scheme');

// Threshold presets — value thresholds are expressed in the panel's unit.
local okWarnCritThresholds = [
  { color: 'green', value: null },
  { color: 'orange', value: 1 },
  { color: 'red', value: 5 },
];
local latencyThresholds = [
  { color: 'green', value: null },
  { color: 'orange', value: 0.5 },
  { color: 'red', value: 2 },
];
local booleanUpThresholds = [
  { color: 'red', value: null },
  { color: 'green', value: 1 },
];

// =============================================================================
// Row 1 — Golden signals (at-a-glance health)
// =============================================================================
local goldenSignalsRow = row.new('Golden Signals') + row.withCollapsed(false);

local goldenSignals = [
  statPanel(
    'Request rate',
    'sum(rate(apiserver_request_total{%s}[$__rate_interval]))' % jobSel,
    'reqps',
    description='Total quota apiserver request rate across all verbs and resources.'
  ),
  statPanel(
    'Error rate (5xx)',
    'sum(rate(apiserver_request_total{%s, code=~"5.."}[$__rate_interval]))' % jobSel,
    'reqps',
    thresholds=okWarnCritThresholds,
    description='5xx response rate. Non-zero indicates server-side failures.'
  ),
  statPanel(
    'p99 latency (all verbs)',
    'histogram_quantile(0.99, sum by (le) (rate(apiserver_request_duration_seconds_bucket{%s, verb!~"WATCH|CONNECT"}[$__rate_interval])))' % jobSel,
    's',
    thresholds=latencyThresholds,
    description='99th percentile request latency across all non-streaming verbs.'
  ),
  statPanel(
    'In-flight requests',
    'sum(apiserver_current_inflight_requests{%s})' % jobSel,
    'short',
    description='Requests currently being served. Rising values can indicate saturation.'
  ),
];

// =============================================================================
// Row 2 — Apiserver request performance (RED)
// =============================================================================
local apiserverRedRow = row.new('Apiserver: Request Performance (RED)');

local apiserverRed = [
  tsPanel(
    'Request rate by verb',
    [{
      expr: 'sum by (verb) (rate(apiserver_request_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{verb}}',
    }],
    unit='reqps',
    stack=true,
  ),
  tsPanel(
    'Request rate by resource',
    [{
      expr: 'sum by (resource) (rate(apiserver_request_total{%s, resource=~"quota.*"}[$__rate_interval]))' % jobSel,
      legend: '{{resource}}',
    }],
    unit='reqps',
    stack=true,
  ),
  tsPanel(
    'Request latency (p50 / p95 / p99)',
    [
      {
        expr: 'histogram_quantile(0.50, sum by (le, verb) (rate(apiserver_request_duration_seconds_bucket{%s, verb!~"WATCH|CONNECT"}[$__rate_interval])))' % jobSel,
        legend: 'p50 {{verb}}',
      },
      {
        expr: 'histogram_quantile(0.95, sum by (le, verb) (rate(apiserver_request_duration_seconds_bucket{%s, verb!~"WATCH|CONNECT"}[$__rate_interval])))' % jobSel,
        legend: 'p95 {{verb}}',
      },
      {
        expr: 'histogram_quantile(0.99, sum by (le, verb) (rate(apiserver_request_duration_seconds_bucket{%s, verb!~"WATCH|CONNECT"}[$__rate_interval])))' % jobSel,
        legend: 'p99 {{verb}}',
      },
    ],
    unit='s',
  ),
  heatPanel(
    'Request duration heatmap',
    'sum by (le) (rate(apiserver_request_duration_seconds_bucket{%s, verb!~"WATCH|CONNECT"}[$__rate_interval]))' % jobSel,
    description='Distribution of request latency across the full histogram. Use this to spot bimodal latency.'
  ),
];

// =============================================================================
// Row 3 — Apiserver errors & saturation
// =============================================================================
local apiserverErrorsRow = row.new('Apiserver: Errors & Saturation');

local apiserverErrors = [
  tsPanel(
    'Request rate by status code',
    [{
      expr: 'sum by (code) (rate(apiserver_request_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{code}}',
    }],
    unit='reqps',
    stack=true,
  ),
  tsPanel(
    'Client vs server errors',
    [
      {
        expr: 'sum(rate(apiserver_request_total{%s, code=~"4.."}[$__rate_interval]))' % jobSel,
        legend: '4xx (client)',
      },
      {
        expr: 'sum(rate(apiserver_request_total{%s, code=~"5.."}[$__rate_interval]))' % jobSel,
        legend: '5xx (server)',
      },
    ],
    unit='reqps',
  ),
  tsPanel(
    'Flow-control rejected requests',
    [{
      expr: 'sum by (reason) (rate(apiserver_flowcontrol_rejected_requests_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{reason}}',
    }],
    unit='reqps',
    description='APF (API Priority and Fairness) rejections. Non-zero indicates the apiserver is shedding load.'
  ),
  tsPanel(
    'In-flight requests by kind',
    [{
      expr: 'sum by (request_kind) (apiserver_current_inflight_requests{%s})' % jobSel,
      legend: '{{request_kind}}',
    }],
    unit='short',
    description='Mutating vs read-only in-flight requests. Quota CLAIM creates are mutating.'
  ),
];

// =============================================================================
// Row 4 — Quota claim hot path
// =============================================================================
// The claim path is the performance-critical transactional flow. We show both
// the custom histogram (quota_claim_duration_seconds) for decision latency and
// the apiserver view (filtered to quotaclaims) for end-to-end request latency.
local claimRow = row.new('Quota Claim Hot Path');

local claim = [
  tsPanel(
    'Claim decision rate by result',
    [{
      expr: 'sum by (result) (rate(quota_claim_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{result}}',
    }],
    unit='reqps',
    stack=true,
    description='Claims processed per second, split by outcome (granted / denied / error). Sourced from internal/metrics/metrics.go.'
  ),
  tsPanel(
    'Claim decision latency (p50 / p95 / p99)',
    [
      {
        expr: 'histogram_quantile(0.50, sum by (le, result) (rate(quota_claim_duration_seconds_bucket{%s}[$__rate_interval])))' % jobSel,
        legend: 'p50 {{result}}',
      },
      {
        expr: 'histogram_quantile(0.95, sum by (le, result) (rate(quota_claim_duration_seconds_bucket{%s}[$__rate_interval])))' % jobSel,
        legend: 'p95 {{result}}',
      },
      {
        expr: 'histogram_quantile(0.99, sum by (le, result) (rate(quota_claim_duration_seconds_bucket{%s}[$__rate_interval])))' % jobSel,
        legend: 'p99 {{result}}',
      },
    ],
    unit='s',
    description='Time from QuotaClaim CREATE arrival to transactional decision. Drill into tail latency here.'
  ),
  heatPanel(
    'Claim decision latency heatmap',
    'sum by (le) (rate(quota_claim_duration_seconds_bucket{%s}[$__rate_interval]))' % jobSel,
    description='Decision latency distribution. A long upper tail points at bucket contention or storage lag.'
  ),
  tsPanel(
    'Claim apiserver errors (denial / conflict)',
    [
      {
        expr: 'sum(rate(apiserver_request_total{%s, resource="quotaclaims", verb="create", code="403"}[$__rate_interval]))' % jobSel,
        legend: '403 denied',
      },
      {
        expr: 'sum(rate(apiserver_request_total{%s, resource="quotaclaims", verb="create", code="409"}[$__rate_interval]))' % jobSel,
        legend: '409 conflict',
      },
      {
        expr: 'sum(rate(apiserver_request_total{%s, resource="quotaclaims", verb="create", code=~"5.."}[$__rate_interval]))' % jobSel,
        legend: '5xx',
      },
    ],
    unit='reqps',
    description='Denial = quota exhausted. Conflict = optimistic lock collision on bucket status (hot row).'
  ),
];

// =============================================================================
// Row 5 — Quota write operations
// =============================================================================
local writesRow = row.new('Quota Write Operations');

local writes = [
  tsPanel(
    'QuotaGrant operations',
    [{
      expr: 'sum by (operation) (rate(quota_grant_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{operation}}',
    }],
    unit='ops',
    stack=true,
  ),
  tsPanel(
    'QuotaDefinition operations',
    [{
      expr: 'sum by (operation) (rate(quota_definition_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{operation}}',
    }],
    unit='ops',
    stack=true,
  ),
  tsPanel(
    'p99 write latency by resource',
    [{
      expr: 'histogram_quantile(0.99, sum by (le, resource) (rate(apiserver_request_duration_seconds_bucket{%s, resource=~"quota.*", verb=~"POST|PUT|PATCH|DELETE"}[$__rate_interval])))' % jobSel,
      legend: '{{resource}}',
    }],
    unit='s',
  ),
  tsPanel(
    'Write error rate by resource',
    [{
      expr: 'sum by (resource) (rate(apiserver_request_total{%s, resource=~"quota.*", verb=~"POST|PUT|PATCH|DELETE", code=~"5.."}[$__rate_interval]))' % jobSel,
      legend: '{{resource}}',
    }],
    unit='reqps',
  ),
];

// =============================================================================
// Row 6 — Storage backend (etcd / Postgres via storage_operation_*)
// =============================================================================
local storageRow = row.new('Storage Backend');

local storage = [
  tsPanel(
    'Storage operation rate',
    [{
      expr: 'sum by (operation) (rate(storage_operation_duration_seconds_count{%s}[$__rate_interval]))' % jobSel,
      legend: '{{operation}}',
    }],
    unit='ops',
    stack=true,
  ),
  tsPanel(
    'p99 storage op latency',
    [{
      expr: 'histogram_quantile(0.99, sum by (le, operation) (rate(storage_operation_duration_seconds_bucket{%s}[$__rate_interval])))' % jobSel,
      legend: '{{operation}}',
    }],
    unit='s',
    description='Per-operation storage latency. Spikes here propagate directly into claim decision latency.'
  ),
  tsPanel(
    'Storage errors',
    [{
      expr: 'sum by (operation) (rate(storage_operation_errors_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{operation}}',
    }],
    unit='ops',
  ),
  tsPanel(
    'p99 etcd request duration',
    [{
      expr: 'histogram_quantile(0.99, sum by (le, operation) (rate(etcd_request_duration_seconds_bucket{%s}[$__rate_interval])))' % jobSel,
      legend: '{{operation}}',
    }],
    unit='s',
    description='Only present when the etcd backend is in use.'
  ),
];

// =============================================================================
// Row 7 — etcd health
// =============================================================================
local etcdHealthRow = row.new('etcd Health') + row.withCollapsed(true);

// NOTE: These panels query etcd *server-side* metrics (from the etcd pods
// themselves), not the apiserver's client-side etcd view. They rely on
// `metrics.enabled: true` in config/components/etcd/helmrelease.yaml and
// the quota-etcd ServiceMonitor in config/components/observability/
// servicemonitors/. The selector is `etcdSel`, not `jobSel`, because the
// job label on these metrics is set by the etcd ServiceMonitor.
local etcdHealth = [
  statPanel(
    'etcd DB size',
    'max(etcd_mvcc_db_total_size_in_bytes{%s}) or max(etcd_db_total_size_in_bytes{%s})' % [etcdSel, etcdSel],
    'bytes',
    description='Apparent etcd database size. Sustained growth without compaction indicates retention problems.'
  ),
  statPanel(
    'etcd has leader',
    'min(etcd_server_has_leader{%s})' % etcdSel,
    'short',
    thresholds=booleanUpThresholds,
  ),
  tsPanel(
    'etcd leader changes',
    [{
      expr: 'sum(increase(etcd_server_leader_changes_seen_total{%s}[$__rate_interval]))' % etcdSel,
      legend: 'leader changes',
    }],
    unit='short',
    description='Any non-zero value is a signal — leader churn causes request latency spikes.'
  ),
  tsPanel(
    'p99 etcd WAL fsync',
    [{
      expr: 'histogram_quantile(0.99, sum by (le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket{%s}[$__rate_interval])))' % etcdSel,
      legend: 'p99',
    }],
    unit='s',
    description='WAL fsync latency. The etcd "slow fsync" warning fires at 1s — keep this well under 100ms.'
  ),
];

// =============================================================================
// Row 8 — Policy engine & workqueue
// =============================================================================
local policyRow = row.new('Policy Engine & Workqueue');

local policy = [
  tsPanel(
    'Active policies & load rate',
    [
      {
        expr: 'max(quota_policies_active{%s})' % jobSel,
        legend: 'active',
      },
      {
        expr: 'sum(rate(quota_policy_load_total{%s}[$__rate_interval]))' % jobSel,
        legend: 'load rate',
      },
    ],
    unit='short',
    description='quota_policies_active = current count in the shared informer cache. quota_policy_load_total = full-reload rate.'
  ),
  tsPanel(
    'Policy informer events',
    [{
      expr: 'sum by (type) (rate(quota_policy_informer_events_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{type}}',
    }],
    unit='ops',
    stack=true,
  ),
  tsPanel(
    'Workqueue depth',
    [{
      expr: 'sum by (name) (workqueue_depth{%s, name=~"quota.*"})' % jobSel,
      legend: '{{name}}',
    }],
    unit='short',
    description='Items queued for reconciliation. Sustained growth indicates the policy engine cannot keep up.'
  ),
  tsPanel(
    'p99 workqueue work duration',
    [{
      expr: 'histogram_quantile(0.99, sum by (le, name) (rate(workqueue_work_duration_seconds_bucket{%s, name=~"quota.*"}[$__rate_interval])))' % jobSel,
      legend: '{{name}}',
    }],
    unit='s',
    description='How long a work item takes once dequeued. This is the policy engine reconcile latency.'
  ),
];

// =============================================================================
// Row 9 — Workqueue saturation
// =============================================================================
local workqueueRow = row.new('Workqueue Saturation');

local workqueue = [
  tsPanel(
    'Workqueue add rate',
    [{
      expr: 'sum by (name) (rate(workqueue_adds_total{%s, name=~"quota.*"}[$__rate_interval]))' % jobSel,
      legend: '{{name}}',
    }],
    unit='ops',
  ),
  tsPanel(
    'p99 workqueue queue duration',
    [{
      expr: 'histogram_quantile(0.99, sum by (le, name) (rate(workqueue_queue_duration_seconds_bucket{%s, name=~"quota.*"}[$__rate_interval])))' % jobSel,
      legend: '{{name}}',
    }],
    unit='s',
    description='Time an item sits in the queue before a worker picks it up. High values = worker starvation.'
  ),
  tsPanel(
    'Workqueue retries',
    [{
      expr: 'sum by (name) (rate(workqueue_retries_total{%s, name=~"quota.*"}[$__rate_interval]))' % jobSel,
      legend: '{{name}}',
    }],
    unit='ops',
    description='Retry rate. Non-zero retries usually indicate transient conflicts or downstream errors.'
  ),
  tsPanel(
    'Longest running processor / unfinished work',
    [
      {
        expr: 'max by (name) (workqueue_longest_running_processor_seconds{%s, name=~"quota.*"})' % jobSel,
        legend: '{{name}} longest',
      },
      {
        expr: 'max by (name) (workqueue_unfinished_work_seconds{%s, name=~"quota.*"})' % jobSel,
        legend: '{{name}} unfinished',
      },
    ],
    unit='s',
    description='Stuck or slow reconciles show up here before they show up in latency histograms.'
  ),
];

// =============================================================================
// Row 10 — Watch & caching
// =============================================================================
local watchRow = row.new('Watch & Caching') + row.withCollapsed(true);

local watch = [
  tsPanel(
    'Active watches',
    [{
      expr: 'sum by (resource) (apiserver_longrunning_requests{%s, verb="WATCH", resource=~"quota.*"})' % jobSel,
      legend: '{{resource}}',
    }],
    unit='short',
    description='Open long-running WATCH requests per resource. apiserver_registered_watchers is deprecated — apiserver_longrunning_requests replaces it.'
  ),
  tsPanel(
    'Watch establishment rate',
    [{
      expr: 'sum by (resource) (rate(apiserver_request_total{%s, verb="WATCH", resource=~"quota.*"}[$__rate_interval]))' % jobSel,
      legend: '{{resource}}',
    }],
    unit='ops',
    description='Rate of new WATCH requests being opened (not the event stream rate). Event-stream volume is visible via Row 8 policy-informer events for our own caches.'
  ),
  tsPanel(
    'LIST / GET latency p99',
    [
      {
        expr: 'histogram_quantile(0.99, sum by (le, resource) (rate(apiserver_request_duration_seconds_bucket{%s, verb="LIST", resource=~"quota.*"}[$__rate_interval])))' % jobSel,
        legend: 'LIST {{resource}}',
      },
      {
        expr: 'histogram_quantile(0.99, sum by (le, resource) (rate(apiserver_request_duration_seconds_bucket{%s, verb="GET", resource=~"quota.*"}[$__rate_interval])))' % jobSel,
        legend: 'GET {{resource}}',
      },
    ],
    unit='s',
  ),
  tsPanel(
    'Response size p99',
    [{
      expr: 'histogram_quantile(0.99, sum by (le, resource) (rate(apiserver_response_sizes_bucket{%s, resource=~"quota.*"}[$__rate_interval])))' % jobSel,
      legend: '{{resource}}',
    }],
    unit='bytes',
  ),
];

// =============================================================================
// Row 11 — Go runtime
// =============================================================================
local runtimeRow = row.new('Go Runtime') + row.withCollapsed(true);

local runtime = [
  tsPanel(
    'CPU usage',
    [{
      expr: 'sum by (instance) (rate(process_cpu_seconds_total{%s}[$__rate_interval]))' % jobSel,
      legend: '{{instance}}',
    }],
    unit='percentunit',
  ),
  tsPanel(
    'Resident memory',
    [{
      expr: 'sum by (instance) (process_resident_memory_bytes{%s})' % jobSel,
      legend: '{{instance}}',
    }],
    unit='bytes',
  ),
  tsPanel(
    'Goroutines',
    [{
      expr: 'sum by (instance) (go_goroutines{%s})' % jobSel,
      legend: '{{instance}}',
    }],
    unit='short',
  ),
  tsPanel(
    'GC pause p99',
    [{
      expr: 'histogram_quantile(0.99, sum by (le, instance) (rate(go_gc_duration_seconds_bucket{%s}[$__rate_interval])))' % jobSel,
      legend: '{{instance}}',
    }],
    unit='s',
    description='If quantile buckets are unavailable on this Go version, fall back to go_gc_duration_seconds{quantile="0.99"}.'
  ),
];

// =============================================================================
// Dashboard assembly
// =============================================================================
// `makeGrid` treats every `row` panel as a linebreak and grids the following
// panels at panelWidth x panelHeight. Using panelWidth=6 gives us 4 panels per
// visual row (24 columns / 6 = 4). We wrap the final list in `setPanelIDs` so
// every panel gets a unique auto-assigned ID — this avoids collisions if
// panels are ever moved between rows.

local panels = util.panel.setPanelIDs(
  util.grid.makeGrid(
    [
      goldenSignalsRow + row.withPanels(goldenSignals),
      apiserverRedRow + row.withPanels(apiserverRed),
      apiserverErrorsRow + row.withPanels(apiserverErrors),
      claimRow + row.withPanels(claim),
      writesRow + row.withPanels(writes),
      storageRow + row.withPanels(storage),
      etcdHealthRow + row.withPanels(etcdHealth),
      policyRow + row.withPanels(policy),
      workqueueRow + row.withPanels(workqueue),
      watchRow + row.withPanels(watch),
      runtimeRow + row.withPanels(runtime),
    ],
    panelWidth=6,
    panelHeight=8,
  )
);

dashboard.new('Quota Performance')
+ dashboard.withUid('datum-quota-performance')
+ dashboard.withDescription(
  'Performance drill-down for the quota apiserver. Surfaces bottlenecks across ' +
  'the claim hot path, storage backend, policy engine workqueue, and Go runtime.'
)
+ dashboard.withTags(config.dashboards.tags)
+ dashboard.withEditable(true)
+ dashboard.withRefresh(config.dashboards.refresh)
+ dashboard.withTimezone(config.dashboards.timezone)
+ dashboard.time.withFrom(config.dashboards.timeRange.from)
+ dashboard.time.withTo(config.dashboards.timeRange.to)
+ dashboard.graphTooltip.withSharedCrosshair()
+ dashboard.withVariables([datasourceVar, jobVar, instanceVar])
+ dashboard.withPanels(panels)
