// Shared configuration for quota-service observability.
//
// These values are referenced by every dashboard under observability/dashboards/
// and by any future alerting/recording rule mixins. They exist so that queries
// do not hard-code job/namespace selectors in dozens of places.
{
  dashboards: {
    refresh: '30s',
    timezone: 'utc',
    timeRange: { from: 'now-1h', to: 'now' },
    tags: ['quota', 'datum', 'performance'],
  },

  // The quota apiserver job is selected dynamically by the dashboard's
  // `$job` template variable (populated via label_values on
  // apiserver_request_total) so dashboards do not hard-code the label
  // value produced by prometheus-operator's ServiceMonitor conventions.
  //
  // The workqueue name comes from internal/quota/engine/policy.go, where
  // the ClaimCreationPolicy shared informer's rate-limiting queue is
  // constructed with name "quota_policy_engine". Panels match
  // workqueue_*{name=~"quota.*"} so this is not hard-coded in the query,
  // but the prefix convention is intentional — keep it.
}
