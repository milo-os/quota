package enforcement

import (
	"k8s.io/component-base/metrics"
	legacyregistry "k8s.io/component-base/metrics/legacyregistry"
)

// Metrics for watch manager
// All metrics are aggregated at the system level to avoid cardinality explosion.
var (
	// Lifecycle metrics
	watchManagersCreated = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_managers_created_total",
			Help:           "Total number of watch managers created.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	watchManagersStopped = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_managers_stopped_total",
			Help:           "Total number of watch managers stopped.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	watchManagersActive = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_managers_active",
			Help:           "Current number of active watch managers.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// Watch stream health metrics
	watchStreamsDesired = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_streams_desired",
			Help:           "Number of watch streams that should be running (one per active watch manager).",
			StabilityLevel: metrics.ALPHA,
		},
	)

	watchStreamsConnected = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_streams_connected",
			Help:           "Number of watch streams currently connected and processing events.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	watchRestarts = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_restarts_total",
			Help:           "Total number of watch stream restarts, labeled by HTTP status code.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status_code"},
	)

	watchRestartDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_restart_duration_seconds",
			Help:           "Duration of watch stream restart attempts.",
			Buckets:        []float64{0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
			StabilityLevel: metrics.ALPHA,
		},
	)

	watchStreamLifetimeSeconds = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_stream_lifetime_seconds",
			Help:           "Total lifetime of watch streams from connection to disconnect.",
			Buckets:        []float64{1, 10, 30, 60, 300, 600, 1800, 3600, 7200, 14400, 28800, 86400},
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason"},
	)

	watchStreamMaxUptimeSeconds = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_stream_max_uptime_seconds",
			Help:           "Maximum uptime across all currently connected watch streams.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	watchBookmarkMaxAgeSeconds = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_bookmark_max_age_seconds",
			Help:           "Maximum bookmark age across all watch streams.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// Event processing metrics
	watchEventsReceived = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_events_received_total",
			Help:           "Total number of watch events received, labeled by event type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"event_type"},
	)

	watchEventsProcessed = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "watch_events_processed_total",
			Help:           "Total number of watch events processed, labeled by result.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// Waiter metrics
	waiterRegistrations = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "waiter_registrations_total",
			Help:           "Total number of claim waiter registrations.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	waiterCompletions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "waiter_completions_total",
			Help:           "Total number of claim waiter completions, labeled by result.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	waitersCurrent = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      "quota_enforcement",
			Name:           "waiters_current",
			Help:           "Current number of active claim waiters.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	waiterDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      "quota_enforcement",
			Name:           "waiter_duration_seconds",
			Help:           "Duration from waiter registration to completion, labeled by result.",
			Buckets:        []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60},
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// TTL metrics
	ttlResets = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "ttl_resets_total",
			Help:           "Total number of TTL timer resets.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	ttlExpirations = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      "quota_enforcement",
			Name:           "ttl_expirations_total",
			Help:           "Total number of TTL expirations.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// Admission result metrics
	admissionResultTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "quota",
			Name:           "enforcement_result_total",
			Help:           "Total quota enforcement admission decisions by outcome, policy, policy namespace, and resource type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result", "policy_name", "policy_namespace", "resource_group", "resource_kind"},
	)
)

func init() {
	legacyregistry.MustRegister(watchManagersCreated)
	legacyregistry.MustRegister(watchManagersStopped)
	legacyregistry.MustRegister(watchManagersActive)
	legacyregistry.MustRegister(watchStreamsDesired)
	legacyregistry.MustRegister(watchStreamsConnected)
	legacyregistry.MustRegister(watchRestarts)
	legacyregistry.MustRegister(watchRestartDuration)
	legacyregistry.MustRegister(watchStreamLifetimeSeconds)
	legacyregistry.MustRegister(watchStreamMaxUptimeSeconds)
	legacyregistry.MustRegister(watchBookmarkMaxAgeSeconds)
	legacyregistry.MustRegister(watchEventsReceived)
	legacyregistry.MustRegister(watchEventsProcessed)
	legacyregistry.MustRegister(waiterRegistrations)
	legacyregistry.MustRegister(waiterCompletions)
	legacyregistry.MustRegister(waitersCurrent)
	legacyregistry.MustRegister(waiterDuration)
	legacyregistry.MustRegister(ttlResets)
	legacyregistry.MustRegister(ttlExpirations)
	legacyregistry.MustRegister(admissionResultTotal)
}

// claimResolutionTotal tracks how often each pre-create resolution path fires
// for an existing claim at the predetermined name, so stale-claim behaviour
// on admission retries is visible.
var claimResolutionTotal = metrics.NewCounterVec(
	&metrics.CounterOpts{
		Subsystem:      "quota",
		Name:           "enforcement_existing_claim_resolution_total",
		Help:           "Total resolutions of existing QuotaClaims encountered during admission (granted=short-circuit, denied=conflict returned, deleted_pending=stale claim removed, create_conflict=Create returned AlreadyExists).",
		StabilityLevel: metrics.ALPHA,
	},
	[]string{"resolution"},
)

func init() {
	legacyregistry.MustRegister(claimResolutionTotal)
}

// watchRelists counts how often a watch manager re-read the current
// resource version (startup and after 410 Gone).
var watchRelists = metrics.NewCounter(
	&metrics.CounterOpts{
		Subsystem:      "quota_enforcement",
		Name:           "watch_relists_total",
		Help:           "Total number of times a claim watch manager relisted before (re)starting its watch.",
		StabilityLevel: metrics.ALPHA,
	},
)

func init() {
	legacyregistry.MustRegister(watchRelists)
}
