package dual

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// Metrics live in this package rather than internal/metrics so the dual
// backend stays self-contained; they follow the same component-base
// conventions and register into the same legacy registry.
var (
	// shadowWriteFailures counts shadow writes that failed after the primary
	// write succeeded. Any sustained non-zero rate means Postgres is drifting.
	shadowWriteFailures = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "quota",
			Subsystem:      "dual",
			Name:           "shadow_write_failures_total",
			Help:           "Number of shadow (Postgres) writes that failed after the primary (etcd) write succeeded",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "operation"}, // operation: create, update, delete, backfill, repair
	)

	// shadowBackfills counts objects inserted into the shadow because they
	// were missing there.
	shadowBackfills = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "quota",
			Subsystem:      "dual",
			Name:           "shadow_backfills_total",
			Help:           "Number of objects inserted into the shadow because they were missing",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "source"}, // source: write, reconcile
	)

	// shadowWritesSkipped counts shadow writes dropped by the per-key
	// sequencer because a newer primary state was already mirrored.
	shadowWritesSkipped = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "quota",
			Subsystem:      "dual",
			Name:           "shadow_writes_skipped_total",
			Help:           "Number of shadow writes skipped because a newer primary state was already mirrored",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)

	// divergentObjects is the number of unresolved divergent objects found by
	// the last reconcile pass, after any backfill/repair it performed.
	divergentObjects = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "quota",
			Subsystem:      "dual",
			Name:           "divergent_objects",
			Help:           "Unresolved divergent objects between primary and shadow found by the last reconcile pass",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "reason"}, // reason: missing, orphaned, drifted
	)

	// lastReconcileTimestamp is the unix time of the last completed reconcile
	// pass. Alert when it stops advancing.
	lastReconcileTimestamp = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "quota",
			Subsystem:      "dual",
			Name:           "last_reconcile_timestamp_seconds",
			Help:           "Unix timestamp of the last completed primary/shadow reconcile pass",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)

	// reconcileErrors counts reconcile passes that aborted with an error.
	reconcileErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "quota",
			Subsystem:      "dual",
			Name:           "reconcile_errors_total",
			Help:           "Number of primary/shadow reconcile passes that failed",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)
)

const (
	reasonMissing  = "missing"
	reasonOrphaned = "orphaned"
	reasonDrifted  = "drifted"
)

func init() {
	legacyregistry.MustRegister(
		shadowWriteFailures,
		shadowBackfills,
		shadowWritesSkipped,
		divergentObjects,
		lastReconcileTimestamp,
		reconcileErrors,
	)
}
