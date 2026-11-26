// Package metrics defines Prometheus metrics for the quota service.
package metrics

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// ClaimResultError is the QuotaClaimTotal result for an allocation that
// failed with a server-side error (a storage or apply failure), as opposed
// to a quota decision or a client error such as a missing bucket.
const ClaimResultError = "error"

var (
	// QuotaClaimTotal counts total quota claims processed by result.
	QuotaClaimTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "quota",
			Name:           "claim_total",
			Help:           "Total number of quota claims processed",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"}, // "granted", "denied", "partial", "error"
	)

	// QuotaClaimLatency tracks the latency of quota claim processing.
	QuotaClaimLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      "quota",
			Name:           "claim_duration_seconds",
			Help:           "Duration of quota claim processing in seconds",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// QuotaAllocationBatchSize records how many claim allocations and
	// releases the Postgres allocator committed together in one
	// transaction (group commit). A batch of 1 is the unbatched path; the
	// distribution shifts up as concurrent load grows.
	QuotaAllocationBatchSize = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Namespace:      "quota",
			Name:           "allocation_batch_size",
			Help:           "Number of quota claim allocations and releases committed in one Postgres transaction",
			Buckets:        metrics.ExponentialBuckets(1, 2, 9), // 1 .. 256
			StabilityLevel: metrics.ALPHA,
		},
	)

	// QuotaGrantTotal counts total quota grants created.
	QuotaGrantTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "quota",
			Name:           "grant_total",
			Help:           "Total number of quota grants created",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation"}, // "create", "update", "delete"
	)

	// QuotaDefinitionTotal counts total quota definitions created.
	QuotaDefinitionTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "quota",
			Name:           "definition_total",
			Help:           "Total number of quota definitions created",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation"}, // "create", "update", "delete"
	)

	// QuotaClaimNativeActiveWatchers tracks the number of active native
	// postgres-backed watch streams for QuotaClaim. Each native watcher
	// holds one pgx session connection from the listen pool, so this
	// doubles as a pool-utilization signal.
	QuotaClaimNativeActiveWatchers = metrics.NewGauge(
		&metrics.GaugeOpts{
			Namespace:      "quota",
			Name:           "claim_native_active_watchers",
			Help:           "Number of active postgres-native QuotaClaim watch streams",
			StabilityLevel: metrics.ALPHA,
		},
	)
)

func init() {
	legacyregistry.MustRegister(
		QuotaClaimTotal,
		QuotaClaimLatency,
		QuotaAllocationBatchSize,
		QuotaGrantTotal,
		QuotaDefinitionTotal,
		QuotaClaimNativeActiveWatchers,
	)
}
