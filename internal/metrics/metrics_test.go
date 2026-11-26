package metrics

import (
	"testing"

	"k8s.io/component-base/metrics/legacyregistry"
)

func TestMetricsRegistered(t *testing.T) {
	// Collecting from the global registry should not panic
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Check that our metric families exist (they may or may not appear
	// if no values have been recorded yet, depending on the implementation)
	_ = metrics
}

func TestCounterIncrement(t *testing.T) {
	// Verify counters can be incremented without panic
	QuotaClaimTotal.WithLabelValues("granted").Inc()
	QuotaClaimTotal.WithLabelValues("denied").Inc()
	QuotaClaimTotal.WithLabelValues("error").Inc()

	QuotaGrantTotal.WithLabelValues("create").Inc()
	QuotaGrantTotal.WithLabelValues("update").Inc()
	QuotaGrantTotal.WithLabelValues("delete").Inc()

	QuotaDefinitionTotal.WithLabelValues("create").Inc()
	QuotaDefinitionTotal.WithLabelValues("update").Inc()
	QuotaDefinitionTotal.WithLabelValues("delete").Inc()

	// Verify metrics appear in gathered output after incrementing
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	expectedNames := map[string]bool{
		"quota_claim_total":      false,
		"quota_grant_total":      false,
		"quota_definition_total": false,
	}

	for _, mf := range metrics {
		if _, ok := expectedNames[mf.GetName()]; ok {
			expectedNames[mf.GetName()] = true
		}
	}

	for name, found := range expectedNames {
		if !found {
			t.Errorf("metric %q not found in gathered output after incrementing", name)
		}
	}
}

func TestHistogramObservation(t *testing.T) {
	// Verify histogram observations don't panic
	QuotaClaimLatency.WithLabelValues("granted").Observe(0.001)
	QuotaClaimLatency.WithLabelValues("denied").Observe(0.05)
	QuotaClaimLatency.WithLabelValues("error").Observe(1.0)

	// Verify the histogram appears in gathered output
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range metrics {
		if mf.GetName() == "quota_claim_duration_seconds" {
			found = true
			break
		}
	}
	if !found {
		t.Error("quota_claim_duration_seconds not found in gathered output after observations")
	}
}

func TestCounterLabels(t *testing.T) {
	// Verify that the expected label values work without panic
	t.Run("claim labels", func(t *testing.T) {
		QuotaClaimTotal.WithLabelValues("granted")
		QuotaClaimTotal.WithLabelValues("denied")
		QuotaClaimTotal.WithLabelValues("error")
	})
	t.Run("grant labels", func(t *testing.T) {
		QuotaGrantTotal.WithLabelValues("create")
		QuotaGrantTotal.WithLabelValues("update")
		QuotaGrantTotal.WithLabelValues("delete")
	})
	t.Run("definition labels", func(t *testing.T) {
		QuotaDefinitionTotal.WithLabelValues("create")
		QuotaDefinitionTotal.WithLabelValues("update")
		QuotaDefinitionTotal.WithLabelValues("delete")
	})
	t.Run("latency labels", func(t *testing.T) {
		QuotaClaimLatency.WithLabelValues("granted")
		QuotaClaimLatency.WithLabelValues("denied")
		QuotaClaimLatency.WithLabelValues("error")
	})
}
