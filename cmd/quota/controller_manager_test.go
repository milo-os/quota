package main

import (
	"strings"
	"testing"
)

func TestControllerManagerValidateWorkersAndMetrics(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(o *ControllerManagerOptions)
		wantErr string
	}{
		{name: "defaults", mutate: func(*ControllerManagerOptions) {}},
		{name: "zero workers", mutate: func(o *ControllerManagerOptions) { o.AllocationWorkers = 0 }, wantErr: "--quota-allocation-workers"},
		{name: "negative workers", mutate: func(o *ControllerManagerOptions) { o.AllocationWorkers = -1 }, wantErr: "--quota-allocation-workers"},
		{name: "one worker", mutate: func(o *ControllerManagerOptions) { o.AllocationWorkers = 1 }},
		{name: "insecure metrics on all interfaces", mutate: func(o *ControllerManagerOptions) {
			o.MetricsSecure = false
			o.MetricsAddr = ":8080"
		}, wantErr: "loopback"},
		{name: "insecure metrics on pod IP", mutate: func(o *ControllerManagerOptions) {
			o.MetricsSecure = false
			o.MetricsAddr = "10.0.0.5:8080"
		}, wantErr: "loopback"},
		{name: "insecure metrics on loopback", mutate: func(o *ControllerManagerOptions) {
			o.MetricsSecure = false
			o.MetricsAddr = "127.0.0.1:8080"
		}},
		{name: "insecure metrics on localhost", mutate: func(o *ControllerManagerOptions) {
			o.MetricsSecure = false
			o.MetricsAddr = "localhost:8080"
		}},
		{name: "insecure metrics disabled", mutate: func(o *ControllerManagerOptions) {
			o.MetricsSecure = false
			o.MetricsAddr = "0"
		}},
		{name: "secure metrics on all interfaces", mutate: func(o *ControllerManagerOptions) { o.MetricsAddr = ":8443" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := NewControllerManagerOptions()
			tt.mutate(o)
			if err := o.Complete(); err != nil {
				t.Fatalf("Complete: %v", err)
			}
			err := o.Validate()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestControllerManagerMetricsOptions(t *testing.T) {
	o := NewControllerManagerOptions()
	if !o.MetricsSecure || o.MetricsAddr != ":8443" {
		t.Fatalf("metrics must default to secure serving on :8443, got secure=%v addr=%q", o.MetricsSecure, o.MetricsAddr)
	}
	m := o.metricsOptions()
	if !m.SecureServing || m.FilterProvider == nil {
		t.Fatalf("secure metrics must use TLS and the authn/authz filter: %+v", m)
	}

	o.MetricsSecure = false
	o.MetricsAddr = "127.0.0.1:8080"
	m = o.metricsOptions()
	if m.SecureServing || m.FilterProvider != nil || m.BindAddress != "127.0.0.1:8080" {
		t.Fatalf("insecure metrics must be plain HTTP without a filter: %+v", m)
	}
}
