package main

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"go.miloapis.com/quota/internal/quota/controllers/core"
	pgstore "go.miloapis.com/quota/internal/storage/postgres"
	admissionenforcement "go.miloapis.com/quota/pkg/admission/enforcement"
)

func TestControllerManagerAllocationMode(t *testing.T) {
	cases := []struct {
		name     string
		backend  string
		mode     string
		bucketNS string
		wantMode string
		wantErr  string
	}{
		{name: "nothing set defaults to controller", wantMode: "controller"},
		{name: "etcd derives controller", backend: "etcd", wantMode: "controller"},
		{name: "dual derives controller", backend: "dual", wantMode: "controller"},
		{name: "postgres derives allocator", backend: "postgres", wantMode: "allocator"},
		{name: "explicit compatible mode", backend: "postgres", mode: "allocator", wantMode: "allocator"},
		{name: "controller on postgres rejected", backend: "postgres", mode: "controller", wantErr: "incompatible"},
		{name: "allocator on etcd rejected", backend: "etcd", mode: "allocator", wantErr: "incompatible"},
		{name: "allocator on dual rejected", backend: "dual", mode: "allocator", wantErr: "incompatible"},
		{name: "unknown backend rejected", backend: "mysql", wantErr: "--quota-storage-backend must be one of"},
		{name: "unknown mode rejected", mode: "magic", wantErr: "--quota-allocation-mode must be one of"},
		{name: "custom bucket namespace in controller mode", backend: "etcd", bucketNS: "buckets", wantMode: "controller"},
		{name: "custom bucket namespace in allocator mode", backend: "postgres", bucketNS: "buckets", wantMode: "allocator"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := NewControllerManagerOptions()
			o.StorageBackend = tc.backend
			o.AllocationMode = tc.mode
			if tc.bucketNS != "" {
				o.BucketNamespace = tc.bucketNS
			}
			err := o.Complete()
			if err == nil {
				err = o.Validate()
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if o.AllocationMode != tc.wantMode {
				t.Fatalf("mode = %q, want %q", o.AllocationMode, tc.wantMode)
			}
		})
	}
}

func TestControllerManagerDefaults(t *testing.T) {
	o := NewControllerManagerOptions()
	if o.BucketNamespace != core.DefaultBucketNamespace || o.StalePendingClaimAge != 5*time.Minute {
		t.Fatalf("unexpected defaults: %+v", o)
	}
	// client-go's own defaults (5 QPS, burst 10) cap controller-mode
	// allocation at about 3 claims/s, so the options must not leave them zero.
	if o.KubeAPIQPS != DefaultKubeAPIQPS || o.KubeAPIBurst != DefaultKubeAPIBurst || o.AllocationWorkers != DefaultAllocationWorkers {
		t.Fatalf("unexpected client and worker defaults: qps=%v burst=%d workers=%d", o.KubeAPIQPS, o.KubeAPIBurst, o.AllocationWorkers)
	}
}

func TestControllerManagerClientFlags(t *testing.T) {
	o := NewControllerManagerOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)
	if err := fs.Parse([]string{"--kube-api-qps=-1", "--kube-api-burst=0", "--quota-allocation-workers=4"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.KubeAPIQPS != -1 || o.KubeAPIBurst != 0 || o.AllocationWorkers != 4 {
		t.Fatalf("flags not applied: qps=%v burst=%d workers=%d", o.KubeAPIQPS, o.KubeAPIBurst, o.AllocationWorkers)
	}
}

func TestServeValidateMessages(t *testing.T) {
	o := NewQuotaServerOptions()
	o.StorageBackend = "bogus"
	err := o.Validate()
	if err == nil || !strings.Contains(err.Error(), "--quota-storage-backend must be one of") {
		t.Fatalf("error = %v", err)
	}
	if o.Enforcement.ClaimTimeout != admissionenforcement.ClaimWaitTimeout || o.Enforcement.ClaimTimeout >= 30*time.Second {
		t.Fatalf("claim timeout %v must default below the 30s webhook timeout", o.Enforcement.ClaimTimeout)
	}
}

func TestServeAllocationMaxBatchFlag(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    int
		wantErr string
	}{
		{name: "default", want: pgstore.DefaultMaxBatchSize},
		{name: "disable batching", args: []string{"--postgres-allocation-max-batch=1"}, want: 1},
		{name: "custom", args: []string{"--postgres-allocation-max-batch=16"}, want: 16},
		{name: "zero rejected", args: []string{"--postgres-allocation-max-batch=0"}, want: 0, wantErr: "--postgres-allocation-max-batch must be at least 1"},
		{name: "negative rejected", args: []string{"--postgres-allocation-max-batch=-4"}, want: -4, wantErr: "--postgres-allocation-max-batch must be at least 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := NewQuotaServerOptions()
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			o.AddFlags(fs)
			if err := fs.Parse(tc.args); err != nil {
				t.Fatalf("parse: %v", err)
			}
			if o.AllocationMaxBatch != tc.want {
				t.Fatalf("AllocationMaxBatch = %d, want %d", o.AllocationMaxBatch, tc.want)
			}
			err := o.Validate()
			if tc.wantErr == "" {
				if err != nil && strings.Contains(err.Error(), "postgres-allocation-max-batch") {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseConsumerSources(t *testing.T) {
	got, err := parseConsumerSources("f", []string{
		"iam.example.com/parent-name=Project.resourcemanager.example.com",
		"tenant=Tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []admissionenforcement.ConsumerSource{
		{Key: "iam.example.com/parent-name", Kind: "Project", APIGroup: "resourcemanager.example.com"},
		{Key: "tenant", Kind: "Tenant"},
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for _, bad := range []string{"novalue", "=Kind", "key=", "key=.group"} {
		if _, err := parseConsumerSources("f", []string{bad}); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestParseConsumerRefSources(t *testing.T) {
	got, err := parseConsumerRefSources("f", []string{
		"iam.example.com/parent-name,iam.example.com/parent-type,iam.example.com/parent-api-group",
		"name,kind",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []admissionenforcement.ConsumerRefSource{
		{NameKey: "iam.example.com/parent-name", KindKey: "iam.example.com/parent-type", APIGroupKey: "iam.example.com/parent-api-group"},
		{NameKey: "name", KindKey: "kind"},
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for _, bad := range []string{"onlyname", ",kind", "name,", "a,b,c,d"} {
		if _, err := parseConsumerRefSources("f", []string{bad}); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}
