package main

import (
	"strings"
	"testing"
)

// Validate runs the generic apiserver option checks, requiring etcd servers
// only for the backends that use etcd.
func TestServeValidateRecommendedOptions(t *testing.T) {
	cases := []struct {
		name       string
		backend    string
		etcd       []string
		securePort int
		wantErr    string
		notWant    string
	}{
		{name: "etcd without servers", backend: "etcd", wantErr: "--etcd-servers must be specified"},
		{name: "dual without servers", backend: "dual", wantErr: "--etcd-servers must be specified"},
		{name: "postgres needs no etcd", backend: "postgres", notWant: "--etcd-servers"},
		{name: "etcd with servers", backend: "etcd", etcd: []string{"http://etcd:2379"}, notWant: "--etcd-servers"},
		{name: "serving options are checked", backend: "etcd", etcd: []string{"http://etcd:2379"}, securePort: 70000, wantErr: "--secure-port"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := NewQuotaServerOptions()
			o.StorageBackend = tc.backend
			o.PostgresDSN = "postgres://x"
			o.RecommendedOptions.Etcd.StorageConfig.Transport.ServerList = tc.etcd
			if tc.securePort != 0 {
				o.RecommendedOptions.SecureServing.BindPort = tc.securePort
			}
			err := o.Validate()
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
			if tc.notWant != "" && err != nil && strings.Contains(err.Error(), tc.notWant) {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
	// Validate must not mutate the options Config later relies on.
	o := NewQuotaServerOptions()
	o.StorageBackend = "postgres"
	o.PostgresDSN = "postgres://x"
	_ = o.Validate()
	if o.RecommendedOptions.Etcd == nil {
		t.Fatal("Validate cleared the etcd options")
	}
}

// The enforcement webhook must never bypass authorization.
func TestServeValidateRejectsAlwaysAllowWebhook(t *testing.T) {
	cases := []struct {
		paths   []string
		wantErr bool
	}{
		{paths: []string{"/healthz", "/readyz", "/livez"}},
		{paths: []string{"/webhook/other"}},
		{paths: []string{"/healthz", "/webhook/enforce"}, wantErr: true},
		{paths: []string{"/webhook/enforce/"}, wantErr: true},
		{paths: []string{"/webhook/*"}, wantErr: true},
		{paths: []string{"/*"}, wantErr: true},
		{paths: []string{"/webhook/enf*"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.paths, ","), func(t *testing.T) {
			o := NewQuotaServerOptions()
			o.StorageBackend = "postgres"
			o.PostgresDSN = "postgres://x"
			o.RecommendedOptions.Authorization.AlwaysAllowPaths = tc.paths
			err := o.Validate()
			got := err != nil && strings.Contains(err.Error(), "--authorization-always-allow-paths")
			if got != tc.wantErr {
				t.Fatalf("paths %v: error = %v, wantErr %v", tc.paths, err, tc.wantErr)
			}
		})
	}
}
