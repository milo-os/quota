package storage

import (
	"testing"
)

func TestNewRESTOptionsGetter_BackendEtcd(t *testing.T) {
	cfg := Config{
		Backend: BackendEtcd,
		Etcd: EtcdConfig{
			Servers: []string{"localhost:2379"},
		},
	}
	// Etcd backend is expected to return an error instructing the caller
	// to use the standard apiserver etcd RESTOptionsGetter.
	_, err := NewRESTOptionsGetter(cfg)
	if err == nil {
		t.Fatal("expected error for etcd backend (should use standard RESTOptionsGetter)")
	}
}

func TestNewRESTOptionsGetter_BackendPostgres_NoDSN(t *testing.T) {
	cfg := Config{
		Backend: BackendPostgres,
		Postgres: PostgresConfig{
			DSN: "",
		},
	}
	// With an empty DSN, we expect an error (connection will fail).
	_, err := NewRESTOptionsGetter(cfg)
	if err == nil {
		t.Fatal("expected error for empty postgres DSN")
	}
}

func TestNewRESTOptionsGetter_BackendDual(t *testing.T) {
	cfg := Config{
		Backend: BackendDual,
	}
	// Dual backend should return an error indicating it's not yet implemented
	// at the RESTOptionsGetter level.
	_, err := NewRESTOptionsGetter(cfg)
	if err == nil {
		t.Fatal("expected error for dual backend")
	}
}

func TestNewRESTOptionsGetter_UnknownBackend(t *testing.T) {
	cfg := Config{
		Backend: BackendType("unknown"),
	}
	_, err := NewRESTOptionsGetter(cfg)
	if err == nil {
		t.Fatal("expected error for unknown backend")
	}
}

func TestBackendTypeConstants(t *testing.T) {
	tests := []struct {
		name     string
		backend  BackendType
		expected string
	}{
		{"etcd", BackendEtcd, "etcd"},
		{"postgres", BackendPostgres, "postgres"},
		{"dual", BackendDual, "dual"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if string(tc.backend) != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, string(tc.backend))
			}
		})
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{}
	if cfg.Backend != "" {
		t.Errorf("expected empty default backend, got %q", cfg.Backend)
	}
	if cfg.Etcd.Prefix != "" {
		t.Errorf("expected empty default etcd prefix, got %q", cfg.Etcd.Prefix)
	}
	if cfg.Postgres.DSN != "" {
		t.Errorf("expected empty default postgres DSN, got %q", cfg.Postgres.DSN)
	}
}

func TestEtcdConfig_Fields(t *testing.T) {
	cfg := EtcdConfig{
		Servers:  []string{"server1:2379", "server2:2379"},
		Prefix:   "/quota",
		CertFile: "/path/to/cert",
		KeyFile:  "/path/to/key",
		CAFile:   "/path/to/ca",
	}
	if len(cfg.Servers) != 2 {
		t.Errorf("expected 2 servers, got %d", len(cfg.Servers))
	}
	if cfg.Prefix != "/quota" {
		t.Errorf("expected prefix '/quota', got %q", cfg.Prefix)
	}
	if cfg.CertFile != "/path/to/cert" {
		t.Errorf("unexpected CertFile: %q", cfg.CertFile)
	}
	if cfg.KeyFile != "/path/to/key" {
		t.Errorf("unexpected KeyFile: %q", cfg.KeyFile)
	}
	if cfg.CAFile != "/path/to/ca" {
		t.Errorf("unexpected CAFile: %q", cfg.CAFile)
	}
}

func TestPostgresConfig_Fields(t *testing.T) {
	cfg := PostgresConfig{
		DSN: "postgres://user:pass@localhost:5432/quota?sslmode=disable",
	}
	if cfg.DSN == "" {
		t.Error("expected non-empty DSN")
	}
}
