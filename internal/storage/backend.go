// Package storage provides configuration and factory functions for selecting
// the quota service storage backend (etcd, Postgres, or dual-write).
package storage

import (
	"fmt"

	"k8s.io/apiserver/pkg/registry/generic"

	pgstore "go.miloapis.com/quota/internal/storage/postgres"
)

// BackendType represents the storage backend to use.
type BackendType string

const (
	// BackendEtcd uses etcd3 as the storage backend (default, matches k8s.io/apiserver built-in).
	BackendEtcd BackendType = "etcd"
	// BackendPostgres uses PostgreSQL as the storage backend.
	BackendPostgres BackendType = "postgres"
	// BackendDual writes to both etcd and Postgres (reads from etcd).
	BackendDual BackendType = "dual"
)

// Config holds configuration for all storage backends.
type Config struct {
	Backend  BackendType
	Etcd     EtcdConfig
	Postgres PostgresConfig
}

// EtcdConfig holds etcd-specific configuration.
type EtcdConfig struct {
	// Servers is the list of etcd server endpoints.
	Servers []string
	// Prefix is the key prefix for all stored objects.
	Prefix string
	// CertFile is the path to the client TLS certificate.
	CertFile string
	// KeyFile is the path to the client TLS key.
	KeyFile string
	// CAFile is the path to the CA certificate for verifying the etcd server.
	CAFile string
}

// PostgresConfig holds Postgres-specific configuration.
type PostgresConfig struct {
	// DSN is the Postgres data source name (connection string).
	DSN string
}

// NewRESTOptionsGetter returns a generic.RESTOptionsGetter based on the configured backend type.
//
// For etcd, the caller should use the standard k8s.io/apiserver etcd options
// (RecommendedOptions.Etcd) which already provides a RESTOptionsGetter. This
// function only handles the Postgres case.
//
// For dual-write, the caller should compose backends using the dual.Store
// wrapper at the storage.Interface level rather than at the RESTOptionsGetter level.
func NewRESTOptionsGetter(cfg Config) (generic.RESTOptionsGetter, error) {
	switch cfg.Backend {
	case BackendEtcd:
		return nil, fmt.Errorf("etcd backend should use the standard apiserver etcd RESTOptionsGetter; do not call NewRESTOptionsGetter for etcd")
	case BackendPostgres:
		return pgstore.NewRESTOptionsGetter(cfg.Postgres.DSN)
	case BackendDual:
		return nil, fmt.Errorf("dual backend RESTOptionsGetter not yet implemented; use dual.Store wrapper directly")
	default:
		return nil, fmt.Errorf("unknown storage backend: %q", cfg.Backend)
	}
}
