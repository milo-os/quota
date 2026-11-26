// Package etcd provides helpers for configuring etcd3 as a storage backend
// for the quota aggregated API server.
//
// Since k8s.io/apiserver already includes a full etcd3 storage implementation,
// this package is primarily configuration glue. It converts our EtcdConfig into
// the storagebackend.Config that the standard apiserver machinery expects.
package etcd

import (
	"k8s.io/apiserver/pkg/storage/storagebackend"

	quotastorage "go.miloapis.com/quota/internal/storage"
)

// DefaultPrefix is the default etcd key prefix for quota objects.
const DefaultPrefix = "/registry/quota.miloapis.com"

// NewStorageBackendConfig builds a storagebackend.Config from our EtcdConfig.
// The returned config can be passed to the standard apiserver etcd3 storage factory.
func NewStorageBackendConfig(cfg quotastorage.EtcdConfig) *storagebackend.Config {
	sc := storagebackend.NewDefaultConfig(prefix(cfg), nil)
	sc.Transport.ServerList = cfg.Servers
	sc.Transport.CertFile = cfg.CertFile
	sc.Transport.KeyFile = cfg.KeyFile
	sc.Transport.TrustedCAFile = cfg.CAFile
	return sc
}

func prefix(cfg quotastorage.EtcdConfig) string {
	if cfg.Prefix != "" {
		return cfg.Prefix
	}
	return DefaultPrefix
}
