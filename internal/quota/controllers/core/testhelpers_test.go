package core

import (
	"context"
	"net/http"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// testCluster implements cluster.Cluster with only GetClient and
// GetAPIReader functional. client stands in for the cache-backed client;
// reader, when set, is the uncached apiserver reader (defaults to client).
type testCluster struct {
	client client.Client
	reader client.Reader
}

func (c *testCluster) GetClient() client.Client                        { return c.client }
func (c *testCluster) GetScheme() *runtime.Scheme                      { return nil }
func (c *testCluster) GetHTTPClient() *http.Client                     { return nil }
func (c *testCluster) GetConfig() *rest.Config                         { return nil }
func (c *testCluster) GetCache() cache.Cache                           { return nil }
func (c *testCluster) GetFieldIndexer() client.FieldIndexer            { return nil }
func (c *testCluster) GetEventRecorderFor(string) record.EventRecorder { return nil }
func (c *testCluster) GetEventRecorder(string) events.EventRecorder    { return nil }
func (c *testCluster) GetRESTMapper() meta.RESTMapper                  { return nil }
func (c *testCluster) GetAPIReader() client.Reader {
	if c.reader != nil {
		return c.reader
	}
	return c.client
}
func (c *testCluster) Start(context.Context) error { return nil }

// testManager implements mcmanager.Manager with only GetCluster functional.
type testManager struct{ cluster cluster.Cluster }

var _ mcmanager.Manager = &testManager{}

func (m *testManager) GetCluster(_ context.Context, _ multicluster.ClusterName) (cluster.Cluster, error) {
	return m.cluster, nil
}
func (m *testManager) Add(mcmanager.Runnable) error                            { return nil }
func (m *testManager) Elected() <-chan struct{}                                { return nil }
func (m *testManager) AddMetricsServerExtraHandler(string, http.Handler) error { return nil }
func (m *testManager) AddHealthzCheck(string, healthz.Checker) error           { return nil }
func (m *testManager) AddReadyzCheck(string, healthz.Checker) error            { return nil }
func (m *testManager) Start(context.Context) error                             { return nil }
func (m *testManager) GetWebhookServer() webhook.Server                        { return nil }
func (m *testManager) GetLogger() logr.Logger                                  { return logr.Discard() }
func (m *testManager) GetControllerOptions() config.Controller                 { return config.Controller{} }
func (m *testManager) ClusterFromContext(context.Context) (cluster.Cluster, error) {
	return nil, nil
}
func (m *testManager) GetManager(context.Context, multicluster.ClusterName) (manager.Manager, error) {
	return nil, nil
}
func (m *testManager) GetLocalManager() manager.Manager     { return nil }
func (m *testManager) GetProvider() multicluster.Provider   { return nil }
func (m *testManager) GetFieldIndexer() client.FieldIndexer { return nil }
func (m *testManager) Engage(context.Context, multicluster.ClusterName, cluster.Cluster) error {
	return nil
}
