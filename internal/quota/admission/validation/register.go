package validation

import (
	"io"
	"net/http"
	"sync"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/klog/v2"
)

var (
	// pluginInstance stores the singleton plugin instance for readiness checks
	pluginInstance *QuotaValidationPlugin
	pluginMutex    sync.RWMutex
)

// Register registers the QuotaValidation admission plugin
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		klog.InfoS("Registered quota validation plugin with apiserver")
		plugin, err := NewQuotaValidationPlugin()
		if err != nil {
			return nil, err
		}

		// Store the plugin instance for access by API server readiness checks
		pluginMutex.Lock()
		pluginInstance = plugin
		pluginMutex.Unlock()

		return plugin, nil
	})
}

// ReadinessCheck returns a readiness check for the quota validator integrated
// into the admission plugin.
//
// NOTE: In an aggregated apiserver that serves its own types via loopback, we
// CANNOT block readiness on the validator cache being synced — the validator's
// dynamic client routes list requests through the aggregator back to us, and
// if we're not ready, those lists fail, preventing the validator from ever
// syncing. This is a chicken-and-egg problem that doesn't affect CRD-based
// deployments.
//
// Instead, we report ready immediately. The validator syncs in the background
// and becomes effective for cross-resource duplicate detection once its cache
// is warm (typically within a few hundred milliseconds after startup).
func ReadinessCheck() healthz.HealthChecker {
	return healthz.NamedCheck("quota-validator", func(r *http.Request) error {
		pluginMutex.RLock()
		plugin := pluginInstance
		pluginMutex.RUnlock()

		if plugin == nil {
			klog.V(4).Info("quota validation plugin not enabled for apiserver")
		}
		return nil
	})
}
