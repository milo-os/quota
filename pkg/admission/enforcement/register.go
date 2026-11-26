package enforcement

import (
	"io"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/klog/v2"
)

// Register registers the QuotaEnforcement admission plugin with a factory
// that creates the plugin with nil engines. The engines must be set before
// the plugin processes any requests (via SetPolicyEngine and SetTemplateEngine).
//
// For consumers like Milo that wire the plugin themselves, use
// NewQuotaEnforcementPlugin directly instead of Register.
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		klog.InfoS("Registered quota enforcement plugin with apiserver")
		// Create with nil engines - they must be set before use
		plugin, err := NewQuotaEnforcementPlugin(nil, nil)
		if err != nil {
			return nil, err
		}
		return plugin, nil
	})
}
