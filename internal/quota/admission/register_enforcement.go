package admission

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	"k8s.io/apiserver/pkg/admission"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"go.miloapis.com/quota/internal/quota/engine"
	"go.miloapis.com/quota/pkg/admission/enforcement"
)

// EnforcementPostStartHookName names the apiserver post-start hook that runs
// the enforcement plugin's policy engine.
const EnforcementPostStartHookName = "quota-enforcement-policy-engine"

// enforcementPluginWithAutoInit wraps the enforcement plugin to auto-initialize
// engines when the dynamic client is set. This is used by the quota service itself
// to wire internal engine implementations into the exported enforcement plugin.
//
// The policy engine is built when the dynamic client is injected but is not
// started then: admission initialization happens before the server has a
// lifecycle context. RunPolicyEngine starts it under the server's context and
// closes it when that context ends.
type enforcementPluginWithAutoInit struct {
	*enforcement.QuotaEnforcementPlugin

	mu           sync.Mutex
	policyEngine engine.PolicyEngine
}

// EnforcementPlugin returns the underlying QuotaEnforcementPlugin. This is used
// by the webhook handler to delegate AdmissionReview requests to the same
// enforcement logic that runs in the aggregated apiserver's admission chain.
func (p *enforcementPluginWithAutoInit) EnforcementPlugin() *enforcement.QuotaEnforcementPlugin {
	return p.QuotaEnforcementPlugin
}

func (p *enforcementPluginWithAutoInit) SetDynamicClient(client dynamic.Interface) {
	p.QuotaEnforcementPlugin.SetDynamicClient(client)

	if client == nil {
		return
	}
	logger := klog.NewKlogr().WithName("quota-enforcement-plugin")

	celEngine, err := engine.NewCELEngine()
	if err != nil {
		logger.Error(err, "Failed to create CEL engine")
		return
	}

	templateEngine := engine.NewTemplateEngine(celEngine, logger.WithName("template"))
	policyEngine := engine.NewPolicyEngine(client, logger)

	p.SetPolicyEngine(NewPolicyEngineAdapter(policyEngine))
	p.SetTemplateEngine(NewTemplateEngineAdapter(templateEngine))

	p.mu.Lock()
	p.policyEngine = policyEngine
	p.mu.Unlock()

	logger.V(2).Info("Engines initialized for enforcement plugin; policy engine starts with the server")
}

// RunPolicyEngine starts the policy engine under ctx and closes it once ctx
// is done. It returns immediately; startup errors are logged.
//
// When loopback is set the engine lists policies through it. The admission
// dynamic client reaches this server through kube-apiserver's aggregation
// layer, which stops routing to a replica that is not ready, and readiness
// waits for the policies: loopback avoids that deadlock on a cold start.
func (p *enforcementPluginWithAutoInit) RunPolicyEngine(ctx context.Context, loopback *rest.Config) {
	p.mu.Lock()
	pe := p.policyEngine
	p.mu.Unlock()
	if pe == nil {
		klog.InfoS("Quota enforcement policy engine not initialized; nothing to start")
		return
	}
	if loopback != nil {
		if settable, ok := pe.(interface{ SetDynamicClient(dynamic.Interface) }); ok {
			client, err := dynamic.NewForConfig(loopback)
			if err != nil {
				klog.ErrorS(err, "Failed to create loopback client for the quota policy engine; using the admission client")
			} else {
				settable.SetDynamicClient(client)
			}
		}
	}
	go func() {
		if err := pe.Start(ctx); err != nil {
			klog.ErrorS(err, "Failed to start quota enforcement policy engine")
		}
	}()
	go func() {
		<-ctx.Done()
		klog.V(2).InfoS("Stopping quota enforcement policy engine")
		pe.Close()
	}()
}

// registeredEnforcementPlugin holds a reference to the enforcement plugin
// created during admission plugin registration. It is used by
// GetEnforcementPlugin to retrieve the plugin for the webhook handler.
var registeredEnforcementPlugin *enforcementPluginWithAutoInit

// RegisterEnforcement registers the QuotaEnforcement admission plugin with
// auto-initialization of internal engine implementations. This should be used
// by the quota service binary. External consumers (like Milo) should use
// enforcement.NewQuotaEnforcementPlugin directly with their own engines.
//
// optsFn, when non-nil, is called when the plugin is constructed (after
// flags are parsed) and its options are passed to
// enforcement.NewQuotaEnforcementPlugin.
func RegisterEnforcement(plugins *admission.Plugins, optsFn func() []enforcement.Option) {
	plugins.Register(enforcement.PluginName, func(config io.Reader) (admission.Interface, error) {
		klog.InfoS("Registered quota enforcement plugin with apiserver (with auto-init)")
		var opts []enforcement.Option
		if optsFn != nil {
			opts = optsFn()
		}
		plugin, err := enforcement.NewQuotaEnforcementPlugin(nil, nil, opts...)
		if err != nil {
			return nil, err
		}
		wrapper := &enforcementPluginWithAutoInit{QuotaEnforcementPlugin: plugin}
		registeredEnforcementPlugin = wrapper
		return wrapper, nil
	})
}

// GetEnforcementPlugin returns the enforcement plugin instance created during
// admission registration. Returns nil if RegisterEnforcement has not been
// called or the plugin factory has not yet been invoked. This is used by the
// serve command to pass the plugin to the webhook handler.
func GetEnforcementPlugin() *enforcement.QuotaEnforcementPlugin {
	if registeredEnforcementPlugin == nil {
		return nil
	}
	return registeredEnforcementPlugin.EnforcementPlugin()
}

// EnforcementPostStartHook returns a post-start hook that runs the
// registered enforcement plugin's policy engine for the lifetime of the
// server and the plugin's watch managers until the server stops. Returns nil
// when no enforcement plugin was registered.
func EnforcementPostStartHook() genericapiserver.PostStartHookFunc {
	wrapper := registeredEnforcementPlugin
	if wrapper == nil {
		return nil
	}
	return func(hookCtx genericapiserver.PostStartHookContext) error {
		wrapper.RunPolicyEngine(hookCtx, hookCtx.LoopbackClientConfig)
		go func() {
			<-hookCtx.Done()
			wrapper.Shutdown()
		}()
		return nil
	}
}

// EnforcementReadinessCheck fails until the registered enforcement plugin's
// policy engine has loaded and indexed every ClaimCreationPolicy, so a new or
// restarted replica receives no traffic it would have to reject. It passes
// when the enforcement plugin is not enabled.
func EnforcementReadinessCheck() healthz.HealthChecker {
	return healthz.NamedCheck("quota-enforcement-policies", func(*http.Request) error {
		wrapper := registeredEnforcementPlugin
		if wrapper == nil {
			return nil
		}
		wrapper.mu.Lock()
		pe := wrapper.policyEngine
		wrapper.mu.Unlock()
		if pe == nil {
			return fmt.Errorf("quota enforcement policy engine is not initialized")
		}
		if !pe.HasSynced() {
			return fmt.Errorf("ClaimCreationPolicies are not loaded yet")
		}
		return nil
	})
}
