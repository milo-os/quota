package admission

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"go.miloapis.com/quota/pkg/admission/enforcement"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

type countingPolicyEngine struct {
	started, closed atomic.Int32
	startCtx        atomic.Value
	synced          atomic.Bool
	client          atomic.Value
}

func (e *countingPolicyEngine) GetPoliciesForKind(schema.GroupKind) ([]*quotav1alpha1.ClaimCreationPolicy, error) {
	return nil, nil
}
func (e *countingPolicyEngine) HasSynced() bool { return e.synced.Load() }
func (e *countingPolicyEngine) SetDynamicClient(c dynamic.Interface) {
	e.client.Store(c)
}
func (e *countingPolicyEngine) Start(ctx context.Context) error {
	e.startCtx.Store(ctx)
	e.started.Add(1)
	return nil
}
func (e *countingPolicyEngine) Close() { e.closed.Add(1) }

// TestPolicyEngineFollowsServerLifecycle verifies 6e: the policy engine is
// started with the server's context by the post-start hook and closed when
// that context ends, instead of running on context.Background forever.
func TestPolicyEngineFollowsServerLifecycle(t *testing.T) {
	plugin, err := enforcement.NewQuotaEnforcementPlugin(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	pe := &countingPolicyEngine{}
	registeredEnforcementPlugin = &enforcementPluginWithAutoInit{QuotaEnforcementPlugin: plugin, policyEngine: pe}
	t.Cleanup(func() { registeredEnforcementPlugin = nil })

	hook := EnforcementPostStartHook()
	if hook == nil {
		t.Fatal("expected a post-start hook")
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := hook(genericapiserver.PostStartHookContext{Context: ctx}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return pe.started.Load() == 1 })
	if pe.closed.Load() != 0 {
		t.Fatal("engine closed before the server stopped")
	}
	cancel()
	waitFor(t, func() bool { return pe.closed.Load() == 1 })
}

func TestNoHookWithoutPlugin(t *testing.T) {
	registeredEnforcementPlugin = nil
	if EnforcementPostStartHook() != nil {
		t.Fatal("expected no hook when the plugin is not registered")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The readiness check fails until the policy engine has synced, and passes
// when the enforcement plugin is not enabled at all.
func TestEnforcementReadinessWaitsForPolicies(t *testing.T) {
	registeredEnforcementPlugin = nil
	check := EnforcementReadinessCheck()
	if err := check.Check(nil); err != nil {
		t.Fatalf("no plugin registered: expected ready, got %v", err)
	}

	plugin, err := enforcement.NewQuotaEnforcementPlugin(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	pe := &countingPolicyEngine{}
	registeredEnforcementPlugin = &enforcementPluginWithAutoInit{QuotaEnforcementPlugin: plugin, policyEngine: pe}
	t.Cleanup(func() { registeredEnforcementPlugin = nil })

	if err := check.Check(nil); err == nil {
		t.Fatal("expected not ready before policies are loaded")
	}
	pe.synced.Store(true)
	if err := check.Check(nil); err != nil {
		t.Fatalf("expected ready after sync, got %v", err)
	}
}

// The post-start hook points the policy engine at the loopback config so it
// does not depend on the aggregation layer routing to this (not yet ready)
// replica.
func TestPolicyEngineUsesLoopback(t *testing.T) {
	plugin, err := enforcement.NewQuotaEnforcementPlugin(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	pe := &countingPolicyEngine{}
	registeredEnforcementPlugin = &enforcementPluginWithAutoInit{QuotaEnforcementPlugin: plugin, policyEngine: pe}
	t.Cleanup(func() { registeredEnforcementPlugin = nil })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hook := EnforcementPostStartHook()
	if err := hook(genericapiserver.PostStartHookContext{Context: ctx, LoopbackClientConfig: &rest.Config{Host: "https://127.0.0.1:6443"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return pe.started.Load() == 1 })
	if pe.client.Load() == nil {
		t.Fatal("policy engine was not given a loopback client")
	}
}
