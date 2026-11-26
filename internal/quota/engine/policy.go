package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/metrics"
	legacyregistry "k8s.io/component-base/metrics/legacyregistry"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// Metrics for the policy engine shared informer. Registered once at init.
var (
	policyInformerEventsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "quota",
			Name:           "policy_informer_events_total",
			Help:           "ClaimCreationPolicy events observed by the shared informer.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type"}, // add|update|delete
	)

	policyLoadTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      "quota",
			Name:           "policy_load_total",
			Help:           "Total number of ClaimCreationPolicy load operations.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	policyActiveGauge = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      "quota",
			Name:           "policies_active",
			Help:           "Current number of active ClaimCreationPolicy objects in the cache.",
			StabilityLevel: metrics.ALPHA,
		},
	)
)

func init() {
	// Register metrics with Kubernetes legacy registry so they are exposed on the apiserver /metrics.
	legacyregistry.MustRegister(policyInformerEventsTotal)
	legacyregistry.MustRegister(policyLoadTotal)
	legacyregistry.MustRegister(policyActiveGauge)
}

// ErrPoliciesNotSynced is returned by GetPoliciesForKind until the engine has
// listed every ClaimCreationPolicy and indexed it. Callers must treat it as
// "unknown", never as "no policy applies".
var ErrPoliciesNotSynced = errors.New("ClaimCreationPolicies are not loaded yet")

// PolicyEngine manages ClaimCreationPolicy resources and answers which
// policies apply to a resource kind.
//
// Matching is by API group and kind only: a policy whose trigger names
// example.com/v1 Widget also applies to Widgets created through any other
// served version (v1beta1, v2, ...). The policy's constraints and templates
// are evaluated against the object in the version the request used, so a
// policy that targets a kind served in several versions must only reference
// fields present in all of them, or guard with has().
type PolicyEngine interface {
	// GetPoliciesForKind returns every ready, enabled policy whose trigger
	// targets gk, sorted by name. It returns ErrPoliciesNotSynced until
	// HasSynced is true.
	GetPoliciesForKind(gk schema.GroupKind) ([]*quotav1alpha1.ClaimCreationPolicy, error)

	// HasSynced reports whether the initial list of policies has been
	// loaded and indexed.
	HasSynced() bool

	// Start loads and watches policies. It blocks until the initial list
	// is indexed or the engine is closed.
	Start(ctx context.Context) error

	// Close stops the policy engine and cleans up resources like watchers.
	Close()
}

// policyIndex is a multi-policy index: by name to the policy (and its
// target), and by target kind to the set of policy names. It holds only
// ready, enabled policies. It is safe for concurrent use.
type policyIndex struct {
	mu     sync.RWMutex
	byName map[string]*quotav1alpha1.ClaimCreationPolicy
	byKind map[schema.GroupKind]map[string]struct{}
}

func newPolicyIndex() *policyIndex {
	return &policyIndex{
		byName: map[string]*quotav1alpha1.ClaimCreationPolicy{},
		byKind: map[schema.GroupKind]map[string]struct{}{},
	}
}

func policyTarget(p *quotav1alpha1.ClaimCreationPolicy) schema.GroupKind {
	return p.Spec.Trigger.Resource.GetGVK().GroupKind()
}

// upsert replaces whatever is indexed under the policy's name. Policies that
// are not ready or are disabled are removed instead.
func (i *policyIndex) upsert(p *quotav1alpha1.ClaimCreationPolicy) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.removeLocked(p.Name)
	if !isPolicyActive(p) {
		return
	}
	gk := policyTarget(p)
	i.byName[p.Name] = p.DeepCopy()
	names, ok := i.byKind[gk]
	if !ok {
		names = map[string]struct{}{}
		i.byKind[gk] = names
	}
	names[p.Name] = struct{}{}
	policyActiveGauge.Set(float64(len(i.byName)))
}

func (i *policyIndex) remove(name string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.removeLocked(name)
}

func (i *policyIndex) removeLocked(name string) {
	old, ok := i.byName[name]
	if !ok {
		return
	}
	delete(i.byName, name)
	gk := policyTarget(old)
	if names := i.byKind[gk]; names != nil {
		delete(names, name)
		if len(names) == 0 {
			delete(i.byKind, gk)
		}
	}
	policyActiveGauge.Set(float64(len(i.byName)))
}

func (i *policyIndex) forKind(gk schema.GroupKind) []*quotav1alpha1.ClaimCreationPolicy {
	i.mu.RLock()
	defer i.mu.RUnlock()
	names := i.byKind[gk]
	if len(names) == 0 {
		return nil
	}
	out := make([]*quotav1alpha1.ClaimCreationPolicy, 0, len(names))
	for name := range names {
		out = append(out, i.byName[name])
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out
}

// isPolicyActive reports whether a policy is Ready and not disabled.
func isPolicyActive(p *quotav1alpha1.ClaimCreationPolicy) bool {
	if p.Spec.Disabled != nil && *p.Spec.Disabled {
		return false
	}
	return apimeta.IsStatusConditionTrue(p.Status.Conditions, quotav1alpha1.ClaimCreationPolicyReady)
}

// policyEngine implements PolicyEngine with a shared informer. Informer
// events update the index synchronously, so once the handler registration
// has synced every listed policy is indexed.
type policyEngine struct {
	logger logr.Logger
	index  *policyIndex
	synced atomic.Bool

	clientMu      sync.Mutex
	dynamicClient dynamic.Interface

	stopCh       chan struct{}
	closeOnce    sync.Once
	startOnce    sync.Once
	resyncPeriod time.Duration
}

// NewPolicyEngine creates a policy engine. Call Start to begin loading and
// watching policies.
func NewPolicyEngine(dynamicClient dynamic.Interface, logger logr.Logger) PolicyEngine {
	return &policyEngine{
		dynamicClient: dynamicClient,
		logger:        logger.WithName("policy-engine"),
		index:         newPolicyIndex(),
		stopCh:        make(chan struct{}),
		resyncPeriod:  10 * time.Minute,
	}
}

// SetDynamicClient replaces the client used to list and watch policies. It
// only has an effect before Start.
func (e *policyEngine) SetDynamicClient(c dynamic.Interface) {
	e.clientMu.Lock()
	defer e.clientMu.Unlock()
	e.dynamicClient = c
}

var claimCreationPolicyGVR = schema.GroupVersionResource{
	Group:    quotav1alpha1.SchemeGroupVersion.Group,
	Version:  quotav1alpha1.SchemeGroupVersion.Version,
	Resource: "claimcreationpolicies",
}

// Start begins the policy loading and watching process using a shared
// informer. It returns once every listed policy is indexed.
func (e *policyEngine) Start(ctx context.Context) error {
	var startErr error
	e.startOnce.Do(func() {
		e.clientMu.Lock()
		client := e.dynamicClient
		e.clientMu.Unlock()
		if client == nil {
			startErr = fmt.Errorf("policy engine has no client")
			return
		}
		e.logger.Info("Starting policy engine with shared informer")

		lw := &cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return client.Resource(claimCreationPolicyGVR).List(ctx, options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return client.Resource(claimCreationPolicyGVR).Watch(ctx, options)
			},
		}
		informer := cache.NewSharedIndexInformer(cache.ToListWatcherWithWatchListSemantics(lw, client),
			&unstructured.Unstructured{}, e.resyncPeriod, cache.Indexers{})

		reg, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				policyInformerEventsTotal.WithLabelValues("add").Inc()
				e.applyObject(obj)
			},
			UpdateFunc: func(_, newObj interface{}) {
				policyInformerEventsTotal.WithLabelValues("update").Inc()
				e.applyObject(newObj)
			},
			DeleteFunc: func(obj interface{}) {
				policyInformerEventsTotal.WithLabelValues("delete").Inc()
				e.deleteObject(obj)
			},
		})
		if err != nil {
			startErr = fmt.Errorf("failed to add event handler: %w", err)
			return
		}

		go informer.Run(e.stopCh)

		if !cache.WaitForCacheSync(e.stopCh, informer.HasSynced, reg.HasSynced) {
			startErr = fmt.Errorf("policy engine stopped before policies were loaded")
			return
		}
		e.synced.Store(true)
		e.logger.Info("Policy engine synced")
	})
	return startErr
}

// HasSynced implements PolicyEngine.
func (e *policyEngine) HasSynced() bool { return e.synced.Load() }

// GetPoliciesForKind implements PolicyEngine.
func (e *policyEngine) GetPoliciesForKind(gk schema.GroupKind) ([]*quotav1alpha1.ClaimCreationPolicy, error) {
	if !e.synced.Load() {
		return nil, ErrPoliciesNotSynced
	}
	return e.index.forKind(gk), nil
}

func (e *policyEngine) applyObject(obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		e.logger.Error(nil, "Received non-unstructured object in policy event handler", "type", fmt.Sprintf("%T", obj))
		return
	}
	policyLoadTotal.Inc()
	var p quotav1alpha1.ClaimCreationPolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &p); err != nil {
		// Drop any earlier version: an undecodable policy must not keep
		// enforcing stale rules.
		e.logger.Error(err, "Failed to decode ClaimCreationPolicy; removing it from the index", "policy", u.GetName())
		e.index.remove(u.GetName())
		return
	}
	e.index.upsert(&p)
	e.logger.V(1).Info("Policy indexed", "policy", p.Name, "target", policyTarget(&p), "active", isPolicyActive(&p))
}

func (e *policyEngine) deleteObject(obj interface{}) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	m, err := apimeta.Accessor(obj)
	if err != nil {
		e.logger.Error(err, "Received policy delete without metadata")
		return
	}
	e.index.remove(m.GetName())
	e.logger.V(1).Info("Policy removed from index", "policy", m.GetName())
}

// Close stops the policy engine. It is safe to call more than once.
func (e *policyEngine) Close() {
	e.closeOnce.Do(func() {
		e.logger.V(1).Info("Closing policy engine")
		close(e.stopCh)
	})
}
