// Package controllers provides a simplified setup function for all quota controllers.
package controllers

import (
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/client-go/dynamic"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	"go.miloapis.com/quota/internal/quota/controllers/core"
	"go.miloapis.com/quota/internal/quota/controllers/lifecycle"
	"go.miloapis.com/quota/internal/quota/controllers/policy"
	"go.miloapis.com/quota/internal/quota/engine"
	"go.miloapis.com/quota/internal/quota/informer"
	"go.miloapis.com/quota/internal/quota/validation"
)

// AllocationMode selects which component writes QuotaClaim and QuotaBucket
// allocation status. In AllocationModeController (the default, etcd/dual
// world) the QuotaBucketAllocationController and QuotaClaimController are
// registered with the manager. In AllocationModeAllocator (the Postgres
// world) the synchronous allocator in the claim CREATE request path owns
// all allocation writes and neither reconciler is registered.
type AllocationMode string

const (
	AllocationModeController AllocationMode = "controller"
	AllocationModeAllocator  AllocationMode = "allocator"
)

// Options configures SetupQuotaControllers.
type Options struct {
	// AllocationMode selects which component writes allocation status.
	// Empty means AllocationModeController.
	AllocationMode AllocationMode

	// BucketNamespace is the namespace QuotaBuckets are placed in when the
	// consumerRef is not namespaced. Empty means core.DefaultBucketNamespace.
	// It must match the apiserver's --quota-bucket-namespace so the
	// synchronous allocator and the controllers address the same buckets.
	BucketNamespace string

	// StalePendingClaimAge is how long an auto-created claim may stay
	// pending before the cleanup controller deletes it. Zero means
	// lifecycle.DefaultStalePendingClaimAge.
	StalePendingClaimAge time.Duration

	// AllocationWorkers is how many buckets the allocation controller and
	// how many claims the claim controller reconcile concurrently in
	// controller mode. Zero means one, controller-runtime's default.
	AllocationWorkers int
}

// SetupQuotaControllers registers all quota controllers with the provided multicluster manager.
//
// Parameters:
//   - mgr: Multicluster controller manager
//   - dynamicClient: Dynamic client for resource type validation
//   - informerManager: Informer manager for dynamic watches (used by grant creation controller)
//   - opts: Allocation mode, bucket placement and cleanup tuning
//   - logger: Logger for quota controller operations
func SetupQuotaControllers(mgr mcmanager.Manager, dynamicClient dynamic.Interface, informerManager informer.Manager, opts Options, logger logr.Logger) error {
	logger.Info("Setting up quota controllers with multicluster support")

	allocationMode := opts.AllocationMode
	if allocationMode == "" {
		allocationMode = AllocationModeController
	}

	standardMgr := mgr.GetLocalManager()

	// Create shared validation components
	sharedResourceTypeValidator := validation.NewResourceTypeValidator(dynamicClient)
	logger.Info("Shared ResourceTypeValidator created, will sync in background")

	celValidator, err := validation.NewCELValidator()
	if err != nil {
		return fmt.Errorf("failed to create CEL validator: %w", err)
	}

	celEngine, err := engine.NewCELEngine()
	if err != nil {
		return fmt.Errorf("failed to create CEL engine: %w", err)
	}

	// 1. QuotaDefinition controller (foundational - core cluster only)
	logger.V(1).Info("Setting up QuotaDefinition controller (core cluster only)")
	if err := (&core.QuotaDefinitionController{
		Scheme:  standardMgr.GetScheme(),
		Manager: mgr,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup QuotaDefinitionController: %w", err)
	}

	// 2. QuotaGrant controller (all clusters)
	logger.V(1).Info("Setting up QuotaGrant controller (all clusters)")
	grantValidator := validation.NewQuotaGrantValidator(sharedResourceTypeValidator)
	if err := (&core.QuotaGrantController{
		Scheme:          standardMgr.GetScheme(),
		Manager:         mgr,
		GrantValidator:  grantValidator,
		BucketNamespace: opts.BucketNamespace,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup QuotaGrantController: %w", err)
	}

	// 3. QuotaBucket controller (grant-derived Limit/GrantCount - always on)
	logger.V(1).Info("Setting up QuotaBucket controller (all clusters)")
	if err := (&core.QuotaBucketController{
		Scheme:          standardMgr.GetScheme(),
		Manager:         mgr,
		BucketNamespace: opts.BucketNamespace,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup QuotaBucketController: %w", err)
	}

	// 4. Allocation writers — only when the controller is responsible for
	// allocation state. In allocator mode the synchronous Postgres
	// allocator owns these writes and registering these reconcilers would
	// race on the same status fields.
	if allocationMode == AllocationModeController {
		logger.V(1).Info("Setting up QuotaBucketAllocation controller (all clusters)")
		if err := (&core.QuotaBucketAllocationController{
			Scheme:          standardMgr.GetScheme(),
			Manager:         mgr,
			BucketNamespace: opts.BucketNamespace,
			Workers:         opts.AllocationWorkers,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("failed to setup QuotaBucketAllocationController: %w", err)
		}

		logger.V(1).Info("Setting up QuotaClaim controller (all clusters)")
		if err := (&core.QuotaClaimController{
			Scheme:  standardMgr.GetScheme(),
			Manager: mgr,
			Workers: opts.AllocationWorkers,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("failed to setup QuotaClaimController: %w", err)
		}
	} else {
		logger.V(1).Info("Skipping QuotaBucketAllocation and QuotaClaim controllers; synchronous allocator owns allocation writes")
	}

	// 5. ClaimCreationPolicy controller (policy validation - core cluster only)
	logger.V(1).Info("Setting up ClaimCreationPolicy controller (core cluster only)")
	claimCreationPolicyValidator := validation.NewClaimCreationPolicyValidator(sharedResourceTypeValidator)
	if err := (&policy.ClaimCreationPolicyReconciler{
		Scheme:          standardMgr.GetScheme(),
		Manager:         mgr,
		PolicyValidator: claimCreationPolicyValidator,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup ClaimCreationPolicyReconciler: %w", err)
	}

	// 6. GrantCreationPolicy controller (policy validation - core cluster only)
	logger.V(1).Info("Setting up GrantCreationPolicy controller (core cluster only)")
	grantTemplateValidator, err := validation.NewGrantTemplateValidator(sharedResourceTypeValidator)
	if err != nil {
		return fmt.Errorf("failed to create GrantTemplateValidator: %w", err)
	}
	grantCreationPolicyValidator := validation.NewGrantCreationPolicyValidator(celValidator, grantTemplateValidator)
	if err := (&policy.GrantCreationPolicyReconciler{
		Scheme:          standardMgr.GetScheme(),
		Manager:         mgr,
		PolicyValidator: grantCreationPolicyValidator,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup GrantCreationPolicyReconciler: %w", err)
	}

	// 7. Grant Creation controller (automatic grant creation - core cluster only)
	logger.V(1).Info("Setting up Grant Creation controller (core cluster only)")
	templateEngine := engine.NewTemplateEngine(celEngine, logger)
	parentContextResolver := policy.NewParentContextResolver(standardMgr.GetClient(), standardMgr.GetConfig(), standardMgr.GetScheme(), policy.ParentContextResolverOptions{})

	if err := standardMgr.Add(informerManager); err != nil {
		return fmt.Errorf("failed to add informer manager to controller manager: %w", err)
	}

	grantCreationController := policy.NewGrantCreationController(
		mgr,
		standardMgr.GetScheme(),
		templateEngine,
		celEngine,
		parentContextResolver,
		standardMgr.GetEventRecorder("grant-creation"),
		informerManager,
	)
	if err := grantCreationController.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup GrantCreationController: %w", err)
	}

	// 8. Claim Creation controller (automatic claim creation - core cluster only)
	logger.V(1).Info("Setting up Claim Creation controller (core cluster only)")
	claimCreationController := policy.NewClaimCreationController(
		mgr,
		standardMgr.GetScheme(),
		templateEngine,
		celEngine,
		standardMgr.GetEventRecorder("claim-creation"),
		informerManager,
	)
	if err := claimCreationController.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup ClaimCreationController: %w", err)
	}

	// 9. QuotaClaim Ownership controller (lifecycle management - all clusters)
	logger.V(1).Info("Setting up QuotaClaim Ownership controller (all clusters)")
	if err := (&lifecycle.QuotaClaimOwnershipController{
		Scheme:  standardMgr.GetScheme(),
		Manager: mgr,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup QuotaClaimOwnershipController: %w", err)
	}

	// 10. DeniedAutoClaim Cleanup controller (lifecycle management - all clusters)
	logger.V(1).Info("Setting up DeniedAutoClaim Cleanup controller (all clusters)")
	deniedCleanupController := lifecycle.NewDeniedAutoClaimCleanupController(
		standardMgr.GetScheme(),
		mgr,
	)
	deniedCleanupController.StalePendingClaimAge = opts.StalePendingClaimAge
	if err := deniedCleanupController.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup DeniedAutoClaimCleanupController: %w", err)
	}

	logger.Info("All quota controllers set up successfully")
	return nil
}
