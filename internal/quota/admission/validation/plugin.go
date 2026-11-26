package validation

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	"go.miloapis.com/quota/internal/quota/validation"
	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// conversionScheme is a scheme with both internal and v1alpha1 types registered
// for converting between internal and external representations in admission.
var conversionScheme = func() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(quotainternal.AddToScheme(s))
	utilruntime.Must(quotav1alpha1.AddToScheme(s))
	return s
}()

// toTyped converts an admission object (which may be internal, v1alpha1, or unstructured)
// to a v1alpha1 typed object. In an aggregated apiserver, objects arrive as internal types.
func toTyped[T any, PT interface {
	*T
	runtime.Object
}](obj runtime.Object) (PT, error) {
	// Already the target v1alpha1 type
	if typed, ok := obj.(PT); ok {
		return typed, nil
	}

	// Unstructured (CRD path)
	if u, ok := obj.(*unstructured.Unstructured); ok {
		out := PT(new(T))
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, out); err != nil {
			return nil, fmt.Errorf("failed to convert unstructured to %T: %w", out, err)
		}
		return out, nil
	}

	// Internal type — convert via scheme
	out := PT(new(T))
	if err := conversionScheme.Convert(obj, out, nil); err != nil {
		return nil, fmt.Errorf("failed to convert %T to %T: %w", obj, out, err)
	}
	return out, nil
}

const (
	// PluginName is the name of the validation admission plugin.
	PluginName = "QuotaValidation"
)

// QuotaValidationPlugin validates quota API types when they are created/updated
// in the quota apiserver. This includes QuotaDefinition, QuotaGrant, QuotaClaim,
// ClaimCreationPolicy, and GrantCreationPolicy validation.
type QuotaValidationPlugin struct {
	*admission.Handler
	dynamicClient                dynamic.Interface
	resourceTypeValidator        validation.ResourceTypeValidator
	quotaClaimValidator          validation.QuotaClaimValidator
	quotaDefinitionValidator     *validation.QuotaDefinitionValidator
	claimCreationPolicyValidator *validation.ClaimCreationPolicyValidator
	grantCreationPolicyValidator *validation.GrantCreationPolicyValidator
	quotaGrantValidator          *validation.QuotaGrantValidator
	logger                       logr.Logger
}

// Ensure QuotaValidationPlugin implements the required interfaces
var _ initializer.WantsDynamicClient = &QuotaValidationPlugin{}
var _ admission.ValidationInterface = &QuotaValidationPlugin{}
var _ admission.InitializationValidator = &QuotaValidationPlugin{}

// NewQuotaValidationPlugin creates a new QuotaValidationPlugin.
func NewQuotaValidationPlugin() (*QuotaValidationPlugin, error) {
	logger := klog.NewKlogr().WithName("quota-validation-plugin")
	klog.V(1).InfoS("Creating QuotaValidation admission plugin instance")

	plugin := &QuotaValidationPlugin{
		Handler: admission.NewHandler(admission.Create),
		logger:  logger,
	}

	return plugin, nil
}

// SetDynamicClient implements initializer.WantsDynamicClient
func (p *QuotaValidationPlugin) SetDynamicClient(dynamicClient dynamic.Interface) {
	p.dynamicClient = dynamicClient
	p.logger.V(2).Info("Dynamic client set", "plugin", PluginName)

	if dynamicClient != nil && p.resourceTypeValidator == nil {
		p.initializeValidators()
	}
}

// ValidateInitialization implements admission.InitializationValidator
func (p *QuotaValidationPlugin) ValidateInitialization() error {
	if p.dynamicClient == nil {
		return fmt.Errorf("dynamic client not initialized")
	}
	if p.quotaClaimValidator == nil {
		return fmt.Errorf("quota claim validator not initialized")
	}
	if p.quotaDefinitionValidator == nil {
		return fmt.Errorf("quota definition validator not initialized")
	}
	if p.claimCreationPolicyValidator == nil {
		return fmt.Errorf("claim creation policy validator not initialized")
	}
	if p.grantCreationPolicyValidator == nil {
		return fmt.Errorf("grant creation policy validator not initialized")
	}
	if p.quotaGrantValidator == nil {
		return fmt.Errorf("quota grant validator not initialized")
	}
	return nil
}

// initializeValidators initializes all validators
func (p *QuotaValidationPlugin) initializeValidators() {
	p.logger.V(2).Info("Initializing validators for validation plugin")

	p.resourceTypeValidator = validation.NewResourceTypeValidator(p.dynamicClient)
	p.quotaClaimValidator = validation.NewQuotaClaimValidator(p.dynamicClient, p.resourceTypeValidator)
	p.quotaDefinitionValidator = validation.NewQuotaDefinitionValidator(p.resourceTypeValidator)

	celValidator, err := validation.NewCELValidator()
	if err != nil {
		p.logger.Error(err, "Failed to create CEL validator")
		return
	}

	grantTemplateValidator, err := validation.NewGrantTemplateValidator(p.resourceTypeValidator)
	if err != nil {
		p.logger.Error(err, "Failed to create grant template validator")
		return
	}

	p.claimCreationPolicyValidator = validation.NewClaimCreationPolicyValidator(p.resourceTypeValidator)
	p.grantCreationPolicyValidator = validation.NewGrantCreationPolicyValidator(celValidator, grantTemplateValidator)
	p.quotaGrantValidator = validation.NewQuotaGrantValidator(p.resourceTypeValidator)

	p.logger.V(2).Info("Validators initialized successfully")
}

// ResourceTypeValidator returns the resource type validator for readiness checks.
func (p *QuotaValidationPlugin) ResourceTypeValidator() validation.ResourceTypeValidator {
	return p.resourceTypeValidator
}

// Validate implements admission.ValidationInterface and handles validation of quota API types.
func (p *QuotaValidationPlugin) Validate(ctx context.Context, attrs admission.Attributes, _ admission.ObjectInterfaces) error {
	p.logger.V(3).Info("QuotaValidation admission plugin triggered",
		"operation", attrs.GetOperation(),
		"resource.group", attrs.GetKind().Group,
		"resource.version", attrs.GetKind().Version,
		"resource.kind", attrs.GetKind().Kind,
		"name", attrs.GetName(),
		"namespace", attrs.GetNamespace(),
		"user", attrs.GetUserInfo().GetName(),
		"dryRun", attrs.IsDryRun(),
	)

	// Only handle resources in the quota API group
	if attrs.GetKind().Group != quotav1alpha1.GroupName {
		return nil
	}

	switch attrs.GetKind().Kind {
	case "QuotaClaim":
		return p.validateQuotaClaim(ctx, attrs)
	case "QuotaDefinition":
		return p.validateQuotaDefinition(ctx, attrs)
	case "ClaimCreationPolicy":
		return p.validateClaimCreationPolicy(ctx, attrs)
	case "GrantCreationPolicy":
		return p.validateGrantCreationPolicy(ctx, attrs)
	case "QuotaGrant":
		return p.validateQuotaGrant(ctx, attrs)
	}

	return nil
}

// startSpan safely starts a span using the tracer provider from the context
func (p *QuotaValidationPlugin) startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracerProvider := trace.SpanFromContext(ctx).TracerProvider()
	tracer := tracerProvider.Tracer("go.miloapis.com/quota/admission/validation")
	return tracer.Start(ctx, name, opts...)
}

// validateQuotaClaim validates QuotaClaim objects when they are created directly
func (p *QuotaValidationPlugin) validateQuotaClaim(ctx context.Context, attrs admission.Attributes) error {
	ctx, span := p.startSpan(ctx, "quota.admission.QuotaClaimValidation",
		trace.WithAttributes(
			attribute.String("operation", string(attrs.GetOperation())),
			attribute.String("claim.name", attrs.GetName()),
			attribute.String("claim.namespace", attrs.GetNamespace()),
			attribute.String("user.name", attrs.GetUserInfo().GetName()),
			attribute.Bool("dry_run", attrs.IsDryRun()),
		))
	defer span.End()

	if attrs.GetOperation() != admission.Create {
		span.SetAttributes(attribute.String("validation.status", "skipped"))
		p.logger.V(4).Info("Skipping non-CREATE operation for QuotaClaim", "operation", attrs.GetOperation())
		return nil
	}

	obj := attrs.GetObject()
	if obj == nil {
		return nil
	}

	claim, err := toTyped[quotav1alpha1.QuotaClaim](obj)
	if err != nil {
		return fmt.Errorf("failed to convert to QuotaClaim: %w", err)
	}

	span.SetAttributes(
		attribute.String("claim.name", claim.Name),
		attribute.String("claim.namespace", claim.Namespace),
		attribute.Int("claim.request_count", len(claim.Spec.Requests)),
	)

	if errs := p.quotaClaimValidator.Validate(ctx, claim); len(errs) > 0 {
		span.SetAttributes(
			attribute.String("validation.status", "failed"),
			attribute.Int("validation.error_count", len(errs)),
		)
		span.SetStatus(codes.Error, "QuotaClaim validation failed")
		p.logger.Info("QuotaClaim validation failed",
			"name", claim.Name,
			"namespace", claim.Namespace,
			"errors", errs)
		return admission.NewForbidden(attrs, errors.NewInvalid(
			quotav1alpha1.SchemeGroupVersion.WithKind("QuotaClaim").GroupKind(),
			claim.Name,
			errs))
	}

	span.SetAttributes(attribute.String("validation.status", "passed"))
	return nil
}

// validateQuotaDefinition validates QuotaDefinition objects for cross-resource duplicates.
func (p *QuotaValidationPlugin) validateQuotaDefinition(ctx context.Context, attrs admission.Attributes) error {
	_, span := p.startSpan(ctx, "quota.admission.QuotaDefinitionValidation",
		trace.WithAttributes(
			attribute.String("operation", string(attrs.GetOperation())),
			attribute.String("definition.name", attrs.GetName()),
			attribute.String("user.name", attrs.GetUserInfo().GetName()),
		))
	defer span.End()

	if attrs.GetOperation() != admission.Create {
		span.SetAttributes(attribute.String("validation.status", "skipped"))
		return nil
	}

	obj := attrs.GetObject()
	if obj == nil {
		return nil
	}

	definition, err := toTyped[quotav1alpha1.QuotaDefinition](obj)
	if err != nil {
		return fmt.Errorf("failed to convert to QuotaDefinition: %w", err)
	}

	span.SetAttributes(
		attribute.String("definition.name", definition.Name),
		attribute.String("definition.resourceType", definition.Spec.ResourceType),
	)

	if validationErrs := p.quotaDefinitionValidator.Validate(definition); len(validationErrs) > 0 {
		span.SetAttributes(attribute.String("validation.status", "failed"))
		span.SetStatus(codes.Error, "QuotaDefinition validation failed")

		return admission.NewForbidden(attrs, errors.NewInvalid(
			quotav1alpha1.SchemeGroupVersion.WithKind("QuotaDefinition").GroupKind(),
			definition.Name,
			validationErrs,
		))
	}

	span.SetAttributes(attribute.String("validation.status", "passed"))
	return nil
}

// validateClaimCreationPolicy validates ClaimCreationPolicy objects.
func (p *QuotaValidationPlugin) validateClaimCreationPolicy(ctx context.Context, attrs admission.Attributes) error {
	ctx, span := p.startSpan(ctx, "quota.admission.ClaimCreationPolicyValidation",
		trace.WithAttributes(
			attribute.String("operation", string(attrs.GetOperation())),
			attribute.String("policy.name", attrs.GetName()),
			attribute.String("policy.namespace", attrs.GetNamespace()),
			attribute.String("user.name", attrs.GetUserInfo().GetName()),
		))
	defer span.End()

	if attrs.GetOperation() != admission.Create {
		span.SetAttributes(attribute.String("validation.status", "skipped"))
		return nil
	}

	obj := attrs.GetObject()
	if obj == nil {
		return nil
	}

	policy, err := toTyped[quotav1alpha1.ClaimCreationPolicy](obj)
	if err != nil {
		return fmt.Errorf("failed to convert to ClaimCreationPolicy: %w", err)
	}

	span.SetAttributes(
		attribute.String("policy.name", policy.Name),
		attribute.String("policy.namespace", policy.Namespace),
	)

	if validationErrs := p.claimCreationPolicyValidator.Validate(ctx, policy, validation.AdmissionValidationOptions()); len(validationErrs) > 0 {
		span.SetAttributes(attribute.String("validation.status", "failed"))
		span.SetStatus(codes.Error, "ClaimCreationPolicy validation failed")

		p.logger.Info("ClaimCreationPolicy validation failed",
			"name", policy.Name,
			"namespace", policy.Namespace,
			"errors", validationErrs)

		return admission.NewForbidden(attrs, errors.NewInvalid(
			quotav1alpha1.SchemeGroupVersion.WithKind("ClaimCreationPolicy").GroupKind(),
			policy.Name,
			validationErrs,
		))
	}

	span.SetAttributes(attribute.String("validation.status", "passed"))
	return nil
}

// validateGrantCreationPolicy validates GrantCreationPolicy objects.
func (p *QuotaValidationPlugin) validateGrantCreationPolicy(ctx context.Context, attrs admission.Attributes) error {
	ctx, span := p.startSpan(ctx, "quota.admission.GrantCreationPolicyValidation",
		trace.WithAttributes(
			attribute.String("operation", string(attrs.GetOperation())),
			attribute.String("policy.name", attrs.GetName()),
			attribute.String("policy.namespace", attrs.GetNamespace()),
			attribute.String("user.name", attrs.GetUserInfo().GetName()),
		))
	defer span.End()

	if attrs.GetOperation() != admission.Create {
		span.SetAttributes(attribute.String("validation.status", "skipped"))
		return nil
	}

	obj := attrs.GetObject()
	if obj == nil {
		return nil
	}

	policy, err := toTyped[quotav1alpha1.GrantCreationPolicy](obj)
	if err != nil {
		return fmt.Errorf("failed to convert to GrantCreationPolicy: %w", err)
	}

	span.SetAttributes(
		attribute.String("policy.name", policy.Name),
		attribute.String("policy.namespace", policy.Namespace),
	)

	if validationErrs := p.grantCreationPolicyValidator.Validate(ctx, policy, validation.AdmissionValidationOptions()); len(validationErrs) > 0 {
		span.SetAttributes(attribute.String("validation.status", "failed"))
		span.SetStatus(codes.Error, "GrantCreationPolicy validation failed")

		p.logger.Info("GrantCreationPolicy validation failed",
			"name", policy.Name,
			"namespace", policy.Namespace,
			"errors", validationErrs)

		return admission.NewForbidden(attrs, errors.NewInvalid(
			quotav1alpha1.SchemeGroupVersion.WithKind("GrantCreationPolicy").GroupKind(),
			policy.Name,
			validationErrs,
		))
	}

	span.SetAttributes(attribute.String("validation.status", "passed"))
	return nil
}

// validateQuotaGrant validates QuotaGrant objects for resource type validity.
func (p *QuotaValidationPlugin) validateQuotaGrant(ctx context.Context, attrs admission.Attributes) error {
	ctx, span := p.startSpan(ctx, "quota.admission.QuotaGrantValidation",
		trace.WithAttributes(
			attribute.String("operation", string(attrs.GetOperation())),
			attribute.String("grant.name", attrs.GetName()),
			attribute.String("grant.namespace", attrs.GetNamespace()),
			attribute.String("user.name", attrs.GetUserInfo().GetName()),
		))
	defer span.End()

	if attrs.GetOperation() != admission.Create {
		span.SetAttributes(attribute.String("validation.status", "skipped"))
		return nil
	}

	obj := attrs.GetObject()
	if obj == nil {
		return nil
	}

	grant, err := toTyped[quotav1alpha1.QuotaGrant](obj)
	if err != nil {
		return fmt.Errorf("failed to convert to QuotaGrant: %w", err)
	}

	span.SetAttributes(
		attribute.String("grant.name", grant.Name),
		attribute.String("grant.namespace", grant.Namespace),
		attribute.Int("grant.allowances_count", len(grant.Spec.Allowances)),
	)

	if validationErrs := p.quotaGrantValidator.Validate(ctx, grant, validation.AdmissionValidationOptions()); len(validationErrs) > 0 {
		span.SetAttributes(attribute.String("validation.status", "failed"))
		span.SetStatus(codes.Error, "QuotaGrant validation failed")

		p.logger.Info("QuotaGrant validation failed",
			"name", grant.Name,
			"namespace", grant.Namespace,
			"errors", validationErrs)

		return admission.NewForbidden(attrs, errors.NewInvalid(
			quotav1alpha1.SchemeGroupVersion.WithKind("QuotaGrant").GroupKind(),
			grant.Name,
			validationErrs,
		))
	}

	span.SetAttributes(attribute.String("validation.status", "passed"))
	return nil
}
