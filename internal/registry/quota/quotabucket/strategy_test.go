package quotabucket

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/install"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	install.Install(s)
	return s
}

func validQuotaBucket() *quota.QuotaBucket {
	return &quota.QuotaBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-bucket",
			Namespace: "default",
		},
		Spec: quota.QuotaBucketSpec{
			ConsumerRef: quota.ConsumerRef{
				APIGroup: "example.com",
				Kind:     "Project",
				Name:     "my-project",
			},
			ResourceType: "widgets",
		},
	}
}

func TestNamespaceScoped(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	if !strategy.NamespaceScoped() {
		t.Error("QuotaBucket should be namespaced (NamespaceScoped=true)")
	}
}

func TestPrepareForCreate(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	bucket := validQuotaBucket()
	bucket.Status = quota.QuotaBucketStatus{
		ObservedGeneration: 5,
		Limit:              100,
		Allocated:          30,
		Available:          70,
		ClaimCount:         3,
		GrantCount:         2,
	}

	strategy.PrepareForCreate(context.Background(), bucket)

	if bucket.Status.ObservedGeneration != 0 {
		t.Errorf("status.observedGeneration should be cleared, got %d", bucket.Status.ObservedGeneration)
	}
	if bucket.Status.Limit != 0 {
		t.Errorf("status.limit should be cleared, got %d", bucket.Status.Limit)
	}
	if bucket.Status.Allocated != 0 {
		t.Errorf("status.allocated should be cleared, got %d", bucket.Status.Allocated)
	}
}

func TestPrepareForUpdate(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldBucket := validQuotaBucket()
	oldBucket.Status = quota.QuotaBucketStatus{
		ObservedGeneration: 3,
		Limit:              100,
		Allocated:          30,
		Available:          70,
	}

	newBucket := validQuotaBucket()
	newBucket.Status = quota.QuotaBucketStatus{ObservedGeneration: 999}

	strategy.PrepareForUpdate(context.Background(), newBucket, oldBucket)

	if newBucket.Status.ObservedGeneration != 3 {
		t.Errorf("status should be preserved from old, got observedGeneration=%d", newBucket.Status.ObservedGeneration)
	}
	if newBucket.Status.Limit != 100 {
		t.Errorf("status.limit should be preserved, got %d", newBucket.Status.Limit)
	}
}

func TestValidateValid(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	bucket := validQuotaBucket()
	errs := strategy.Validate(context.Background(), bucket)
	if len(errs) != 0 {
		t.Errorf("expected no errors for valid bucket, got %v", errs)
	}
}

func TestValidateRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*quota.QuotaBucket)
		errPath string
	}{
		{
			name:    "missing consumerRef kind",
			mutate:  func(b *quota.QuotaBucket) { b.Spec.ConsumerRef.Kind = "" },
			errPath: "spec.consumerRef.kind",
		},
		{
			name:    "missing consumerRef name",
			mutate:  func(b *quota.QuotaBucket) { b.Spec.ConsumerRef.Name = "" },
			errPath: "spec.consumerRef.name",
		},
		{
			name:    "missing resourceType",
			mutate:  func(b *quota.QuotaBucket) { b.Spec.ResourceType = "" },
			errPath: "spec.resourceType",
		},
	}

	strategy := NewStrategy(testScheme(t))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := validQuotaBucket()
			tt.mutate(bucket)
			errs := strategy.Validate(context.Background(), bucket)
			if len(errs) == 0 {
				t.Fatal("expected validation errors, got none")
			}
			found := false
			for _, e := range errs {
				if e.Field == tt.errPath {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected error at path %q, got errors: %v", tt.errPath, errs)
			}
		})
	}
}

func TestValidateUpdateImmutableConsumerRef(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldBucket := validQuotaBucket()
	newBucket := validQuotaBucket()
	newBucket.Spec.ConsumerRef.Name = "different-project"

	errs := strategy.ValidateUpdate(context.Background(), newBucket, oldBucket)
	if len(errs) == 0 {
		t.Fatal("expected error for changing immutable consumerRef")
	}
	found := false
	for _, e := range errs {
		if e.Field == "spec.consumerRef" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected error at spec.consumerRef, got %v", errs)
	}
}

func TestValidateUpdateImmutableResourceType(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldBucket := validQuotaBucket()
	newBucket := validQuotaBucket()
	newBucket.Spec.ResourceType = "gadgets"

	errs := strategy.ValidateUpdate(context.Background(), newBucket, oldBucket)
	if len(errs) == 0 {
		t.Fatal("expected error for changing immutable resourceType")
	}
	found := false
	for _, e := range errs {
		if e.Field == "spec.resourceType" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected error at spec.resourceType, got %v", errs)
	}
}

func TestValidateUpdateNoImmutableChange(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldBucket := validQuotaBucket()
	newBucket := validQuotaBucket()
	// No changes to immutable fields
	errs := strategy.ValidateUpdate(context.Background(), newBucket, oldBucket)
	if len(errs) != 0 {
		t.Errorf("expected no errors when immutable fields unchanged, got %v", errs)
	}
}

func TestGetAttrs(t *testing.T) {
	bucket := validQuotaBucket()
	bucket.Labels = map[string]string{"env": "test"}

	lbls, flds, err := GetAttrs(bucket)
	if err != nil {
		t.Fatalf("GetAttrs error: %v", err)
	}
	if lbls["env"] != "test" {
		t.Errorf("expected label env=test, got %v", lbls)
	}
	if flds["spec.consumerRef.kind"] != "Project" {
		t.Errorf("expected field spec.consumerRef.kind=Project, got %q", flds["spec.consumerRef.kind"])
	}
	if flds["spec.consumerRef.name"] != "my-project" {
		t.Errorf("expected field spec.consumerRef.name=my-project, got %q", flds["spec.consumerRef.name"])
	}
	if flds["spec.resourceType"] != "widgets" {
		t.Errorf("expected field spec.resourceType=widgets, got %q", flds["spec.resourceType"])
	}
}

func TestGetAttrsWrongType(t *testing.T) {
	_, _, err := GetAttrs(&quota.QuotaGrant{})
	if err == nil {
		t.Error("expected error for wrong type, got nil")
	}
}

func TestStatusStrategyPrepareForUpdate(t *testing.T) {
	statusStrategy := NewStatusStrategy(testScheme(t))

	oldBucket := validQuotaBucket()
	oldBucket.Labels = map[string]string{"env": "test"}
	oldBucket.Annotations = map[string]string{"note": "original"}

	newBucket := validQuotaBucket()
	newBucket.Spec.ResourceType = "should-be-reverted"
	newBucket.Labels = map[string]string{"env": "changed"}
	newBucket.Status = quota.QuotaBucketStatus{
		Limit:     200,
		Allocated: 50,
		Available: 150,
	}

	statusStrategy.PrepareForUpdate(context.Background(), newBucket, oldBucket)

	if newBucket.Spec.ResourceType != "widgets" {
		t.Error("spec should be preserved from old object on status update")
	}
	if newBucket.Labels["env"] != "test" {
		t.Error("labels should be preserved from old object on status update")
	}
	if newBucket.Status.Limit != 200 {
		t.Error("status should be kept from new object")
	}
}
