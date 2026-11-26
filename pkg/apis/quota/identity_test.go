package quota

import (
	"math"
	"testing"
)

func TestBucketNameDistinguishesFullConsumerIdentity(t *testing.T) {
	base := BucketName("widgets", "example.com", "Tenant", "", "acme")
	others := map[string]string{
		"apiGroup":     BucketName("widgets", "other.com", "Tenant", "", "acme"),
		"namespace":    BucketName("widgets", "example.com", "Tenant", "ns-a", "acme"),
		"kind":         BucketName("widgets", "example.com", "Org", "", "acme"),
		"name":         BucketName("widgets", "example.com", "Tenant", "", "globex"),
		"resourceType": BucketName("gadgets", "example.com", "Tenant", "", "acme"),
		// Without separators "widgets"+"Tenant"+"acme" and
		// "widgetsT"+"enant"+"acme" hash the same input.
		"boundary shift": BucketName("widgetsT", "example.com", "enant", "", "acme"),
	}
	for what, name := range others {
		if name == base {
			t.Errorf("changing %s did not change the bucket name", what)
		}
	}
	if got := BucketName("widgets", "example.com", "Tenant", "", "acme"); got != base {
		t.Error("BucketName is not deterministic")
	}
	if len(base) != len("bucket-")+64 {
		t.Errorf("unexpected bucket name length %d", len(base))
	}
}

func TestConsumerKeyUnambiguous(t *testing.T) {
	if ConsumerKey("a", "b/c", "", "d") == ConsumerKey("a", "b", "c", "d") {
		t.Error("ConsumerKey collides when a part contains the separator")
	}
}

func TestSaturatingAdd(t *testing.T) {
	if got := SaturatingAdd(math.MaxInt64, 1); got != math.MaxInt64 {
		t.Errorf("SaturatingAdd(MaxInt64, 1) = %d", got)
	}
	if got := SaturatingAdd(MaxAmount, MaxAmount); got != 2*MaxAmount {
		t.Errorf("SaturatingAdd(MaxAmount, MaxAmount) = %d", got)
	}
	if got := SaturatingAdd(2, 3); got != 5 {
		t.Errorf("SaturatingAdd(2, 3) = %d", got)
	}
}
