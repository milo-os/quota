package quota

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strconv"
	"strings"
)

// MaxAmount is the largest quantity a grant bucket, claim request or policy
// template may carry. It is far above any realistic quota (one exa-unit, or
// an exabyte when the base unit is bytes) and leaves enough headroom that a
// handful of maximal grants cannot overflow int64 before SaturatingAdd caps
// the sum.
const MaxAmount int64 = 1_000_000_000_000_000_000

// SaturatingAdd returns a+b for non-negative operands, capped at
// math.MaxInt64 instead of wrapping negative.
func SaturatingAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// ConsumerKey returns a canonical, unambiguous string for a consumer
// identity: every part is length-prefixed, so no two distinct
// (apiGroup, kind, namespace, name) tuples share a key even when a part
// contains a separator character.
func ConsumerKey(apiGroup, kind, namespace, name string) string {
	var b strings.Builder
	for _, part := range []string{apiGroup, kind, namespace, name} {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
		b.WriteByte(';')
	}
	return b.String()
}

// BucketName returns the deterministic QuotaBucket name for a resource type
// and consumer. It is the single definition shared by the bucket controllers
// and the Postgres allocator, so both address the same bucket. The full
// consumer identity (apiGroup, kind, namespace, name) is hashed, so consumers
// that share a kind and name in different API groups or namespaces get
// separate buckets.
func BucketName(resourceType, apiGroup, kind, namespace, name string) string {
	sum := sha256.Sum256([]byte(strconv.Itoa(len(resourceType)) + ":" + resourceType + ";" + ConsumerKey(apiGroup, kind, namespace, name)))
	return "bucket-" + hex.EncodeToString(sum[:])
}

// Key returns ConsumerKey for c.
func (c ConsumerRef) Key() string {
	return ConsumerKey(c.APIGroup, c.Kind, c.Namespace, c.Name)
}
