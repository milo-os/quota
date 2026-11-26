package install

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	metafuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
)

// TestRoundTripTypes fuzzes every registered quota kind and round-trips it
// through v1alpha1 <-> internal conversion and the JSON/CBOR serializers.
//
// This is the guard against conversion drift: a field added to v1alpha1 (or
// the internal types) without a matching field on the other side, or a
// conversion that drops data, fails here with a diff of the lost field.
// Protobuf is skipped because these types use JSON-backed Marshal/Unmarshal
// shims rather than generated protobuf.
func TestRoundTripTypes(t *testing.T) {
	roundtrip.RoundTripTestForAPIGroup(t, Install, metafuzzer.Funcs)
}
