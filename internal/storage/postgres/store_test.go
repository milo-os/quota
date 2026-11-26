package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage"

	"go.miloapis.com/quota/internal/testutil/pgtest"
)

// testKind is a minimal runtime.Object for testing.
type testKind struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              testSpec `json:"spec,omitempty"`
}

type testSpec struct {
	Value string `json:"value,omitempty"`
}

func (t *testKind) DeepCopyObject() runtime.Object {
	out := *t
	out.ObjectMeta = *t.DeepCopy()
	return &out
}

// testKindList is a list type for testKind.
type testKindList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []testKind `json:"items"`
}

func (t *testKindList) DeepCopyObject() runtime.Object {
	out := *t
	if t.Items != nil {
		out.Items = make([]testKind, len(t.Items))
		for i := range t.Items {
			out.Items[i] = *t.Items[i].DeepCopyObject().(*testKind)
		}
	}
	return &out
}

var testScheme = runtime.NewScheme()
var testCodec runtime.Codec

func init() {
	gv := schema.GroupVersion{Group: "test.quota", Version: "v1"}
	internalGV := schema.GroupVersion{Group: "test.quota", Version: runtime.APIVersionInternal}
	testScheme.AddKnownTypeWithName(gv.WithKind("TestKind"), &testKind{})
	testScheme.AddKnownTypeWithName(gv.WithKind("TestKindList"), &testKindList{})
	// Register internal versions so codec.Decode(data, nil, nil) works
	testScheme.AddKnownTypeWithName(internalGV.WithKind("TestKind"), &testKind{})
	testScheme.AddKnownTypeWithName(internalGV.WithKind("TestKindList"), &testKindList{})

	codecs := serializer.NewCodecFactory(testScheme)
	testCodec = codecs.LegacyCodec(gv)
}

// getTestDB returns a DB connection for integration tests with the schema
// applied. It skips without POSTGRES_DSN, or fails when
// QUOTA_REQUIRE_POSTGRES is set (see pgtest).
func getTestDB(t testing.TB) *sql.DB {
	t.Helper()
	return pgtest.OpenDB(t)
}

// cleanupTestData removes all test data between tests.
func cleanupTestData(t testing.TB, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`DELETE FROM quota_changelog`); err != nil {
		t.Fatalf("failed to clean changelog: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM quota_objects`); err != nil {
		t.Fatalf("failed to clean objects: %v", err)
	}
}

func newTestObject(name, value string) *testKind {
	return &testKind{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "test.quota/v1",
			Kind:       "TestKind",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: testSpec{Value: value},
	}
}

func TestStore_Create(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)

	store := New(db, testCodec, "")
	ctx := context.Background()

	t.Run("creates object and sets resourceVersion", func(t *testing.T) {
		cleanupTestData(t, db)
		obj := newTestObject("test-create", "hello")
		out := &testKind{}
		err := store.Create(ctx, "/quota/default/test-create", obj, out, 0)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		if out.Name != "test-create" {
			t.Errorf("expected name 'test-create', got %q", out.Name)
		}
		if out.ResourceVersion == "" {
			t.Error("expected resourceVersion to be set")
		}
	})

	t.Run("duplicate key returns AlreadyExists", func(t *testing.T) {
		cleanupTestData(t, db)
		obj := newTestObject("test-dup", "v1")
		out := &testKind{}
		if err := store.Create(ctx, "/quota/default/test-dup", obj, out, 0); err != nil {
			t.Fatalf("first Create failed: %v", err)
		}

		obj2 := newTestObject("test-dup", "v2")
		out2 := &testKind{}
		err := store.Create(ctx, "/quota/default/test-dup", obj2, out2, 0)
		if err == nil {
			t.Fatal("expected error for duplicate key, got nil")
		}
		if !storage.IsExist(err) {
			t.Errorf("expected AlreadyExists error, got: %v", err)
		}
	})

	t.Run("empty key returns error", func(t *testing.T) {
		obj := newTestObject("test", "v")
		out := &testKind{}
		err := store.Create(ctx, "", obj, out, 0)
		if err == nil {
			t.Fatal("expected error for empty key, got nil")
		}
	})
}

func TestStore_Get(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)

	store := New(db, testCodec, "")
	ctx := context.Background()

	t.Run("returns existing object", func(t *testing.T) {
		cleanupTestData(t, db)
		obj := newTestObject("test-get", "getvalue")
		out := &testKind{}
		if err := store.Create(ctx, "/quota/default/test-get", obj, out, 0); err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		got := &testKind{}
		if err := store.Get(ctx, "/quota/default/test-get", storage.GetOptions{}, got); err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got.Name != "test-get" {
			t.Errorf("expected name 'test-get', got %q", got.Name)
		}
		if got.Spec.Value != "getvalue" {
			t.Errorf("expected spec.value 'getvalue', got %q", got.Spec.Value)
		}
	})

	t.Run("returns NotFound for missing key", func(t *testing.T) {
		cleanupTestData(t, db)
		got := &testKind{}
		err := store.Get(ctx, "/quota/default/missing", storage.GetOptions{}, got)
		if err == nil {
			t.Fatal("expected error for missing key, got nil")
		}
		if !storage.IsNotFound(err) {
			t.Errorf("expected NotFound error, got: %v", err)
		}
	})

	t.Run("IgnoreNotFound returns zero value", func(t *testing.T) {
		cleanupTestData(t, db)
		got := &testKind{}
		err := store.Get(ctx, "/quota/default/missing", storage.GetOptions{IgnoreNotFound: true}, got)
		if err != nil {
			t.Fatalf("Get with IgnoreNotFound failed: %v", err)
		}
	})
}

func TestStore_Delete(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)

	store := New(db, testCodec, "")
	ctx := context.Background()

	t.Run("deletes object and returns it", func(t *testing.T) {
		cleanupTestData(t, db)
		obj := newTestObject("test-del", "delvalue")
		created := &testKind{}
		if err := store.Create(ctx, "/quota/default/test-del", obj, created, 0); err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		deleted := &testKind{}
		err := store.Delete(ctx, "/quota/default/test-del", deleted, nil, nil, nil, storage.DeleteOptions{})
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if deleted.Name != "test-del" {
			t.Errorf("expected deleted object name 'test-del', got %q", deleted.Name)
		}

		// Verify it's gone
		got := &testKind{}
		err = store.Get(ctx, "/quota/default/test-del", storage.GetOptions{}, got)
		if !storage.IsNotFound(err) {
			t.Errorf("expected NotFound after delete, got: %v", err)
		}
	})

	t.Run("delete missing key returns NotFound", func(t *testing.T) {
		cleanupTestData(t, db)
		out := &testKind{}
		err := store.Delete(ctx, "/quota/default/nonexistent", out, nil, nil, nil, storage.DeleteOptions{})
		if !storage.IsNotFound(err) {
			t.Errorf("expected NotFound, got: %v", err)
		}
	})

	t.Run("delete with wrong UID precondition fails", func(t *testing.T) {
		cleanupTestData(t, db)
		obj := newTestObject("test-prec", "precval")
		obj.UID = types.UID("correct-uid")
		created := &testKind{}
		if err := store.Create(ctx, "/quota/default/test-prec", obj, created, 0); err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		wrongUID := types.UID("wrong-uid")
		preconditions := &storage.Preconditions{UID: &wrongUID}
		out := &testKind{}
		err := store.Delete(ctx, "/quota/default/test-prec", out, preconditions, nil, nil, storage.DeleteOptions{})
		if err == nil {
			t.Fatal("expected error with wrong UID precondition, got nil")
		}
	})
}

func TestStore_GuaranteedUpdate(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)

	store := New(db, testCodec, "")
	ctx := context.Background()

	t.Run("updates existing object", func(t *testing.T) {
		cleanupTestData(t, db)
		obj := newTestObject("test-update", "before")
		created := &testKind{}
		if err := store.Create(ctx, "/quota/default/test-update", obj, created, 0); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		origRV := created.ResourceVersion

		dest := &testKind{}
		err := store.GuaranteedUpdate(ctx, "/quota/default/test-update", dest, false, nil,
			func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
				existing := input.(*testKind)
				existing.Spec.Value = "after"
				return existing, nil, nil
			}, nil)
		if err != nil {
			t.Fatalf("GuaranteedUpdate failed: %v", err)
		}
		if dest.Spec.Value != "after" {
			t.Errorf("expected spec.value 'after', got %q", dest.Spec.Value)
		}
		if dest.ResourceVersion == origRV {
			t.Error("expected resourceVersion to change after update")
		}
	})

	t.Run("not found without ignoreNotFound returns error", func(t *testing.T) {
		cleanupTestData(t, db)
		dest := &testKind{}
		err := store.GuaranteedUpdate(ctx, "/quota/default/missing-update", dest, false, nil,
			func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
				return input, nil, nil
			}, nil)
		if err == nil {
			t.Fatal("expected error for missing key without ignoreNotFound")
		}
		if !storage.IsNotFound(err) {
			t.Errorf("expected NotFound error, got: %v", err)
		}
	})

	t.Run("ignoreNotFound creates if missing", func(t *testing.T) {
		cleanupTestData(t, db)
		dest := &testKind{}
		err := store.GuaranteedUpdate(ctx, "/quota/default/create-via-update", dest, true, nil,
			func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
				obj := input.(*testKind)
				obj.Name = "create-via-update"
				obj.Namespace = "default"
				obj.Spec.Value = "created"
				obj.SetGroupVersionKind(schema.GroupVersionKind{
					Group: "test.quota", Version: "v1", Kind: "TestKind",
				})
				return obj, nil, nil
			}, nil)
		if err != nil {
			t.Fatalf("GuaranteedUpdate with ignoreNotFound failed: %v", err)
		}

		// Verify it was created
		got := &testKind{}
		if err := store.Get(ctx, "/quota/default/create-via-update", storage.GetOptions{}, got); err != nil {
			t.Fatalf("Get after create-via-update failed: %v", err)
		}
		if got.Spec.Value != "created" {
			t.Errorf("expected spec.value 'created', got %q", got.Spec.Value)
		}
	})
}

func TestStore_GetList(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)

	store := New(db, testCodec, "")
	ctx := context.Background()

	t.Run("returns objects matching key prefix", func(t *testing.T) {
		cleanupTestData(t, db)
		// Create several objects
		for i := 0; i < 3; i++ {
			obj := newTestObject(fmt.Sprintf("item-%d", i), fmt.Sprintf("val-%d", i))
			out := &testKind{}
			key := fmt.Sprintf("/quota/default/item-%d", i)
			if err := store.Create(ctx, key, obj, out, 0); err != nil {
				t.Fatalf("Create %d failed: %v", i, err)
			}
		}

		// Create object with different prefix
		other := newTestObject("other", "other")
		outOther := &testKind{}
		if err := store.Create(ctx, "/other/default/other", other, outOther, 0); err != nil {
			t.Fatalf("Create other failed: %v", err)
		}

		list := &testKindList{}
		err := store.GetList(ctx, "/quota/default", storage.ListOptions{Recursive: true}, list)
		if err != nil {
			t.Fatalf("GetList failed: %v", err)
		}
		if len(list.Items) != 3 {
			t.Errorf("expected 3 items, got %d", len(list.Items))
		}
	})

	t.Run("empty prefix returns no items", func(t *testing.T) {
		cleanupTestData(t, db)
		list := &testKindList{}
		err := store.GetList(ctx, "/nonexistent", storage.ListOptions{Recursive: true}, list)
		if err != nil {
			t.Fatalf("GetList failed: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("expected 0 items, got %d", len(list.Items))
		}
	})

	t.Run("empty changelog returns non-zero resource version", func(t *testing.T) {
		cleanupTestData(t, db)
		list := &testKindList{}
		if err := store.GetList(ctx, "/quota/default", storage.ListOptions{Recursive: true}, list); err != nil {
			t.Fatalf("GetList failed: %v", err)
		}
		if list.ResourceVersion == "" || list.ResourceVersion == "0" {
			t.Errorf("expected non-zero resourceVersion on empty db, got %q", list.ResourceVersion)
		}
	})
}

func TestStore_Stats(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)

	store := New(db, testCodec, "")
	ctx := context.Background()

	t.Run("returns correct count", func(t *testing.T) {
		cleanupTestData(t, db)
		for i := 0; i < 5; i++ {
			obj := newTestObject(fmt.Sprintf("count-%d", i), "v")
			out := &testKind{}
			key := fmt.Sprintf("/quota/ns/count-%d", i)
			if err := store.Create(ctx, key, obj, out, 0); err != nil {
				t.Fatalf("Create %d failed: %v", i, err)
			}
		}

		stats, err := store.Stats(ctx)
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}
		if stats.ObjectCount != 5 {
			t.Errorf("expected count 5, got %d", stats.ObjectCount)
		}
	})

	t.Run("returns zero for empty table", func(t *testing.T) {
		cleanupTestData(t, db)
		stats, err := store.Stats(ctx)
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}
		if stats.ObjectCount != 0 {
			t.Errorf("expected count 0, got %d", stats.ObjectCount)
		}
	})
}

func TestStore_CreateThenGetRoundtrip(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)

	store := New(db, testCodec, "")
	ctx := context.Background()

	obj := newTestObject("roundtrip", "roundtrip-value")
	created := &testKind{}
	if err := store.Create(ctx, "/quota/default/roundtrip", obj, created, 0); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got := &testKind{}
	if err := store.Get(ctx, "/quota/default/roundtrip", storage.GetOptions{}, got); err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Name != "roundtrip" {
		t.Errorf("name mismatch: got %q, want %q", got.Name, "roundtrip")
	}
	if got.Namespace != "default" {
		t.Errorf("namespace mismatch: got %q, want %q", got.Namespace, "default")
	}
	if got.Spec.Value != "roundtrip-value" {
		t.Errorf("spec.value mismatch: got %q, want %q", got.Spec.Value, "roundtrip-value")
	}
	if got.ResourceVersion != created.ResourceVersion {
		t.Errorf("resourceVersion mismatch: got %q, want %q", got.ResourceVersion, created.ResourceVersion)
	}
}

func TestStore_Changelog(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)

	store := New(db, testCodec, "")
	ctx := context.Background()

	// Create
	obj := newTestObject("changelog-test", "v1")
	created := &testKind{}
	if err := store.Create(ctx, "/quota/default/changelog-test", obj, created, 0); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update
	dest := &testKind{}
	if err := store.GuaranteedUpdate(ctx, "/quota/default/changelog-test", dest, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testKind)
			existing.Spec.Value = "v2"
			return existing, nil, nil
		}, nil); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Delete
	deleted := &testKind{}
	if err := store.Delete(ctx, "/quota/default/changelog-test", deleted, nil, nil, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Check changelog entries
	rows, err := db.QueryContext(ctx,
		`SELECT event_type FROM quota_changelog WHERE key = $1 ORDER BY resource_version ASC`,
		"/quota/default/changelog-test",
	)
	if err != nil {
		t.Fatalf("failed to query changelog: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var events []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		events = append(events, et)
	}

	expected := []string{"ADDED", "MODIFIED", "DELETED"}
	if len(events) != len(expected) {
		t.Fatalf("expected %d changelog entries, got %d: %v", len(expected), len(events), events)
	}
	for i, want := range expected {
		if events[i] != want {
			t.Errorf("changelog[%d]: expected %q, got %q", i, want, events[i])
		}
	}
}

func TestStore_Versioner(t *testing.T) {
	db := getTestDB(t)
	store := New(db, testCodec, "")
	if store.Versioner() == nil {
		t.Error("expected non-nil Versioner")
	}
}

func TestStore_RequestWatchProgress(t *testing.T) {
	db := getTestDB(t)
	store := New(db, testCodec, "")
	if err := store.RequestWatchProgress(context.Background()); err != nil {
		t.Errorf("expected nil error from RequestWatchProgress, got: %v", err)
	}
}

// TestCodecHelpers tests the codec utility functions.
func TestCodecHelpers(t *testing.T) {
	t.Run("extractMetadata", func(t *testing.T) {
		obj := newTestObject("meta-test", "v")
		kind, ns, name := extractMetadata(obj)
		if kind != "TestKind" {
			t.Errorf("expected kind 'TestKind', got %q", kind)
		}
		if ns != "default" {
			t.Errorf("expected namespace 'default', got %q", ns)
		}
		if name != "meta-test" {
			t.Errorf("expected name 'meta-test', got %q", name)
		}
	})

	t.Run("isUniqueViolation", func(t *testing.T) {
		tests := []struct {
			msg    string
			expect bool
		}{
			{"duplicate key value violates unique constraint", true},
			{"error code 23505", true},
			{"some other error", false},
		}
		for _, tc := range tests {
			got := isUniqueViolation(fmt.Errorf("%s", tc.msg))
			if got != tc.expect {
				t.Errorf("isUniqueViolation(%q) = %v, want %v", tc.msg, got, tc.expect)
			}
		}
	})

	t.Run("matchesPredicate with Everything predicate matches", func(t *testing.T) {
		obj := newTestObject("pred-test", "v")
		result := matchesPredicate(obj, storage.Everything)
		if !result {
			t.Error("expected Everything predicate to match")
		}
	})

	t.Run("decode roundtrip", func(t *testing.T) {
		obj := newTestObject("decode-test", "decode-value")
		obj.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "test.quota", Version: "v1", Kind: "TestKind",
		})
		data, err := runtime.Encode(testCodec, obj)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		out := &testKind{}
		if err := decode(testCodec, data, out, 42); err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if out.Name != "decode-test" {
			t.Errorf("expected name 'decode-test', got %q", out.Name)
		}
		if out.ResourceVersion != "42" {
			t.Errorf("expected resourceVersion '42', got %q", out.ResourceVersion)
		}
	})

	t.Run("decodeToObject", func(t *testing.T) {
		obj := newTestObject("decode-obj", "val")
		obj.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "test.quota", Version: "v1", Kind: "TestKind",
		})
		data, err := runtime.Encode(testCodec, obj)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := decodeToObject(testCodec, data)
		if err != nil {
			t.Fatalf("decodeToObject failed: %v", err)
		}
		tk := decoded.(*testKind)
		if tk.Name != "decode-obj" {
			t.Errorf("expected name 'decode-obj', got %q", tk.Name)
		}
	})
}

func TestStore_ValidateKey(t *testing.T) {
	db := getTestDB(t)
	store := New(db, testCodec, "")

	if err := store.validateKey(""); err == nil {
		t.Error("expected error for empty key")
	}
	if err := store.validateKey("/valid/key"); err != nil {
		t.Errorf("expected nil for valid key, got: %v", err)
	}
}

// The kind column is fixed at insert. Objects reaching storage are internal
// versions with empty TypeMeta; an update must not blank the column, or a
// claim drops out of every kind = 'QuotaClaim' index and native query.
func TestStore_KindColumnSurvivesUpdates(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	ctx := context.Background()
	s := New(db, testCodec, "")
	s.SetNewFunc(func() runtime.Object { return &testKind{} })

	kindOf := func(key string) string {
		t.Helper()
		var kind string
		if err := db.QueryRow(`SELECT kind FROM quota_objects WHERE key = $1`, key).Scan(&kind); err != nil {
			t.Fatalf("read kind: %v", err)
		}
		return kind
	}

	key := "/test/kinds/ns/a"
	obj := newTestObject("a", "v1")
	obj.TypeMeta = metav1.TypeMeta{} // as an internal object arrives
	out := &testKind{}
	if err := s.Create(ctx, key, obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := kindOf(key); got != "testKind" {
		t.Fatalf("kind after Create = %q, want testKind", got)
	}

	if err := s.GuaranteedUpdate(ctx, key, &testKind{}, false, nil, func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
		next := input.(*testKind).DeepCopyObject().(*testKind)
		next.TypeMeta = metav1.TypeMeta{}
		next.Spec.Value = "v2"
		return next, nil, nil
	}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if got := kindOf(key); got != "testKind" {
		t.Fatalf("kind after GuaranteedUpdate = %q, want testKind", got)
	}
}
