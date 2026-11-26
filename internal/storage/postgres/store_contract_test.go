package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// --- DB-free tests ---

func TestCreateRejectsResourceVersion(t *testing.T) {
	s := New(nil, testCodec, "")
	obj := newTestObject("rv-set", "v")
	obj.ResourceVersion = "5"
	err := s.Create(context.Background(), "/quota/default/rv-set", obj, &testKind{}, 0)
	if !errors.Is(err, storage.ErrResourceVersionSetOnCreate) {
		t.Fatalf("expected ErrResourceVersionSetOnCreate, got %v", err)
	}
}

func TestPrefixRangeEnd(t *testing.T) {
	tests := map[string]string{
		"/quota/": "/quota0",
		"/a/b/":   "/a/b0",
		"abc":     "abd",
		"":        "\x7f\x7f\x7f\x7f",
		"a\x7f":   "b",
	}
	for in, want := range tests {
		if got := PrefixRangeEnd(in); got != want {
			t.Errorf("PrefixRangeEnd(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestContinueStart(t *testing.T) {
	from, incl := continueStart("/p/a\x00")
	if from != "/p/a" || incl {
		t.Errorf("NUL-terminated key: got %q inclusive=%v", from, incl)
	}
	from, incl = continueStart("/p/a")
	if from != "/p/a" || !incl {
		t.Errorf("plain key: got %q inclusive=%v", from, incl)
	}
}

func TestExpiredContinueError(t *testing.T) {
	err := expiredContinueError("/p/a\x00", "/p/")
	var status *apierrors.StatusError
	if !errors.As(err, &status) || !apierrors.IsResourceExpired(err) {
		t.Fatalf("expected 410, got %v", err)
	}
	key, rv, decErr := storage.DecodeContinue(status.ErrStatus.Continue, "/p/")
	if decErr != nil || rv != -1 || key != "/p/a\x00" {
		t.Fatalf("inconsistent-continue token wrong: key=%q rv=%d err=%v", key, rv, decErr)
	}
}

// --- DB-gated tests ---

func createN(t *testing.T, s *Store, prefix string, n int) []int64 {
	t.Helper()
	var rvs []int64
	for i := 0; i < n; i++ {
		out := &testKind{}
		name := fmt.Sprintf("item-%02d", i)
		if err := s.Create(context.Background(), prefix+name, newTestObject(name, "v"), out, 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		rv, _ := strconv.ParseInt(out.ResourceVersion, 10, 64)
		rvs = append(rvs, rv)
	}
	return rvs
}

func TestStore_DeleteReturnsDeleteRV(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	s := New(db, testCodec, "")
	ctx := context.Background()
	created := &testKind{}
	if err := s.Create(ctx, "/quota/default/d", newTestObject("d", "v"), created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	deleted := &testKind{}
	if err := s.Delete(ctx, "/quota/default/d", deleted, nil, nil, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.ResourceVersion == created.ResourceVersion {
		t.Fatalf("Delete returned the pre-delete RV %s", deleted.ResourceVersion)
	}
	current, err := s.GetCurrentResourceVersion(ctx)
	if err != nil {
		t.Fatalf("GetCurrentResourceVersion: %v", err)
	}
	if strconv.FormatUint(current, 10) != deleted.ResourceVersion {
		t.Fatalf("delete RV %s is not the current RV %d", deleted.ResourceVersion, current)
	}
}

func TestStore_GuaranteedUpdateDoesNotRetryUserConflict(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	s := New(db, testCodec, "")
	ctx := context.Background()
	if err := s.Create(ctx, "/quota/default/u", newTestObject("u", "v"), &testKind{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	calls := 0
	err := s.GuaranteedUpdate(ctx, "/quota/default/u", &testKind{}, false, nil,
		func(runtime.Object, storage.ResponseMeta) (runtime.Object, *uint64, error) {
			calls++
			return nil, nil, storage.NewResourceVersionConflictsError("/quota/default/u", 0)
		}, nil)
	if !storage.IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("tryUpdate called %d times; caller conflicts must not be retried", calls)
	}
}

// Concurrent updates to one key all land: storage races are retried.
func TestStore_GuaranteedUpdateRetriesStorageRaces(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	s := New(db, testCodec, "")
	ctx := context.Background()
	key := "/quota/default/race"
	if err := s.Create(ctx, key, newTestObject("race", ""), &testKind{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.GuaranteedUpdate(ctx, key, &testKind{}, false, nil,
				func(in runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
					o := in.(*testKind)
					o.Spec.Value += "x"
					return o, nil, nil
				}, nil)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("GuaranteedUpdate: %v", err)
		}
	}
	got := &testKind{}
	if err := s.Get(ctx, key, storage.GetOptions{}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Spec.Value) != n {
		t.Fatalf("lost updates: value %q", got.Spec.Value)
	}
}

func TestStore_GetListPagination(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	s := New(db, testCodec, "")
	ctx := context.Background()
	createN(t, s, "/quota/page/", 5)

	var names []string
	cont := ""
	pages := 0
	for {
		list := &testKindList{}
		pred := storage.Everything
		pred.Limit = 2
		pred.Continue = cont
		if err := s.GetList(ctx, "/quota/page", storage.ListOptions{Recursive: true, Predicate: pred}, list); err != nil {
			t.Fatalf("GetList page %d: %v", pages, err)
		}
		pages++
		for _, it := range list.Items {
			names = append(names, it.Name)
		}
		if list.Continue == "" {
			break
		}
		if list.RemainingItemCount == nil || *list.RemainingItemCount != int64(5-len(names)) {
			t.Fatalf("remainingItemCount wrong after %d items: %v", len(names), list.RemainingItemCount)
		}
		cont = list.Continue
	}
	if pages != 3 || strings.Join(names, ",") != "item-00,item-01,item-02,item-03,item-04" {
		t.Fatalf("pages=%d names=%v", pages, names)
	}
}

// A continue token whose RV is stale (the range changed) gets 410 with an
// inconsistent-continue token.
func TestStore_GetListStaleContinueIsGone(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	s := New(db, testCodec, "")
	ctx := context.Background()
	createN(t, s, "/quota/stale/", 3)

	first := &testKindList{}
	pred := storage.Everything
	pred.Limit = 1
	if err := s.GetList(ctx, "/quota/stale", storage.ListOptions{Recursive: true, Predicate: pred}, first); err != nil {
		t.Fatalf("GetList: %v", err)
	}
	// Change an object after the first page.
	if err := s.GuaranteedUpdate(ctx, "/quota/stale/item-02", &testKind{}, false, nil,
		func(in runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			o := in.(*testKind)
			o.Spec.Value = "changed"
			return o, nil, nil
		}, nil); err != nil {
		t.Fatalf("update: %v", err)
	}
	pred.Continue = first.Continue
	err := s.GetList(ctx, "/quota/stale", storage.ListOptions{Recursive: true, Predicate: pred}, &testKindList{})
	var status *apierrors.StatusError
	if !apierrors.IsResourceExpired(err) || !errors.As(err, &status) || status.ErrStatus.Continue == "" {
		t.Fatalf("expected 410 with inconsistent continue token, got %v", err)
	}
}

// The list RV is the snapshot's high-water mark and is never ahead of or
// behind the items it returns.
func TestStore_GetListRVMatchesSnapshot(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	s := New(db, testCodec, "")
	ctx := context.Background()
	rvs := createN(t, s, "/quota/snap/", 3)
	list := &testKindList{}
	if err := s.GetList(ctx, "/quota/snap", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, list); err != nil {
		t.Fatalf("GetList: %v", err)
	}
	listRV, _ := strconv.ParseInt(list.ResourceVersion, 10, 64)
	if listRV < rvs[len(rvs)-1] {
		t.Fatalf("list RV %d behind its newest item %d", listRV, rvs[len(rvs)-1])
	}
}

func TestStore_CompactRevision(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	s := New(db, testCodec, "")
	t.Cleanup(s.watcher.Stop)
	if got := s.CompactRevision(); got < 0 {
		t.Fatalf("CompactRevision negative: %d", got)
	}
}
