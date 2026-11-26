package watch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// valuePredicate selects testKind objects by their Value field.
func valuePredicate(value string) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label: labels.Everything(),
		Field: fields.OneTermEqualSelector("value", value),
		GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			o, ok := obj.(*testKind)
			if !ok {
				return nil, nil, fmt.Errorf("not a testKind: %T", obj)
			}
			return labels.Set(o.Labels), fields.Set{"value": o.Value}, nil
		},
	}
}

// expectNoEvent fails if a non-bookmark event arrives within d.
func expectNoEvent(t *testing.T, w watch.Interface, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("result channel closed")
			}
			if ev.Type == watch.Bookmark {
				continue
			}
			t.Fatalf("unexpected event %v %#v", ev.Type, ev.Object)
		case <-timer.C:
			return
		}
	}
}

// An object that stops matching the selector must produce DELETED, and one
// that starts matching must produce ADDED, as the etcd3 watcher does.
func TestPostgresWatch_SelectorTransitions(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	pw := newWatcher(db)
	t.Cleanup(pw.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w, err := pw.Watch(ctx, "/quota/sel", storage.ListOptions{
		ResourceVersion: "0", SendInitialEvents: boolPtr(false), Recursive: true, Predicate: valuePredicate("x"),
	}, func() runtime.Object { return &testKind{} })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	key := "/quota/sel/ns/a"
	rvAdd := writeChange(t, db, key, "ADDED", newObj("a", "ns", "x"))
	if ev := recvNonBookmark(t, w); ev.Type != watch.Added || rvOf(t, ev.Object) != rvAdd {
		t.Fatalf("expected ADDED at %d, got %v at %d", rvAdd, ev.Type, rvOf(t, ev.Object))
	}

	rvLeave := writeChange(t, db, key, "MODIFIED", newObj("a", "ns", "y"))
	ev := recvNonBookmark(t, w)
	if ev.Type != watch.Deleted || rvOf(t, ev.Object) != rvLeave {
		t.Fatalf("expected DELETED at %d when the object left the selector, got %v at %d", rvLeave, ev.Type, rvOf(t, ev.Object))
	}
	if got := ev.Object.(*testKind).Value; got != "x" {
		t.Fatalf("DELETED must carry the last matching state, got value %q", got)
	}

	// Still outside the selector: nothing.
	writeChange(t, db, key, "MODIFIED", newObj("a", "ns", "z"))
	expectNoEvent(t, w, 5*fastOptions.PollInterval)

	rvBack := writeChange(t, db, key, "MODIFIED", newObj("a", "ns", "x"))
	if ev := recvNonBookmark(t, w); ev.Type != watch.Added || rvOf(t, ev.Object) != rvBack {
		t.Fatalf("expected ADDED at %d when the object entered the selector, got %v at %d", rvBack, ev.Type, rvOf(t, ev.Object))
	}

	rvMod := writeChange(t, db, key, "MODIFIED", newObj("a", "ns", "x"))
	if ev := recvNonBookmark(t, w); ev.Type != watch.Modified || rvOf(t, ev.Object) != rvMod {
		t.Fatalf("expected MODIFIED at %d, got %v at %d", rvMod, ev.Type, rvOf(t, ev.Object))
	}
}

// A watch from a resource version above the committed one waits for it.
// Its cursor must never move back to the committed RV: the events at or
// below the requested RV are not part of the watch.
func TestPostgresWatch_FutureRVNeverMovesBackwards(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	pw := newWatcher(db)
	t.Cleanup(pw.Stop)

	var current int64
	if err := db.QueryRow(CurrentRVQuery).Scan(&current); err != nil {
		t.Fatalf("current rv: %v", err)
	}
	start := current + 3

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w, err := pw.Watch(ctx, "/quota/fut", storage.ListOptions{
		ResourceVersion: storageRV(start), Recursive: true, Predicate: storage.Everything,
	}, func() runtime.Object { return &testKind{} })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	// Let the first drain run against the lower committed RV.
	time.Sleep(3 * fastOptions.PollInterval)
	var last int64
	for i := 0; i < 4; i++ {
		last = writeChange(t, db, fmt.Sprintf("/quota/fut/ns/o%d", i), "ADDED", newObj(fmt.Sprintf("o%d", i), "ns", "v"))
	}
	if last <= start {
		t.Fatalf("test setup: last write %d not above start %d", last, start)
	}
	ev := recvNonBookmark(t, w)
	if got := rvOf(t, ev.Object); got <= start {
		t.Fatalf("watch from %d delivered an event at %d", start, got)
	}
}

// A hub whose database is written but never watched (the dual-mode shadow)
// still compacts its changelog once StartCompaction is called.
func TestHub_StartCompactionWithoutWatch(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	rv := writeChange(t, db, "/quota/compact/ns/a", "ADDED", newObj("a", "ns", "v"))
	if _, err := db.Exec(`UPDATE quota_changelog SET created_at = NOW() - interval '1 hour' WHERE resource_version = $1`, rv); err != nil {
		t.Fatalf("age row: %v", err)
	}
	hub := NewHub(db, "", Options{CleanupInterval: 50 * time.Millisecond, Retention: MinChangelogRetention})
	t.Cleanup(hub.Stop)
	hub.StartCompaction()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM quota_changelog WHERE resource_version = $1`, rv).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("changelog row older than retention was not compacted")
}
