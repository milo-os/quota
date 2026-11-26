package watch

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"

	"go.miloapis.com/quota/internal/testutil/pgtest"
)

// testKind is a minimal runtime.Object for testing.
type testKind struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Value             string `json:"value,omitempty"`
}

func (t *testKind) DeepCopyObject() runtime.Object {
	out := *t
	out.ObjectMeta = *t.DeepCopy()
	return &out
}

var testScheme = runtime.NewScheme()
var testCodec runtime.Codec

func init() {
	gv := schema.GroupVersion{Group: "test.watch", Version: "v1"}
	internalGV := schema.GroupVersion{Group: "test.watch", Version: runtime.APIVersionInternal}
	testScheme.AddKnownTypeWithName(gv.WithKind("TestKind"), &testKind{})
	testScheme.AddKnownTypeWithName(internalGV.WithKind("TestKind"), &testKind{})
	codecs := serializer.NewCodecFactory(testScheme)
	testCodec = codecs.LegacyCodec(gv)
}

// fastOptions keeps DB tests quick without a LISTEN connection.
var fastOptions = Options{PollInterval: 50 * time.Millisecond, BookmarkInterval: 100 * time.Millisecond}

func getTestDB(t *testing.T) *sql.DB {
	t.Helper()
	return pgtest.OpenDB(t)
}

func cleanupTestData(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{`DELETE FROM quota_changelog`, `DELETE FROM quota_objects`} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
}

func newObj(name, ns, value string) *testKind {
	return &testKind{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.watch/v1", Kind: "TestKind"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Value:      value,
	}
}

// writeChange writes an object row and its changelog row the way the
// Store does, with an RV from quota_next_rv(), and returns the RV.
func writeChange(t *testing.T, db *sql.DB, key, eventType string, obj *testKind) int64 {
	t.Helper()
	data, err := runtime.Encode(testCodec, obj)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var rv int64
	err = db.QueryRow(
		`WITH rv AS MATERIALIZED (SELECT quota_next_rv() AS v),
		 obj AS (
		   INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data)
		   SELECT $1, v, 'TestKind', $4, $5, $3 FROM rv WHERE $2 <> 'DELETED'
		   ON CONFLICT (key) DO UPDATE SET resource_version = EXCLUDED.resource_version, data = EXCLUDED.data
		 ),
		 del AS (DELETE FROM quota_objects WHERE key = $1 AND $2 = 'DELETED'),
		 log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) SELECT $1, v, $2, $3 FROM rv)
		 SELECT v FROM rv`,
		key, eventType, data, obj.Namespace, obj.Name,
	).Scan(&rv)
	if err != nil {
		t.Fatalf("write change: %v", err)
	}
	return rv
}

func newWatcher(db *sql.DB) *PostgresWatcher {
	pw := NewWithHub(NewHub(db, "", fastOptions), testCodec, nil)
	pw.ownsHub = true
	return pw
}

func boolPtr(b bool) *bool { return &b }

func recv(t *testing.T, w watch.Interface) watch.Event {
	t.Helper()
	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("result channel closed")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}
	return watch.Event{}
}

func recvNonBookmark(t *testing.T, w watch.Interface) watch.Event {
	t.Helper()
	for {
		ev := recv(t, w)
		if ev.Type != watch.Bookmark {
			return ev
		}
	}
}

func rvOf(t *testing.T, obj runtime.Object) int64 {
	t.Helper()
	v, err := storage.APIObjectVersioner{}.ObjectResourceVersion(obj)
	if err != nil {
		t.Fatalf("rv: %v", err)
	}
	return int64(v)
}

// --- DB-free tests ---

func TestInitialEventsRequired(t *testing.T) {
	tests := []struct {
		name string
		rv   uint64
		sie  *bool
		want bool
	}{
		{"legacy rv 0", 0, nil, true},
		{"legacy rv set", 10, nil, false},
		{"sie false rv 0", 0, boolPtr(false), false},
		{"sie true rv set", 10, boolPtr(true), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := initialEventsRequired(tc.rv, storage.ListOptions{SendInitialEvents: tc.sie})
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOptionsWithDefaults(t *testing.T) {
	o := Options{}.withDefaults()
	if o.Retention != DefaultChangelogRetention || o.PollInterval != DefaultPollInterval ||
		o.BookmarkInterval != DefaultBookmarkInterval || o.CleanupInterval != DefaultCleanupInterval {
		t.Errorf("unexpected defaults: %+v", o)
	}
	if got := (Options{Retention: time.Second}).withDefaults().Retention; got != MinChangelogRetention {
		t.Errorf("retention not clamped: %s", got)
	}
	if got := (Options{Retention: 6 * time.Hour}).withDefaults().Retention; got != 6*time.Hour {
		t.Errorf("retention overridden: %s", got)
	}
}

func TestKeyFilter(t *testing.T) {
	w := &postgresWatch{key: "/g/r", prefix: "/g/r/", recursive: true, excludedKeyPrefixes: []string{"/g/r/x/"}}
	sqlText, args := w.keyFilter([]any{1, 2}, true)
	if !strings.Contains(sqlText, "starts_with(key, $3)") || !strings.Contains(sqlText, "NOT starts_with(key, $4)") {
		t.Errorf("unexpected filter: %s", sqlText)
	}
	if len(args) != 4 || args[2] != "/g/r/" || args[3] != "/g/r/x/" {
		t.Errorf("unexpected args: %v", args)
	}

	w = &postgresWatch{key: "/g/r/ns/name"}
	sqlText, args = w.keyFilter(nil, false)
	if !strings.Contains(sqlText, "key = $1") || strings.Contains(sqlText, "hashtext") || args[0] != "/g/r/ns/name" {
		t.Errorf("non-recursive filter wrong: %s %v", sqlText, args)
	}

	// On the changelog the exact key also matches its hash, so the poll can
	// use idx_quota_changelog_key.
	sqlText, args = w.keyFilter(nil, true)
	if !strings.Contains(sqlText, "hashtext(key) = hashtext($1)") || !strings.Contains(sqlText, "key = $1") || len(args) != 1 {
		t.Errorf("non-recursive changelog filter wrong: %s %v", sqlText, args)
	}
}

// Bookmarks must never advertise an RV past events that were not sent.
func TestSendBookmarkUsesDeliveredRV(t *testing.T) {
	w := &postgresWatch{
		newFunc:   func() runtime.Object { return &testKind{} },
		bookmarks: true,
		result:    make(chan watch.Event, 4),
		done:      make(chan struct{}),
	}
	ctx := context.Background()

	// Nothing delivered yet: no bookmark.
	w.sendBookmark(ctx, false)
	if len(w.result) != 0 {
		t.Fatalf("bookmark sent with deliveredRV 0")
	}

	w.deliveredRV = 7
	w.sendBookmark(ctx, false)
	ev := <-w.result
	if ev.Type != watch.Bookmark || rvOf(t, ev.Object) != 7 {
		t.Fatalf("expected bookmark at 7, got %v %v", ev.Type, ev.Object)
	}

	// Periodic bookmark is skipped when nothing advanced; forced is not.
	w.sendBookmark(ctx, false)
	if len(w.result) != 0 {
		t.Fatalf("duplicate periodic bookmark")
	}
	w.sendBookmark(ctx, true)
	if ev := <-w.result; rvOf(t, ev.Object) != 7 {
		t.Fatalf("forced bookmark at wrong RV")
	}

	// Disallowed bookmarks are never sent.
	w.bookmarks = false
	w.deliveredRV = 9
	w.sendBookmark(ctx, true)
	if len(w.result) != 0 {
		t.Fatalf("bookmark sent when not allowed")
	}
}

func TestSetCaughtUpNeverMovesDeliveredBackwards(t *testing.T) {
	w := &postgresWatch{}
	w.setCaughtUp(10)
	w.deliveredRV = 12
	w.setCaughtUp(11)
	if w.deliveredRV != 12 || w.cursorRV != 11 {
		t.Fatalf("delivered=%d cursor=%d", w.deliveredRV, w.cursorRV)
	}
}

func TestHubObserveCompacted(t *testing.T) {
	h := NewHub(nil, "", Options{})
	h.observeCompacted(5)
	h.observeCompacted(3)
	if got := h.CompactedRV(); got != 5 {
		t.Fatalf("compacted RV went backwards: %d", got)
	}
}

func TestToWatchEvent(t *testing.T) {
	w := &postgresWatch{codec: testCodec}
	data, err := runtime.Encode(testCodec, newObj("event-test", "", "val"))
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	tests := []struct {
		name      string
		eventType string
		data      []byte
		wantType  watch.EventType
		wantErr   bool
	}{
		{"ADDED event", "ADDED", data, watch.Added, false},
		{"MODIFIED event", "MODIFIED", data, watch.Modified, false},
		{"DELETED event", "DELETED", data, watch.Deleted, false},
		{"unknown event type", "UNKNOWN", data, "", true},
		{"nil data", "ADDED", nil, "", true},
		{"invalid data", "ADDED", []byte("not valid json"), "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event, err := w.toWatchEvent(tc.eventType, tc.data, 42)
			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if event.Type != tc.wantType {
				t.Errorf("expected type %v, got %v", tc.wantType, event.Type)
			}
			if rvOf(t, event.Object) != 42 {
				t.Errorf("RV not taken from the row column")
			}
		})
	}
}

func TestPostgresWatcher_New(t *testing.T) {
	pw := New(nil, testCodec, "")
	if pw == nil || pw.codec != testCodec || pw.Hub() == nil || !pw.ownsHub {
		t.Fatal("watcher not initialised")
	}
	pw.Stop()
	pw.Stop() // idempotent
}

// --- DB-gated tests ---

func TestPostgresWatch_StreamsEventsInOrder(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	pw := newWatcher(db)
	t.Cleanup(pw.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := pw.Watch(ctx, "/quota/ns", storage.ListOptions{
		ResourceVersion: "0", SendInitialEvents: boolPtr(false), Recursive: true, Predicate: storage.Everything,
	}, func() runtime.Object { return &testKind{} })
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()

	var rvs []int64
	for _, et := range []string{"ADDED", "MODIFIED", "DELETED"} {
		rvs = append(rvs, writeChange(t, db, "/quota/ns/multi", et, newObj("multi", "ns", et)))
	}
	for i, want := range []watch.EventType{watch.Added, watch.Modified, watch.Deleted} {
		ev := recvNonBookmark(t, w)
		if ev.Type != want || rvOf(t, ev.Object) != rvs[i] {
			t.Fatalf("event %d: got %v@%d, want %v@%d", i, ev.Type, rvOf(t, ev.Object), want, rvs[i])
		}
	}
}

// A watch from "now" must not replay history.
func TestPostgresWatch_FromNowSkipsHistory(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	writeChange(t, db, "/quota/now/old", "ADDED", newObj("old", "now", "old"))

	pw := newWatcher(db)
	t.Cleanup(pw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := pw.Watch(ctx, "/quota/now", storage.ListOptions{
		ResourceVersion: "", SendInitialEvents: boolPtr(false), Recursive: true, Predicate: storage.Everything,
	}, nil)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()

	writeChange(t, db, "/quota/now/new", "ADDED", newObj("new", "now", "new"))
	ev := recvNonBookmark(t, w)
	if ev.Object.(*testKind).Name != "new" {
		t.Fatalf("replayed history: got %q", ev.Object.(*testKind).Name)
	}
}

// Resume from an RV emits exactly the later rows.
func TestPostgresWatch_ExactResume(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	rv1 := writeChange(t, db, "/quota/r/a", "ADDED", newObj("a", "r", "1"))
	rv2 := writeChange(t, db, "/quota/r/b", "ADDED", newObj("b", "r", "2"))

	pw := newWatcher(db)
	t.Cleanup(pw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := pw.Watch(ctx, "/quota/r", storage.ListOptions{
		ResourceVersion: storageRV(rv1), Recursive: true, Predicate: storage.Everything,
	}, nil)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()
	ev := recvNonBookmark(t, w)
	if rvOf(t, ev.Object) != rv2 {
		t.Fatalf("resume from %d delivered RV %d, want %d", rv1, rvOf(t, ev.Object), rv2)
	}
}

// Resume from an RV below the compaction floor ends with 410 Gone.
func TestPostgresWatch_CompactedResumeIsGone(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	rv1 := writeChange(t, db, "/quota/c/a", "ADDED", newObj("a", "c", "1"))
	writeChange(t, db, "/quota/c/b", "ADDED", newObj("b", "c", "2"))

	pw := newWatcher(db)
	t.Cleanup(pw.Stop)
	if _, err := pw.Hub().Compact(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if pw.Hub().CompactedRV() <= rv1 {
		t.Fatalf("compaction floor did not advance past %d", rv1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := pw.Watch(ctx, "/quota/c", storage.ListOptions{
		ResourceVersion: storageRV(rv1), Recursive: true, Predicate: storage.Everything,
	}, nil)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()
	ev := recv(t, w)
	status, ok := ev.Object.(*metav1.Status)
	if ev.Type != watch.Error || !ok || status.Code != 410 {
		t.Fatalf("expected 410 error event, got %v %#v", ev.Type, ev.Object)
	}
	if !apierrors.IsResourceExpired(&apierrors.StatusError{ErrStatus: *status}) {
		t.Fatalf("expected ResourceExpired, got %v", status.Reason)
	}
}

// The current RV never goes backwards even when compaction empties the
// changelog.
func TestCurrentRVSurvivesCompaction(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	rv := writeChange(t, db, "/quota/m/a", "ADDED", newObj("a", "m", "1"))
	pw := newWatcher(db)
	t.Cleanup(pw.Stop)
	if _, err := pw.Hub().Compact(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	var current int64
	if err := db.QueryRow(CurrentRVQuery).Scan(&current); err != nil {
		t.Fatalf("current rv: %v", err)
	}
	if current < rv {
		t.Fatalf("current RV went backwards: %d < %d", current, rv)
	}
}

// Drains loop past the batch limit, and bookmarks never pass delivered rows.
func TestPostgresWatch_DrainsPastBatchLimit(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	pw := newWatcher(db)
	t.Cleanup(pw.Stop)

	start := writeChange(t, db, "/quota/big/seed", "ADDED", newObj("seed", "big", "0"))
	const n = pollBatchSize + 50
	var last int64
	for i := 0; i < n; i++ {
		last = writeChange(t, db, "/quota/big/k", "MODIFIED", newObj("k", "big", "v"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w, err := pw.Watch(ctx, "/quota/big", storage.ListOptions{
		ResourceVersion: storageRV(start), Recursive: true, ProgressNotify: true, Predicate: storage.Everything,
	}, func() runtime.Object { return &testKind{} })
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()

	var maxDelivered int64
	for got := 0; got < n; {
		ev := recv(t, w)
		rv := rvOf(t, ev.Object)
		if ev.Type == watch.Bookmark {
			if rv > maxDelivered {
				t.Fatalf("bookmark %d ahead of delivered %d", rv, maxDelivered)
			}
			continue
		}
		if rv <= maxDelivered {
			t.Fatalf("RV went backwards: %d after %d", rv, maxDelivered)
		}
		maxDelivered = rv
		got++
	}
	if maxDelivered != last {
		t.Fatalf("last delivered %d, want %d", maxDelivered, last)
	}
}

// The commit-order trigger rejects an RV drawn before the lock once a
// higher RV has committed.
func TestCommitOrderTriggerRejectsStaleRV(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	var stale int64
	if err := db.QueryRow(`SELECT nextval('quota_resource_version_seq')`).Scan(&stale); err != nil {
		t.Fatalf("nextval: %v", err)
	}
	writeChange(t, db, "/quota/o/a", "ADDED", newObj("a", "o", "1"))
	_, err := db.Exec(`INSERT INTO quota_changelog (key, resource_version, event_type, data) VALUES ('/quota/o/b', $1, 'ADDED', '\x7b7d')`, stale)
	if err == nil || !strings.Contains(err.Error(), "40001") {
		t.Fatalf("expected serialization failure, got %v", err)
	}
}

func TestPostgresWatch_InitialEventsThenStream(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	writeChange(t, db, "/quota/i/a", "ADDED", newObj("a", "i", "1"))
	pw := newWatcher(db)
	t.Cleanup(pw.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pred := storage.Everything
	pred.AllowWatchBookmarks = true
	w, err := pw.Watch(ctx, "/quota/i", storage.ListOptions{
		SendInitialEvents: boolPtr(true), Recursive: true, Predicate: pred,
	}, func() runtime.Object { return &testKind{} })
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()

	if ev := recv(t, w); ev.Type != watch.Added {
		t.Fatalf("expected initial ADDED, got %v", ev.Type)
	}
	ev := recv(t, w)
	if ok, _ := storage.HasInitialEventsEndBookmarkAnnotation(ev.Object); ev.Type != watch.Bookmark || !ok {
		t.Fatalf("expected initial-events-end bookmark, got %v", ev.Type)
	}
	rv := writeChange(t, db, "/quota/i/b", "ADDED", newObj("b", "i", "2"))
	if ev := recvNonBookmark(t, w); rvOf(t, ev.Object) != rv {
		t.Fatalf("expected streamed event at %d", rv)
	}
}

func TestPostgresWatch_StopClosesResultChan(t *testing.T) {
	db := getTestDB(t)
	pw := newWatcher(db)
	t.Cleanup(pw.Stop)
	w, err := pw.Watch(context.Background(), "/quota/stop", storage.ListOptions{
		SendInitialEvents: boolPtr(false), Recursive: true, Predicate: storage.Everything,
	}, nil)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	w.Stop()
	w.Stop()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, open := <-w.ResultChan():
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("ResultChan not closed after Stop")
		}
	}
}

func storageRV(rv int64) string { return strconv.FormatInt(rv, 10) }
