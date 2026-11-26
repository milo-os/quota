// Package watch provides a changelog-based watch implementation for PostgreSQL.
//
// A PostgresWatcher turns rows of the quota_changelog table into Kubernetes
// watch.Event objects. It implements the watch contract expected by
// k8s.io/apiserver clients: optional initial events, an ordered event
// stream, bookmarks, exact resume from a resource version, and 410 Gone
// once the requested history has been compacted.
//
// # Ordering: resource versions are assigned in commit order
//
// The schema (migrations/001_initial_schema.sql) makes every writer draw its resource version with
// quota_next_rv(), which takes a transaction-scoped advisory lock that is
// held until COMMIT. PostgreSQL releases transaction locks only after the
// commit is visible, so the next writer cannot draw an RV until every lower
// RV is visible. Two consequences:
//
//   - RV order equals commit order. The watcher scans quota_changelog by
//     (resource_version, id) and never meets a lower RV later.
//   - Every snapshot sees an exact RV prefix. In a single statement the
//     watcher reads quota_rv_state.committed_rv (the high-water mark) and
//     the changelog rows up to it. All of those rows are visible in that
//     snapshot, so "fewer rows than the batch limit" means "caught up to the
//     high-water mark".
//
// An earlier version ordered by (commit_xid, id) below the xmin horizon.
// That is not commit order: an xid is assigned at a transaction's first
// write, so a transaction that started writing earlier but committed later
// sorted first, and the same key could be emitted out of order.
//
// A statement-level AFTER INSERT trigger on quota_changelog
// rejects (SQLSTATE 40001) any row whose RV was drawn before the lock, so a
// non-compliant writer fails safe instead of corrupting the order.
//
// # Resume and compaction
//
// The cursor is the client's resource version: a watch from RV N emits
// exactly the changelog rows with RV > N. The cleaner deletes changelog
// rows by RV, not by row age alone, and records the highest deleted RV in
// quota_changelog_compaction before it deletes anything. Every poll reads
// that floor in the same snapshot as the rows. If the cursor is below it,
// the watch ends with a 410 Gone (ResourceExpired) error event, as the etcd3
// backend does when a revision has been compacted. This also covers watchers
// that fall behind the retention window while connected.
//
// # Shared listener and cleaner
//
// A Hub owns one LISTEN connection and one changelog cleaner per database.
// The RESTOptionsGetter creates one Hub and every resource Store shares it,
// so the cost no longer scales with the number of resource types.
package watch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
)

const (
	// DefaultPollInterval is the safety-net poll interval. LISTEN/NOTIFY
	// drives the steady state; the poll only catches notifications lost
	// while the LISTEN connection reconnects.
	DefaultPollInterval = 5 * time.Second
	// DefaultBookmarkInterval is the interval between periodic bookmarks.
	DefaultBookmarkInterval = 30 * time.Second
	// DefaultChangelogRetention is how long changelog rows are kept before
	// the cleaner may compact them. Watchers resuming from an older RV get
	// 410 Gone.
	DefaultChangelogRetention = 24 * time.Hour
	// DefaultCleanupInterval is the interval between compaction runs.
	DefaultCleanupInterval = 1 * time.Hour
	// MinChangelogRetention is the floor applied to the configured
	// retention. The cacher's EventsHistoryWindow promises at least 75s of
	// history, so anything shorter would break watch resumption from the
	// watch cache.
	MinChangelogRetention = 2 * time.Minute

	// notifyCoalesceDelay is the debounce window applied to NOTIFY kicks.
	// Delaying a poll only affects latency, never correctness.
	notifyCoalesceDelay = 50 * time.Millisecond
	// pollBatchSize is the LIMIT of one changelog scan. A drain loops until
	// a scan returns fewer rows, so this bounds memory, not progress.
	pollBatchSize = 500
	// cleanupDeleteBatch bounds a single DELETE so compaction never holds a
	// long-running transaction open.
	cleanupDeleteBatch = 5000

	// notifyChannelName must match quota_notify_changelog() in
	// migrations/001_initial_schema.sql.
	notifyChannelName    = "quota_changes"
	listenerMinReconnect = 1 * time.Second
	listenerMaxReconnect = 30 * time.Second
)

// SQL shared with the Store and the native QuotaClaim layer. All three read
// the same tables so their resource versions agree.
const (
	// CurrentRVQuery returns the highest committed resource version. It
	// reads the high-water mark row maintained by the commit-order trigger,
	// which changelog cleanup never deletes, so the value never goes
	// backwards.
	CurrentRVQuery = `SELECT committed_rv FROM quota_rv_state WHERE singleton`
	// StateQuery returns (committed_rv, compacted_rv) in one snapshot.
	// Scalar subqueries make the planner count exactly one row. A join of
	// the two singleton tables was estimated from table size instead (905
	// rows for a never-analyzed quota_changelog_compaction), which
	// multiplied the estimated cost of every poll that joins it and could
	// push a poll over jit_above_cost.
	StateQuery = `SELECT (SELECT committed_rv FROM quota_rv_state WHERE singleton) AS committed_rv,
		(SELECT compacted_rv FROM quota_changelog_compaction WHERE singleton) AS compacted_rv`
	// CompactedRVQuery returns the compaction floor.
	CompactedRVQuery = `SELECT compacted_rv FROM quota_changelog_compaction WHERE singleton`
)

// ChangelogDataSQL returns the expression that yields the object data of
// the quota_changelog row aliased alias. Select it in place of the data
// column, in the same statement (one snapshot) as the row.
//
// Rows normally carry their data. An ADDED row written by the allocator
// carries NULL, because it would repeat the bytes written to
// quota_objects in the same statement. Such a row is rebuilt from:
//
//  1. the quota_objects row, if the object still has that resource
//     version; otherwise
//  2. the next changelog row for the key, if it is the DELETED row: a
//     DELETED row carries the object's last state, which is this version.
//
// Every UPDATE of quota_objects first fills its current changelog row if
// it is NULL (postgres.ChangelogBackfillCTE), so a NULL row is never
// followed by a MODIFIED row. Its successor has a higher resource version,
// so compaction cannot delete the successor and keep the row. The
// expression is NULL only if writers broke that rule; callers treat a NULL
// result as history that is no longer available.
//
// The object lookup uses the primary key. The successor lookup uses
// idx_quota_changelog_key (hashtext(key)).
//
// Apply it above the LIMIT of a poll, inside the lateral subquery, so only
// returned rows are rebuilt and the planner charges its subplans once per
// returned row.
func ChangelogDataSQL(alias string) string {
	return `CASE WHEN ` + alias + `.data IS NOT NULL THEN ` + alias + `.data
		WHEN ` + alias + `.event_type = 'ADDED' THEN COALESCE(
		  (SELECT o.data FROM quota_objects o
		    WHERE o.key = ` + alias + `.key AND o.resource_version = ` + alias + `.resource_version),
		  (SELECT CASE WHEN n.event_type = 'DELETED' THEN n.data END
		     FROM quota_changelog n
		    WHERE hashtext(n.key) = hashtext(` + alias + `.key) AND n.key = ` + alias + `.key
		      AND (n.resource_version, n.id) > (` + alias + `.resource_version, ` + alias + `.id)
		    ORDER BY n.resource_version, n.id
		    LIMIT 1))
		END`
}

// Options tunes a Hub. Zero values fall back to the defaults.
type Options struct {
	// PollInterval is the safety-net poll interval.
	PollInterval time.Duration
	// BookmarkInterval is the interval between periodic bookmarks.
	BookmarkInterval time.Duration
	// Retention is the minimum age of changelog rows before compaction.
	Retention time.Duration
	// CleanupInterval is the interval between compaction runs.
	CleanupInterval time.Duration
}

// withDefaults fills unset fields and clamps retention to the minimum the
// cacher depends on.
func (o Options) withDefaults() Options {
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.BookmarkInterval <= 0 {
		o.BookmarkInterval = DefaultBookmarkInterval
	}
	if o.Retention <= 0 {
		o.Retention = DefaultChangelogRetention
	}
	if o.Retention < MinChangelogRetention {
		klog.Warningf("PostgresWatcher: changelog retention %s is below the minimum %s; using the minimum", o.Retention, MinChangelogRetention)
		o.Retention = MinChangelogRetention
	}
	if o.CleanupInterval <= 0 {
		o.CleanupInterval = DefaultCleanupInterval
	}
	return o
}

// Hub owns the per-database resources shared by every watcher: one LISTEN
// connection that fans NOTIFY kicks out to all active watches, and one
// changelog cleaner. Create one per *sql.DB and share it across Stores.
type Hub struct {
	db   *sql.DB
	dsn  string
	opts Options

	startOnce   sync.Once
	compactOnce sync.Once
	stopOnce    sync.Once
	done        chan struct{}

	// compactedRV caches the last observed compaction floor for
	// CompactRevision, which has no context and must not block.
	compactedRV atomic.Int64

	mu     sync.RWMutex
	active map[*postgresWatch]struct{}
	// subs are kick channels of watches served outside this package (the
	// native QuotaClaim watch); see Subscribe.
	subs map[chan struct{}]struct{}
}

// NewHub creates a Hub. dsn is used for the dedicated LISTEN connection; if
// it is empty, watchers rely on the safety poll only.
func NewHub(db *sql.DB, dsn string, opts Options) *Hub {
	return &Hub{
		db:     db,
		dsn:    dsn,
		opts:   opts.withDefaults(),
		done:   make(chan struct{}),
		active: make(map[*postgresWatch]struct{}),
		subs:   make(map[chan struct{}]struct{}),
	}
}

// Options returns the effective options.
func (h *Hub) Options() Options { return h.opts }

// start launches the listener and cleaner on first use.
func (h *Hub) start() {
	h.StartCompaction()
	h.startOnce.Do(h.startListener)
}

// StartCompaction starts the changelog cleaner without the listener. Watches
// start both on first use; a database that is written but never watched
// (the dual-mode shadow) calls this so its changelog is still compacted.
// Idempotent; Stop ends it.
func (h *Hub) StartCompaction() {
	h.compactOnce.Do(func() { go h.cleanupLoop() })
}

// Subscribe registers a kick channel that receives a (coalesced) signal
// whenever the shared LISTEN connection sees a changelog notification, or
// the listener reconnects. It is how watches outside this package share the
// one LISTEN connection instead of holding a connection each. The channel
// has capacity one: a pending kick already covers later ones. Call cancel
// when the watch ends.
func (h *Hub) Subscribe() (kicks <-chan struct{}, cancel func()) {
	h.start()
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs, ch)
			h.mu.Unlock()
		})
	}
}

// DB returns the database the hub serves.
func (h *Hub) DB() *sql.DB { return h.db }

// Stop terminates the listener and cleaner. Active watches keep running
// until their own context ends, but receive no more NOTIFY kicks.
func (h *Hub) Stop() {
	h.stopOnce.Do(func() { close(h.done) })
}

func (h *Hub) register(w *postgresWatch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active[w] = struct{}{}
}

func (h *Hub) unregister(w *postgresWatch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.active, w)
}

// kickAll asks every active watch to drain. Non-blocking: a full kick
// channel already has a pending drain that will see the same rows.
func (h *Hub) kickAll() {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for w := range h.active {
		select {
		case w.kick <- struct{}{}:
		default:
		}
	}
	for ch := range h.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// notifyProgress asks every watch owned by owner to drain and emit a
// bookmark at the RV it has delivered up to.
func (h *Hub) notifyProgress(owner *PostgresWatcher) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for w := range h.active {
		if w.owner != owner {
			continue
		}
		select {
		case w.progress <- struct{}{}:
		default:
		}
	}
}

// CompactedRV returns the cached compaction floor. If it has never been
// loaded it reads it with a short timeout.
func (h *Hub) CompactedRV() int64 {
	if v := h.compactedRV.Load(); v > 0 {
		return v
	}
	if h.db == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var rv int64
	if err := h.db.QueryRowContext(ctx, CompactedRVQuery).Scan(&rv); err != nil {
		klog.V(2).InfoS("PostgresWatcher: failed to read compaction floor", "err", err)
		return 0
	}
	h.observeCompacted(rv)
	return rv
}

// observeCompacted records a compaction floor seen by a poll or cleanup.
func (h *Hub) observeCompacted(rv int64) {
	for {
		cur := h.compactedRV.Load()
		if rv <= cur || h.compactedRV.CompareAndSwap(cur, rv) {
			return
		}
	}
}

// startListener holds one pgx connection LISTENing on quota_changes and
// kicks every active watch on each notification. The safety poll covers
// any reconnect gap; the RV cursor never skips a committed row.
func (h *Hub) startListener() {
	if h.dsn == "" {
		klog.V(2).InfoS("PostgresWatcher: no DSN provided, LISTEN/NOTIFY disabled, falling back to polling only")
		return
	}
	go func() {
		klog.V(2).InfoS("PostgresWatcher: starting LISTEN/NOTIFY loop", "channel", notifyChannelName)
		defer klog.V(2).InfoS("PostgresWatcher: LISTEN/NOTIFY loop stopped")
		backoff := listenerMinReconnect
		for {
			select {
			case <-h.done:
				return
			default:
			}
			err := h.runListener()
			if err == nil || errors.Is(err, context.Canceled) {
				return
			}
			klog.ErrorS(err, "PostgresWatcher: LISTEN connection lost, reconnecting", "backoff", backoff)
			select {
			case <-h.done:
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > listenerMaxReconnect {
				backoff = listenerMaxReconnect
			}
		}
	}()
}

// runListener returns nil on shutdown and an error when it must reconnect.
func (h *Hub) runListener() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-h.done:
			cancel()
		case <-ctx.Done():
		}
	}()

	conn, err := pgx.Connect(ctx, h.dsn)
	if err != nil {
		return fmt.Errorf("pgx connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannelName); err != nil {
		return fmt.Errorf("LISTEN %s: %w", notifyChannelName, err)
	}
	klog.V(2).InfoS("PostgresWatcher: LISTEN connection established", "channel", notifyChannelName)

	// Drain anything committed while we were disconnected.
	h.kickAll()

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("WaitForNotification: %w", err)
		}
		if n != nil {
			h.kickAll()
		}
	}
}

// cleanupLoop periodically compacts the changelog.
func (h *Hub) cleanupLoop() {
	if h.db == nil {
		return
	}
	ticker := time.NewTicker(h.opts.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), h.opts.CleanupInterval)
			if _, err := h.Compact(ctx, time.Now().Add(-h.opts.Retention)); err != nil {
				klog.ErrorS(err, "Failed to compact changelog")
			}
			cancel()
		}
	}
}

// Compact deletes changelog rows at or below the highest RV written before
// cutoff and returns the new compaction floor.
//
// The floor is raised in its own committed statement BEFORE any row is
// deleted, so a watch or list that resumes from an RV inside the deleted
// range always sees the floor and gets 410 Gone, never a silent gap. Rows
// are then deleted in bounded batches to avoid one long transaction.
//
// Retention is time-based only: a connected watcher that falls behind the
// floor is ended with 410 Gone rather than holding compaction back.
func (h *Hub) Compact(ctx context.Context, cutoff time.Time) (int64, error) {
	var cut int64
	if err := h.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(resource_version), 0) FROM quota_changelog WHERE created_at < $1`,
		cutoff,
	).Scan(&cut); err != nil {
		return 0, fmt.Errorf("find compaction cut: %w", err)
	}

	var floor int64
	if err := h.db.QueryRowContext(ctx,
		`UPDATE quota_changelog_compaction
		    SET compacted_rv = GREATEST(compacted_rv, $1),
		        compacted_at = CASE WHEN $1 > compacted_rv THEN NOW() ELSE compacted_at END
		  WHERE singleton
		RETURNING compacted_rv`,
		cut,
	).Scan(&floor); err != nil {
		return 0, fmt.Errorf("raise compaction floor: %w", err)
	}
	h.observeCompacted(floor)

	var total int64
	for {
		res, err := h.db.ExecContext(ctx,
			`DELETE FROM quota_changelog
			  WHERE id IN (SELECT id FROM quota_changelog WHERE resource_version <= $1 LIMIT $2)`,
			floor, cleanupDeleteBatch,
		)
		if err != nil {
			return floor, fmt.Errorf("delete compacted changelog rows: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return floor, fmt.Errorf("rows affected: %w", err)
		}
		total += n
		if n < cleanupDeleteBatch {
			break
		}
	}
	if total > 0 {
		klog.V(2).InfoS("Compacted changelog", "count", total, "compactedRV", floor)
	}
	return floor, nil
}

// PostgresWatcher serves watches for one Store. It shares a Hub with every
// other Store on the same database.
type PostgresWatcher struct {
	hub     *Hub
	ownsHub bool
	codec   runtime.Codec

	// excludedKeyPrefixes lists key prefixes this watcher does not emit.
	// When the postgres-native QuotaClaim layer is active the claim prefix
	// is excluded here because that layer serves claim watches itself.
	excludedKeyPrefixes []string
}

// New creates a PostgresWatcher with its own Hub. Prefer NewWithHub so that
// all Stores share one listener and cleaner.
func New(db *sql.DB, codec runtime.Codec, dsn string) *PostgresWatcher {
	return NewWithExclusions(db, codec, dsn, nil)
}

// NewWithExclusions creates a PostgresWatcher with its own Hub that skips
// keys under the given prefixes.
func NewWithExclusions(db *sql.DB, codec runtime.Codec, dsn string, excludedKeyPrefixes []string) *PostgresWatcher {
	pw := NewWithHub(NewHub(db, dsn, Options{}), codec, excludedKeyPrefixes)
	pw.ownsHub = true
	return pw
}

// NewWithHub creates a PostgresWatcher on a shared Hub.
func NewWithHub(hub *Hub, codec runtime.Codec, excludedKeyPrefixes []string) *PostgresWatcher {
	return &PostgresWatcher{
		hub:                 hub,
		codec:               codec,
		excludedKeyPrefixes: append([]string(nil), excludedKeyPrefixes...),
	}
}

// Hub returns the shared hub.
func (pw *PostgresWatcher) Hub() *Hub { return pw.hub }

// NotifyProgress is called by Store.RequestWatchProgress. Each of this
// watcher's watches drains the changelog and then emits a bookmark at the
// RV it has delivered up to. The drain reads a snapshot taken after the
// request, so the bookmark covers every write committed before it.
func (pw *PostgresWatcher) NotifyProgress() {
	pw.hub.notifyProgress(pw)
}

// Stop stops the Hub if this watcher owns it.
func (pw *PostgresWatcher) Stop() {
	if pw.ownsHub {
		pw.hub.Stop()
	}
}

// Watch starts a watch on key. Semantics follow the etcd3 backend:
//
//   - RV > 0: stream changes with RV > that value. If the changelog has
//     been compacted past it, the stream ends with 410 Gone.
//   - RV 0 and SendInitialEvents unset (legacy): ADDED events for the
//     current state, then changes after that state.
//   - RV 0 and SendInitialEvents=false: changes after the current RV only.
//   - SendInitialEvents=true: ADDED events for the current state, then an
//     initial-events-end bookmark when bookmarks are allowed, then changes.
//
// newFunc builds bookmark objects; bookmarks are skipped if it is nil.
func (pw *PostgresWatcher) Watch(ctx context.Context, key string, opts storage.ListOptions, newFunc func() runtime.Object) (watch.Interface, error) {
	if pw.hub.db == nil {
		return nil, storage.NewInternalError(errors.New("postgres watcher has no database"))
	}
	pw.hub.start()

	startRV, err := storage.APIObjectVersioner{}.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}

	prefix := key
	if opts.Recursive && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	w := &postgresWatch{
		hub:                 pw.hub,
		owner:               pw,
		codec:               pw.codec,
		key:                 key,
		prefix:              prefix,
		recursive:           opts.Recursive,
		predicate:           opts.Predicate,
		filtered:            !predicateIsEverything(opts.Predicate),
		newFunc:             newFunc,
		startRV:             int64(startRV),
		sendInitialEvents:   initialEventsRequired(startRV, opts),
		initialEndBookmark:  opts.SendInitialEvents != nil && *opts.SendInitialEvents && opts.Predicate.AllowWatchBookmarks,
		bookmarks:           newFunc != nil && (opts.ProgressNotify || opts.Predicate.AllowWatchBookmarks),
		result:              make(chan watch.Event, 100),
		done:                make(chan struct{}),
		progress:            make(chan struct{}, 1),
		kick:                make(chan struct{}, 1),
		excludedKeyPrefixes: pw.excludedKeyPrefixes,
	}
	// A watch from "now" must deliver every write that commits after Watch
	// returns, so the starting RV is read here rather than by the watch
	// goroutine, which may not run until after the caller's next write.
	if !w.sendInitialEvents && w.startRV == 0 {
		if err := pw.hub.db.QueryRowContext(ctx, CurrentRVQuery).Scan(&w.startRV); err != nil {
			return nil, storage.NewInternalError(fmt.Errorf("read current resource version: %w", err))
		}
	}
	pw.hub.register(w)
	go w.run(ctx)
	return w, nil
}

// initialEventsRequired mirrors etcd3's areInitialEventsRequired.
func initialEventsRequired(rv uint64, opts storage.ListOptions) bool {
	if opts.SendInitialEvents == nil {
		return rv == 0
	}
	return *opts.SendInitialEvents
}

// postgresWatch implements watch.Interface for one watch request.
//
// Cursor semantics: (cursorRV, cursorID) is the last changelog row
// examined; the next scan returns rows strictly after it in
// (resource_version, id) order. deliveredRV is the RV up to which every
// matching event has been sent, and is the only RV ever put in a bookmark.
type postgresWatch struct {
	hub       *Hub
	owner     *PostgresWatcher
	codec     runtime.Codec
	key       string
	prefix    string
	recursive bool
	predicate storage.SelectionPredicate
	// filtered is true when predicate selects something narrower than
	// everything. Polls then also read each MODIFIED row's previous state,
	// so an object leaving or entering the selection can be reported as
	// DELETED or ADDED (see transformEvent).
	filtered bool
	newFunc  func() runtime.Object

	startRV            int64
	sendInitialEvents  bool
	initialEndBookmark bool
	bookmarks          bool

	cursorRV       int64
	cursorID       int64
	deliveredRV    int64
	lastBookmarkRV int64

	result   chan watch.Event
	done     chan struct{}
	progress chan struct{}
	kick     chan struct{}
	stopOnce sync.Once

	excludedKeyPrefixes []string
}

// errWatchEnded signals that the watch has been stopped or has already
// emitted a terminal error event.
var errWatchEnded = errors.New("watch ended")

// ResultChan returns the channel of watch events.
func (w *postgresWatch) ResultChan() <-chan watch.Event { return w.result }

// Stop stops the watch. Idempotent.
func (w *postgresWatch) Stop() {
	w.stopOnce.Do(func() {
		if w.hub != nil {
			w.hub.unregister(w)
		}
		close(w.done)
	})
}

// run drives the watch until the context ends, Stop is called, or a
// terminal error is emitted.
func (w *postgresWatch) run(ctx context.Context) {
	defer close(w.result)
	defer w.hub.unregister(w)

	if err := w.start(ctx); err != nil {
		if !errors.Is(err, errWatchEnded) {
			klog.ErrorS(err, "PostgresWatcher: failed to start watch", "key", w.key)
			w.sendError(ctx, storage.NewInternalError(err))
		}
		return
	}

	pollTicker := time.NewTicker(w.hub.opts.PollInterval)
	defer pollTicker.Stop()
	bookmarkTicker := time.NewTicker(w.hub.opts.BookmarkInterval)
	defer bookmarkTicker.Stop()

	drain := func(reason string) bool {
		if err := w.drain(ctx); err != nil {
			if !errors.Is(err, errWatchEnded) && ctx.Err() == nil {
				// Transient database error: keep the cursor and retry on
				// the next kick or poll.
				klog.ErrorS(err, "PostgresWatcher: error draining changelog", "key", w.key, "reason", reason, "cursorRV", w.cursorRV)
				return true
			}
			return false
		}
		return true
	}

	// Catch up from the starting point straight away.
	if !drain("start") {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case <-w.progress:
			if !drain("progress") {
				return
			}
			if !w.sendBookmark(ctx, true) {
				return
			}
		case <-w.kick:
			timer := time.NewTimer(notifyCoalesceDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			case <-w.done:
				timer.Stop()
				return
			}
			select {
			case <-w.kick:
			default:
			}
			if !drain("notify") {
				return
			}
		case <-pollTicker.C:
			if !drain("poll") {
				return
			}
		case <-bookmarkTicker.C:
			if !w.sendBookmark(ctx, false) {
				return
			}
		}
	}
}

// start positions the cursor and emits initial events if required.
func (w *postgresWatch) start(ctx context.Context) error {
	if w.sendInitialEvents {
		if w.startRV > 0 {
			var current int64
			if err := w.hub.db.QueryRowContext(ctx, CurrentRVQuery).Scan(&current); err != nil {
				return fmt.Errorf("read current resource version: %w", err)
			}
			if w.startRV > current {
				w.sendError(ctx, storage.NewTooLargeResourceVersionError(uint64(w.startRV), uint64(current), 1))
				return errWatchEnded
			}
		}
		if err := w.sendInitialEventList(ctx); err != nil {
			return err
		}
		if w.initialEndBookmark {
			if !w.sendInitialEventsEndBookmark(ctx) {
				return errWatchEnded
			}
		}
		return nil
	}

	// Exact resume, or a watch from "now" whose starting RV Watch already
	// resolved. The first drain checks the compaction floor in the same
	// snapshot as the rows it reads.
	w.setCaughtUp(w.startRV)
	return nil
}

// setCaughtUp moves the cursor past every row at or below rv. It never
// moves the cursor backwards: a watch that starts from an RV above the
// committed one (the client saw it from another replica, or asked for a
// future RV) waits for it, as the etcd3 watcher does, instead of replaying
// the rows between the committed RV and its start.
func (w *postgresWatch) setCaughtUp(rv int64) {
	if rv < w.cursorRV {
		return
	}
	w.cursorRV = rv
	w.cursorID = math.MaxInt64
	if rv > w.deliveredRV {
		w.deliveredRV = rv
	}
}

// keyFilter appends the key-scope predicate and exclusions to a query.
// With changelog set, an exact-key predicate also matches hashtext(key), so
// it can use idx_quota_changelog_key; quota_objects
// has no such index and finds the key by its primary key.
func (w *postgresWatch) keyFilter(args []any, changelog bool) (string, []any) {
	var b strings.Builder
	if w.recursive {
		args = append(args, w.prefix)
		fmt.Fprintf(&b, " AND starts_with(key, $%d)", len(args))
	} else {
		args = append(args, w.key)
		if changelog {
			fmt.Fprintf(&b, " AND hashtext(key) = hashtext($%d)", len(args))
		}
		fmt.Fprintf(&b, " AND key = $%d", len(args))
	}
	for _, excl := range w.excludedKeyPrefixes {
		args = append(args, excl)
		fmt.Fprintf(&b, " AND NOT starts_with(key, $%d)", len(args))
	}
	return b.String(), args
}

// sendInitialEventList emits ADDED events for the current state. The rows
// and the high-water mark come from one REPEATABLE READ snapshot, and every
// snapshot sees an exact RV prefix, so the cursor is set to exactly that
// high-water mark: no gap and no duplicates at the list/watch boundary.
func (w *postgresWatch) sendInitialEventList(ctx context.Context) error {
	tx, err := w.hub.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin initial list snapshot: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			klog.ErrorS(rbErr, "Failed to rollback initial list snapshot", "key", w.key)
		}
	}()

	var snapshotRV int64
	if err := tx.QueryRowContext(ctx, CurrentRVQuery).Scan(&snapshotRV); err != nil {
		return fmt.Errorf("read snapshot resource version: %w", err)
	}

	filter, args := w.keyFilter(nil, false)
	query := `SELECT key, resource_version, data FROM quota_objects WHERE TRUE` + filter + ` ORDER BY key`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query objects for initial events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var key string
		var rv int64
		var data []byte
		if err := rows.Scan(&key, &rv, &data); err != nil {
			return fmt.Errorf("scan object row: %w", err)
		}
		event, err := w.toWatchEvent("ADDED", data, rv)
		if err != nil {
			klog.ErrorS(err, "Failed to convert object to initial event", "key", key, "rv", rv)
			continue
		}
		if !w.matches(event) {
			continue
		}
		if !w.send(ctx, *event) {
			return errWatchEnded
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate initial rows: %w", err)
	}
	w.setCaughtUp(snapshotRV)
	return nil
}

// sendInitialEventsEndBookmark emits the k8s.io/initial-events-end bookmark
// at the snapshot RV of the initial list.
func (w *postgresWatch) sendInitialEventsEndBookmark(ctx context.Context) bool {
	if w.newFunc == nil {
		klog.ErrorS(nil, "Cannot send initial-events-end bookmark: newFunc is nil", "key", w.key)
		return false
	}
	obj := w.newFunc()
	if err := (storage.APIObjectVersioner{}).UpdateObject(obj, uint64(w.deliveredRV)); err != nil {
		klog.ErrorS(err, "Failed to set resource version on bookmark object")
		return false
	}
	if err := storage.AnnotateInitialEventsEndBookmark(obj); err != nil {
		klog.ErrorS(err, "Failed to annotate initial-events-end bookmark")
		return false
	}
	if !w.send(ctx, watch.Event{Type: watch.Bookmark, Object: obj}) {
		return false
	}
	w.lastBookmarkRV = w.deliveredRV
	return true
}

// changeRow is one changelog row read by a poll.
type changeRow struct {
	key       string
	rv        int64
	id        int64
	eventType string
	data      []byte
	// prevData is the object before a MODIFIED change, read only for
	// filtered watches. nil means unknown (not read, or compacted).
	prevData []byte
}

// PrevDataSQL returns the expression that yields, for a MODIFIED
// quota_changelog row aliased alias, the object's state before that change:
// the data of the key's previous changelog row. It is NULL for other event
// types, and when the previous row has been compacted. Writers fill the
// previous row's data before any UPDATE (ChangelogBackfillCTE), so the
// previous row of a MODIFIED row always carries data while it exists.
// Like ChangelogDataSQL, apply it above the LIMIT of a poll.
func PrevDataSQL(alias string) string {
	return `CASE WHEN ` + alias + `.event_type = 'MODIFIED' THEN (
		  SELECT p.data FROM quota_changelog p
		   WHERE hashtext(p.key) = hashtext(` + alias + `.key) AND p.key = ` + alias + `.key
		     AND (p.resource_version, p.id) < (` + alias + `.resource_version, ` + alias + `.id)
		   ORDER BY p.resource_version DESC, p.id DESC
		   LIMIT 1)
		END`
}

// TransformEvent applies a watch selector to one change, with the etcd3
// watcher's semantics:
//
//   - ADDED is sent if the new object matches.
//   - DELETED is sent if the deleted object (its last state) matches.
//   - MODIFIED becomes MODIFIED if both the old and new states match, ADDED
//     if only the new one does, and DELETED (carrying the old state stamped
//     with the change's RV) if only the old one does.
//
// prev is the state before a MODIFIED change, or nil if it is unknown
// (compacted). An unknown previous state is treated as matching when the
// new state does not, so a client that may have held the object is told
// it left; a client that never held it ignores the DELETED.
//
// It returns nil when the change is not sent.
func TransformEvent(eventType watch.EventType, cur, prev runtime.Object, rv uint64, matches func(runtime.Object) bool) (*watch.Event, error) {
	switch eventType {
	case watch.Added, watch.Deleted:
		if !matches(cur) {
			return nil, nil
		}
		return &watch.Event{Type: eventType, Object: cur}, nil
	case watch.Modified:
		curOK := matches(cur)
		prevOK := prev == nil || matches(prev)
		switch {
		case curOK && prevOK:
			return &watch.Event{Type: watch.Modified, Object: cur}, nil
		case curOK:
			return &watch.Event{Type: watch.Added, Object: cur}, nil
		case prevOK:
			old := cur
			if prev != nil {
				old = prev
			}
			if err := (storage.APIObjectVersioner{}).UpdateObject(old, rv); err != nil {
				return nil, fmt.Errorf("set resource version: %w", err)
			}
			return &watch.Event{Type: watch.Deleted, Object: old}, nil
		default:
			return nil, nil
		}
	default:
		return nil, fmt.Errorf("unknown event type %q", eventType)
	}
}

// predicateIsEverything reports whether p selects every object. A
// zero-value predicate (nil Label and Field) means no selector.
func predicateIsEverything(p storage.SelectionPredicate) bool {
	return (p.Label == nil || p.Label.Empty()) && (p.Field == nil || p.Field.Empty())
}

// pollOnce reads one batch after the cursor. The high-water mark, the
// compaction floor, and the rows come from one statement, hence one
// snapshot.
func (w *postgresWatch) pollOnce(ctx context.Context) (committed, compacted int64, batch []changeRow, err error) {
	args := []any{w.cursorRV, w.cursorID}
	filter, args := w.keyFilter(args, true)
	args = append(args, pollBatchSize)
	limitArg := len(args)

	// The data expressions are applied above the LIMIT, so only returned
	// rows are rebuilt.
	prevExpr := "NULL::bytea"
	if w.filtered {
		prevExpr = PrevDataSQL("l")
	}
	query := `WITH st AS (` + StateQuery + `)
		SELECT st.committed_rv, st.compacted_rv, c.key, c.resource_version, c.id, c.event_type, c.data, c.prev_data
		  FROM st
		  LEFT JOIN LATERAL (
		    SELECT l.key, l.resource_version, l.id, l.event_type, ` + ChangelogDataSQL("l") + ` AS data,
		           ` + prevExpr + ` AS prev_data
		      FROM (
		        SELECT key, resource_version, id, event_type, data
		          FROM quota_changelog
		         WHERE (resource_version, id) > ($1, $2)
		           AND resource_version <= st.committed_rv` + filter + `
		         ORDER BY resource_version, id
		         LIMIT $` + fmt.Sprint(limitArg) + `
		      ) l
		     ORDER BY l.resource_version, l.id
		  ) c ON TRUE`

	rows, err := w.hub.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("query changelog: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			key, eventType sql.NullString
			rv, id         sql.NullInt64
			data, prev     []byte
		)
		if err := rows.Scan(&committed, &compacted, &key, &rv, &id, &eventType, &data, &prev); err != nil {
			return 0, 0, nil, fmt.Errorf("scan changelog row: %w", err)
		}
		if !key.Valid {
			continue
		}
		batch = append(batch, changeRow{key: key.String, rv: rv.Int64, id: id.Int64, eventType: eventType.String, data: data, prevData: prev})
	}
	if err := rows.Err(); err != nil {
		return 0, 0, nil, fmt.Errorf("iterate changelog rows: %w", err)
	}
	return committed, compacted, batch, nil
}

// drain emits every matching changelog row up to the current high-water
// mark, looping over batches until caught up. It returns errWatchEnded if
// the watch was stopped or expired.
func (w *postgresWatch) drain(ctx context.Context) error {
	for {
		committed, compacted, batch, err := w.pollOnce(ctx)
		if err != nil {
			return err
		}
		w.hub.observeCompacted(compacted)
		if w.cursorRV < compacted {
			// Rows between the cursor and the floor may be gone. Resuming
			// would skip events, so end the watch with 410 Gone.
			w.sendError(ctx, apierrors.NewResourceExpired(fmt.Sprintf(
				"The resourceVersion for the provided watch is too old: requested %d, compacted to %d", w.cursorRV, compacted)))
			return errWatchEnded
		}

		for _, row := range batch {
			if row.data == nil {
				// An elided ADDED row that can no longer be rebuilt
				// (see ChangelogDataSQL). Skipping it would lose the
				// event, so end the watch and let the client relist.
				klog.ErrorS(nil, "PostgresWatcher: changelog row has no data and cannot be rebuilt; ending watch with 410 Gone",
					"key", row.key, "rv", row.rv, "eventType", row.eventType)
				w.sendError(ctx, apierrors.NewResourceExpired(fmt.Sprintf(
					"The resourceVersion for the provided watch is too old: the object recorded at %d is no longer available", row.rv)))
				return errWatchEnded
			}
			w.cursorRV, w.cursorID = row.rv, row.id
			event, err := w.toWatchEvent(row.eventType, row.data, row.rv)
			if err != nil {
				klog.ErrorS(err, "Failed to convert changelog entry to watch event", "key", row.key, "rv", row.rv, "eventType", row.eventType)
				continue
			}
			if w.filtered {
				var prev runtime.Object
				if row.prevData != nil {
					if prev, _, err = w.codec.Decode(row.prevData, nil, nil); err != nil {
						klog.ErrorS(err, "Failed to decode previous changelog state", "key", row.key, "rv", row.rv)
						prev = nil
					}
				}
				event, err = TransformEvent(event.Type, event.Object, prev, uint64(row.rv), w.matchesObject)
				if err != nil {
					klog.ErrorS(err, "Failed to apply watch selector", "key", row.key, "rv", row.rv)
					continue
				}
				if event == nil {
					continue
				}
			}
			if !w.send(ctx, *event) {
				return errWatchEnded
			}
		}

		if len(batch) < pollBatchSize {
			// Everything at or below the high-water mark has been sent.
			w.setCaughtUp(committed)
			return nil
		}
		// A full batch may end in the middle of an RV, so only the RV
		// before the last row is known complete.
		if last := batch[len(batch)-1].rv - 1; last > w.deliveredRV {
			w.deliveredRV = last
		}
	}
}

// matches applies the watch predicate.
func (w *postgresWatch) matches(event *watch.Event) bool {
	return w.matchesObject(event.Object)
}

// matchesObject applies the watch predicate to obj.
func (w *postgresWatch) matchesObject(obj runtime.Object) bool {
	// A zero-value predicate (nil Label and Field) means no selector;
	// SelectionPredicate.Empty would dereference the nil selectors.
	if predicateIsEverything(w.predicate) {
		return true
	}
	ok, err := w.predicate.Matches(obj)
	return err == nil && ok
}

// send delivers an event, blocking until the consumer reads it or the watch
// ends.
func (w *postgresWatch) send(ctx context.Context, event watch.Event) bool {
	select {
	case w.result <- event:
		return true
	case <-w.done:
		return false
	case <-ctx.Done():
		return false
	}
}

// sendError emits a terminal error event.
func (w *postgresWatch) sendError(ctx context.Context, err error) {
	var status *apierrors.StatusError
	if !errors.As(err, &status) {
		status = apierrors.NewInternalError(err)
	}
	_ = w.send(ctx, watch.Event{Type: watch.Error, Object: &status.ErrStatus})
}

// sendBookmark emits a bookmark at deliveredRV, never past an event that
// has not been sent. Periodic bookmarks are skipped when nothing advanced;
// progress-requested bookmarks are always sent.
func (w *postgresWatch) sendBookmark(ctx context.Context, force bool) bool {
	if !w.bookmarks || w.deliveredRV == 0 {
		return true
	}
	if !force && w.deliveredRV <= w.lastBookmarkRV {
		return true
	}
	obj := w.newFunc()
	if err := (storage.APIObjectVersioner{}).UpdateObject(obj, uint64(w.deliveredRV)); err != nil {
		klog.ErrorS(err, "Failed to set resource version on bookmark object")
		return true
	}
	if !w.send(ctx, watch.Event{Type: watch.Bookmark, Object: obj}) {
		return false
	}
	w.lastBookmarkRV = w.deliveredRV
	return true
}

// toWatchEvent converts a changelog row into a watch.Event. The resource
// version always comes from the row column; the encoded blob may carry a
// stale or empty one.
func (w *postgresWatch) toWatchEvent(eventType string, data []byte, rv int64) (*watch.Event, error) {
	var watchType watch.EventType
	switch eventType {
	case "ADDED":
		watchType = watch.Added
	case "MODIFIED":
		watchType = watch.Modified
	case "DELETED":
		watchType = watch.Deleted
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
	if data == nil {
		return nil, fmt.Errorf("changelog entry has nil data")
	}
	obj, _, err := w.codec.Decode(data, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decode changelog data: %w", err)
	}
	if err := (storage.APIObjectVersioner{}).UpdateObject(obj, uint64(rv)); err != nil {
		return nil, fmt.Errorf("failed to set resource version: %w", err)
	}
	return &watch.Event{Type: watchType, Object: obj}, nil
}
