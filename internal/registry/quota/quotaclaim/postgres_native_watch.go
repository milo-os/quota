package quotaclaim

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"

	"go.miloapis.com/quota/internal/metrics"
	pgwatch "go.miloapis.com/quota/internal/watch"
	"go.miloapis.com/quota/pkg/apis/quota"
)

const (
	// nativeWatchResultBuffer is the size of each watch stream's result
	// channel. Matches internal/watch/postgres.go.
	nativeWatchResultBuffer = 100
	// nativeWatchCoalesceDelay is the debounce window applied to NOTIFY
	// kicks. Delaying a drain only affects latency, never correctness.
	nativeWatchCoalesceDelay = 50 * time.Millisecond
	// nativeWatchBookmarkInterval is the periodic bookmark interval.
	nativeWatchBookmarkInterval = 30 * time.Second
	// nativeWatchPollInterval is the backup poll. It catches a NOTIFY lost
	// while the LISTEN connection is busy or reconnecting, so a missed
	// notification delays events by at most this long instead of stalling
	// them until the next write.
	nativeWatchPollInterval = 5 * time.Second
	// nativeWatchChangelogBatch is the LIMIT of one changelog scan. A drain
	// loops until a scan returns fewer rows.
	nativeWatchChangelogBatch = 500
)

// Watch implements rest.Watcher for the native QuotaClaim storage.
//
// Watches hold no database connection of their own. They share the watch
// hub's single LISTEN connection for wake-ups (pgwatch.Hub.Subscribe) and
// read the changelog with pooled connections, so the number of concurrent
// watches is not bounded by a connection cap. A backup poll covers missed
// notifications, and is the only source of wake-ups when there is no hub.
//
// Resource-version semantics match the etcd3 backend and the generic
// Postgres watcher (internal/watch), and use the same commit-ordered RV
// cursor:
//
//   - RV > 0: exactly the changes with RV > that value; 410 Gone if the
//     changelog has been compacted past it. An RV above the committed one
//     waits for it.
//   - RV ""/"0" with sendInitialEvents unset (legacy) or true: ADDED events
//     for the current state from one snapshot, then changes after it.
//   - RV ""/"0" with sendInitialEvents=false: changes after the current RV,
//     read before Watch returns.
//
// Label and field selectors apply to every event: the initial list pushes
// what it can into SQL, and streamed changes are matched in memory against
// the full selector, with etcd3's transitions (an object leaving the
// selector is DELETED, one entering it is ADDED).
//
// Sends block until the client reads, the watch is stopped, or the request
// ends: a slow client slows its own watch down instead of losing events.
func (r *PostgresNativeREST) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	namespace, _ := genericapirequest.NamespaceFrom(ctx)

	labelSel := labels.Everything()
	fieldSel := fields.Everything()
	var rvString string
	var sendInitial *bool
	allowBookmarks := false
	if options != nil {
		if options.LabelSelector != nil {
			labelSel = options.LabelSelector
		}
		if options.FieldSelector != nil {
			fieldSel = options.FieldSelector
		}
		rvString = options.ResourceVersion
		sendInitial = options.SendInitialEvents
		allowBookmarks = options.AllowWatchBookmarks
	}
	startRV, err := r.versioner.ParseResourceVersion(rvString)
	if err != nil {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}
	initial := startRV == 0
	if sendInitial != nil {
		initial = *sendInitial
	}

	// Subscribe before positioning the cursor, so no notification for a
	// row after the cursor can be missed.
	var kicks <-chan struct{}
	unsubscribe := func() {}
	if r.hub != nil {
		kicks, unsubscribe = r.hub.Subscribe()
	}

	// A watch from "now" must deliver every write that commits after Watch
	// returns, so its starting RV is read here, not by the goroutine.
	if !initial && startRV == 0 {
		current, err := r.currentResourceVersion(ctx)
		if err != nil {
			unsubscribe()
			return nil, apierrors.NewInternalError(fmt.Errorf("read current resource version: %w", err))
		}
		startRV = uint64(current)
	}

	runCtx, cancel := context.WithCancel(ctx)

	keyPrefix := claimKeyPrefix
	if namespace != "" {
		keyPrefix += namespace + "/"
	}
	w := &nativeClaimWatch{
		parent:             r,
		namespace:          namespace,
		keyPrefix:          keyPrefix,
		labelSel:           labelSel,
		fieldSel:           fieldSel,
		filtered:           !labelSel.Empty() || !fieldSel.Empty(),
		ch:                 make(chan watch.Event, nativeWatchResultBuffer),
		stopCh:             make(chan struct{}),
		kicks:              kicks,
		unsubscribe:        unsubscribe,
		startRV:            int64(startRV),
		sendInitialEvents:  initial,
		initialEndBookmark: sendInitial != nil && *sendInitial && allowBookmarks,
		bookmarks:          allowBookmarks,
		runCtx:             runCtx,
		cancel:             cancel,
	}

	metrics.QuotaClaimNativeActiveWatchers.Inc()
	go w.run(runCtx)
	return w, nil
}

// nativeClaimWatch is the per-request watch.Interface implementation.
//
// (cursorRV, cursorID) is the last changelog row examined. deliveredRV is
// the RV up to which every matching event has been sent; it is the only RV
// put in bookmarks.
type nativeClaimWatch struct {
	parent    *PostgresNativeREST
	namespace string
	keyPrefix string
	labelSel  labels.Selector
	fieldSel  fields.Selector
	// filtered is true when either selector is narrower than everything;
	// drains then also read each MODIFIED row's previous state.
	filtered bool

	ch     chan watch.Event
	stopCh chan struct{}
	once   sync.Once
	runCtx context.Context
	cancel context.CancelFunc

	// kicks delivers hub wake-ups; nil without a hub.
	kicks       <-chan struct{}
	unsubscribe func()

	startRV            int64
	sendInitialEvents  bool
	initialEndBookmark bool
	bookmarks          bool

	cursorRV       int64
	cursorID       int64
	deliveredRV    int64
	lastBookmarkRV int64
}

// errNativeWatchEnded signals the watch was stopped or already emitted a
// terminal error.
var errNativeWatchEnded = errors.New("native watch ended")

// ResultChan returns the channel on which watch events are delivered.
func (w *nativeClaimWatch) ResultChan() <-chan watch.Event { return w.ch }

// Stop terminates the watch. Idempotent.
func (w *nativeClaimWatch) Stop() {
	w.once.Do(func() {
		close(w.stopCh)
		if w.cancel != nil {
			w.cancel()
		}
	})
}

// run is the main watch loop.
func (w *nativeClaimWatch) run(ctx context.Context) {
	defer func() {
		w.cancel()
		w.unsubscribe()
		metrics.QuotaClaimNativeActiveWatchers.Dec()
		close(w.ch)
	}()

	if err := w.start(ctx); err != nil {
		if !errors.Is(err, errNativeWatchEnded) && ctx.Err() == nil {
			klog.ErrorS(err, "postgres-native watch: start failed")
			w.sendError(apierrors.NewInternalError(err))
		}
		return
	}

	pollTicker := time.NewTicker(nativeWatchPollInterval)
	defer pollTicker.Stop()
	bookmarkTicker := time.NewTicker(nativeWatchBookmarkInterval)
	defer bookmarkTicker.Stop()

	drain := func() bool {
		err := w.drainChangelog(ctx)
		if err == nil {
			return true
		}
		if errors.Is(err, errNativeWatchEnded) || ctx.Err() != nil {
			return false
		}
		// Transient error: keep the cursor and retry on the next kick or
		// backup poll.
		klog.ErrorS(err, "postgres-native watch: drain changelog", "cursorRV", w.cursorRV)
		return true
	}

	if !drain() {
		return
	}

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case <-w.kicks:
			timer := time.NewTimer(nativeWatchCoalesceDelay)
			select {
			case <-timer.C:
			case <-w.stopCh:
				timer.Stop()
				return
			case <-ctx.Done():
				timer.Stop()
				return
			}
			select {
			case <-w.kicks:
			default:
			}
			if !drain() {
				return
			}
		case <-pollTicker.C:
			if !drain() {
				return
			}
		case <-bookmarkTicker.C:
			if !w.emitBookmark() {
				return
			}
		}
	}
}

// start positions the cursor and emits initial events if required.
func (w *nativeClaimWatch) start(ctx context.Context) error {
	if w.sendInitialEvents {
		if w.startRV > 0 {
			current, err := w.parent.currentResourceVersion(ctx)
			if err != nil {
				return fmt.Errorf("read current resource version: %w", err)
			}
			if w.startRV > current {
				w.sendError(storage.NewTooLargeResourceVersionError(uint64(w.startRV), uint64(current), 1))
				return errNativeWatchEnded
			}
		}
		if err := w.emitInitialList(ctx); err != nil {
			return err
		}
		if w.initialEndBookmark && !w.emitInitialEventsEndBookmark() {
			return errNativeWatchEnded
		}
		return nil
	}
	// Exact resume, or a watch from "now" whose RV Watch already read. The
	// first drain checks the compaction floor in the same snapshot as the
	// rows.
	w.setCaughtUp(w.startRV)
	return nil
}

// setCaughtUp moves the cursor past every row at or below rv. It never
// moves the cursor backwards, so a watch from an RV above the committed one
// waits for it instead of replaying older rows.
func (w *nativeClaimWatch) setCaughtUp(rv int64) {
	if rv < w.cursorRV {
		return
	}
	w.cursorRV, w.cursorID = rv, math.MaxInt64
	if rv > w.deliveredRV {
		w.deliveredRV = rv
	}
}

// emitInitialList sends an ADDED event for every matching claim. The rows
// and the high-water mark come from one REPEATABLE READ snapshot, so the
// cursor starts exactly where the list ends.
func (w *nativeClaimWatch) emitInitialList(ctx context.Context) error {
	baseSQL, args, postFilter, err := buildListQuery(w.namespace, w.labelSel, w.fieldSel, nil)
	if err != nil {
		return err
	}
	sqlText, args := appendPage(baseSQL, args, "", false, 0)

	tx, err := w.parent.pool.Query.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin initial list snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var snapshotRV int64
	if err := tx.QueryRow(ctx, pgwatch.CurrentRVQuery).Scan(&snapshotRV); err != nil {
		return fmt.Errorf("read snapshot resource version: %w", err)
	}

	stopped := false
	_, _, err = w.parent.scanClaims(ctx, tx, sqlText, args, func(_ string, obj *quota.QuotaClaim) bool {
		if postFilter != nil && !postFilter(obj) {
			return true
		}
		if !w.send(watch.Event{Type: watch.Added, Object: obj}) {
			stopped = true
			return false
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("initial list query: %w", err)
	}
	if stopped {
		return errNativeWatchEnded
	}
	w.setCaughtUp(snapshotRV)
	return nil
}

// emitInitialEventsEndBookmark emits the k8s.io/initial-events-end bookmark.
func (w *nativeClaimWatch) emitInitialEventsEndBookmark() bool {
	obj := &quota.QuotaClaim{}
	if err := w.parent.versioner.UpdateObject(obj, uint64(w.deliveredRV)); err != nil {
		klog.ErrorS(err, "postgres-native watch: set rv on initial-end bookmark")
		return false
	}
	if err := storage.AnnotateInitialEventsEndBookmark(obj); err != nil {
		klog.ErrorS(err, "postgres-native watch: annotate initial-end bookmark")
		return false
	}
	if !w.send(watch.Event{Type: watch.Bookmark, Object: obj}) {
		return false
	}
	w.lastBookmarkRV = w.deliveredRV
	return true
}

// nativeChangeRow is one changelog row read by a drain.
type nativeChangeRow struct {
	id, rv         int64
	key, eventType string
	data           []byte
	// prevData is the state before a MODIFIED change, read only for
	// filtered watches; nil if unknown.
	prevData []byte
}

// pollChangelog reads one batch after the cursor together with the
// high-water mark and compaction floor, all in one statement (one
// snapshot).
func (w *nativeClaimWatch) pollChangelog(ctx context.Context) (committed, compacted int64, batch []nativeChangeRow, err error) {
	prevExpr := "NULL::bytea"
	if w.filtered {
		prevExpr = pgwatch.PrevDataSQL("l")
	}
	rows, err := w.parent.pool.Query.Query(ctx,
		`WITH st AS (`+pgwatch.StateQuery+`)
		 SELECT st.committed_rv, st.compacted_rv, c.id, c.resource_version, c.key, c.event_type, c.data, c.prev_data
		   FROM st
		   LEFT JOIN LATERAL (
		     SELECT l.id, l.resource_version, l.key, l.event_type, `+pgwatch.ChangelogDataSQL("l")+` AS data,
		            `+prevExpr+` AS prev_data
		       FROM (
		         SELECT id, resource_version, key, event_type, data
		           FROM quota_changelog
		          WHERE (resource_version, id) > ($1, $2)
		            AND resource_version <= st.committed_rv
		            AND starts_with(key, $3)
		          ORDER BY resource_version, id
		          LIMIT $4
		       ) l
		      ORDER BY l.resource_version, l.id
		   ) c ON TRUE`,
		w.cursorRV, w.cursorID, w.keyPrefix, nativeWatchChangelogBatch,
	)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("changelog query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id, rv         *int64
			key, eventType *string
			data, prev     []byte
		)
		if err := rows.Scan(&committed, &compacted, &id, &rv, &key, &eventType, &data, &prev); err != nil {
			return 0, 0, nil, fmt.Errorf("scan changelog row: %w", err)
		}
		if id == nil {
			continue
		}
		batch = append(batch, nativeChangeRow{id: *id, rv: *rv, key: *key, eventType: *eventType, data: data, prevData: prev})
	}
	return committed, compacted, batch, rows.Err()
}

// drainChangelog emits every matching row up to the high-water mark,
// looping over batches until caught up.
func (w *nativeClaimWatch) drainChangelog(ctx context.Context) error {
	for {
		committed, compacted, batch, err := w.pollChangelog(ctx)
		if err != nil {
			return err
		}
		if w.cursorRV < compacted {
			w.sendError(apierrors.NewResourceExpired(fmt.Sprintf(
				"The resourceVersion for the provided watch is too old: requested %d, compacted to %d", w.cursorRV, compacted)))
			return errNativeWatchEnded
		}
		for _, row := range batch {
			if row.data == nil {
				// An elided ADDED row that can no longer be rebuilt
				// (pgwatch.ChangelogDataSQL). Skipping it would lose the
				// event, so end the watch and let the client relist.
				klog.ErrorS(nil, "postgres-native watch: changelog row has no data and cannot be rebuilt; ending watch with 410 Gone",
					"key", row.key, "rv", row.rv, "eventType", row.eventType)
				w.sendError(apierrors.NewResourceExpired(fmt.Sprintf(
					"The resourceVersion for the provided watch is too old: the object recorded at %d is no longer available", row.rv)))
				return errNativeWatchEnded
			}
			w.cursorRV, w.cursorID = row.rv, row.id
			evtType, err := changelogEventType(row.eventType)
			if err != nil {
				klog.ErrorS(err, "postgres-native watch: unknown event type", "key", row.key, "eventType", row.eventType)
				continue
			}
			obj, err := w.parent.decodeClaim(row.data, row.rv)
			if err != nil {
				klog.ErrorS(err, "postgres-native watch: decode changelog row", "key", row.key, "rv", row.rv)
				continue
			}
			ev := &watch.Event{Type: evtType, Object: obj}
			if w.filtered {
				var prev runtime.Object
				if row.prevData != nil {
					if p, err := w.parent.decodeClaim(row.prevData, row.rv); err == nil {
						prev = p
					} else {
						klog.ErrorS(err, "postgres-native watch: decode previous state", "key", row.key, "rv", row.rv)
					}
				}
				ev, err = pgwatch.TransformEvent(evtType, obj, prev, uint64(row.rv), w.matches)
				if err != nil {
					klog.ErrorS(err, "postgres-native watch: apply selector", "key", row.key, "rv", row.rv)
					continue
				}
				if ev == nil {
					continue
				}
			}
			if !w.send(*ev) {
				return errNativeWatchEnded
			}
		}
		if len(batch) < nativeWatchChangelogBatch {
			w.setCaughtUp(committed)
			return nil
		}
		if last := batch[len(batch)-1].rv - 1; last > w.deliveredRV {
			w.deliveredRV = last
		}
	}
}

// matches applies the watch's full label and field selectors to a claim.
// Streamed changes are matched here, never by SQL: the changelog scan
// filters only by key prefix.
func (w *nativeClaimWatch) matches(obj runtime.Object) bool {
	claim, ok := obj.(*quota.QuotaClaim)
	if !ok {
		return false
	}
	return w.labelSel.Matches(labels.Set(claim.Labels)) && w.fieldSel.Matches(SelectableFields(claim))
}

// send delivers an event, blocking until the client reads it, the watch is
// stopped, or the request ends. Blocking applies backpressure to this watch
// only: the drain pauses and resumes from its cursor, and no event is
// dropped. This matches the etcd3 watcher.
func (w *nativeClaimWatch) send(ev watch.Event) bool {
	select {
	case w.ch <- ev:
		return true
	case <-w.stopCh:
		return false
	case <-w.runCtx.Done():
		return false
	}
}

// sendError emits a terminal watch.Error event, blocking like send. run()
// closes w.ch only after its last send, so the channel is open here.
func (w *nativeClaimWatch) sendError(err error) {
	var status *apierrors.StatusError
	if !errors.As(err, &status) {
		status = apierrors.NewInternalError(err)
	}
	w.send(watch.Event{Type: watch.Error, Object: &status.ErrStatus})
}

// emitBookmark sends a periodic bookmark at deliveredRV when it advanced
// and the client allowed bookmarks. It never runs ahead of an unsent event.
func (w *nativeClaimWatch) emitBookmark() bool {
	if !w.bookmarks || w.deliveredRV == 0 || w.deliveredRV <= w.lastBookmarkRV {
		return true
	}
	obj := &quota.QuotaClaim{}
	obj.ResourceVersion = strconv.FormatInt(w.deliveredRV, 10)
	if !w.send(watch.Event{Type: watch.Bookmark, Object: obj}) {
		return false
	}
	w.lastBookmarkRV = w.deliveredRV
	return true
}

// changelogEventType maps the quota_changelog.event_type string to the
// k8s watch.EventType enum.
func changelogEventType(eventType string) (watch.EventType, error) {
	switch eventType {
	case "ADDED":
		return watch.Added, nil
	case "MODIFIED":
		return watch.Modified, nil
	case "DELETED":
		return watch.Deleted, nil
	default:
		return "", fmt.Errorf("unknown event type %q", eventType)
	}
}
