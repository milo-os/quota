package quotaclaim

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"

	"go.miloapis.com/quota/pkg/apis/quota"
)

// seedClaimWith seeds a claim after mutate adjusts it.
func seedClaimWith(t *testing.T, r *PostgresNativeREST, name string, mutate func(*quota.QuotaClaim)) *quota.QuotaClaim {
	t.Helper()
	claim := validQuotaClaim()
	claim.Name = name
	claim.UID = types.UID("uid-test-" + name)
	if mutate != nil {
		mutate(claim)
	}
	data, err := runtime.Encode(r.codec, claim)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	key := claimKeyPrefix + claim.Namespace + "/" + name
	var rv int64
	err = r.pool.Query.QueryRow(context.Background(),
		`WITH rv AS MATERIALIZED (SELECT quota_next_rv() AS v),
		 ins AS (INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data)
		         SELECT $1, v, 'QuotaClaim', $2, $3, $4 FROM rv RETURNING resource_version),
		 log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data)
		         SELECT $1, resource_version, 'ADDED', $4 FROM ins)
		 SELECT resource_version FROM ins`,
		key, claim.Namespace, name, data,
	).Scan(&rv)
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	got, err := r.decodeClaim(data, rv)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

func fromNow() *metainternalversion.ListOptions {
	f := false
	return &metainternalversion.ListOptions{SendInitialEvents: &f}
}

func recvEvent(t *testing.T, w watch.Interface, d time.Duration) watch.Event {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("watch closed")
			}
			if ev.Type == watch.Bookmark {
				continue
			}
			return ev
		case <-timer.C:
			t.Fatal("timed out waiting for event")
		}
	}
}

func claimName(ev watch.Event) string {
	if c, ok := ev.Object.(*quota.QuotaClaim); ok {
		return c.Name
	}
	return fmt.Sprintf("<%T>", ev.Object)
}

// A field selector pushed into SQL for the initial list must still filter
// streamed events.
func TestNativeWatch_FieldSelectorFiltersStream(t *testing.T) {
	r := nativeTestREST(t)
	ctx, cancel := context.WithTimeout(nsCtx(), 20*time.Second)
	defer cancel()
	opts := fromNow()
	opts.FieldSelector = fields.OneTermEqualSelector("spec.consumerRef.name", "alice")
	w, err := r.Watch(ctx, opts)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	consumer := func(name string) func(*quota.QuotaClaim) {
		return func(c *quota.QuotaClaim) { c.Spec.ConsumerRef.Name = name }
	}
	seedClaimWith(t, r, "fs-alice-1", consumer("alice"))
	seedClaimWith(t, r, "fs-bob", consumer("bob"))
	seedClaimWith(t, r, "fs-alice-2", consumer("alice"))

	for _, want := range []string{"fs-alice-1", "fs-alice-2"} {
		if ev := recvEvent(t, w, 10*time.Second); claimName(ev) != want {
			t.Fatalf("expected %s, got %v %s", want, ev.Type, claimName(ev))
		}
	}
}

// An object that leaves a selector produces DELETED; one that enters it
// produces ADDED.
func TestNativeWatch_SelectorTransitions(t *testing.T) {
	r := nativeTestREST(t)
	claim := seedClaimWith(t, r, "lbl-a", func(c *quota.QuotaClaim) { c.Labels = map[string]string{"team": "a"} })

	ctx, cancel := context.WithTimeout(nsCtx(), 20*time.Second)
	defer cancel()
	opts := fromNow()
	opts.LabelSelector = labels.SelectorFromSet(labels.Set{"team": "a"})
	w, err := r.Watch(ctx, opts)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	relabel := func(team string) *quota.QuotaClaim {
		t.Helper()
		cur, err := r.Get(nsCtx(), claim.Name, &metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		next := cur.(*quota.QuotaClaim).DeepCopy()
		next.Labels = map[string]string{"team": team}
		obj, _, err := r.Update(nsCtx(), claim.Name, rest.DefaultUpdatedObjectInfo(next), nil, nil, false, &metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		return obj.(*quota.QuotaClaim)
	}

	left := relabel("b")
	ev := recvEvent(t, w, 10*time.Second)
	if ev.Type != watch.Deleted || ev.Object.(*quota.QuotaClaim).ResourceVersion != left.ResourceVersion {
		t.Fatalf("expected DELETED at %s, got %v %#v", left.ResourceVersion, ev.Type, ev.Object)
	}
	if got := ev.Object.(*quota.QuotaClaim).Labels["team"]; got != "a" {
		t.Fatalf("DELETED must carry the last matching state, got team=%q", got)
	}

	back := relabel("a")
	ev = recvEvent(t, w, 10*time.Second)
	if ev.Type != watch.Added || ev.Object.(*quota.QuotaClaim).ResourceVersion != back.ResourceVersion {
		t.Fatalf("expected ADDED at %s, got %v %#v", back.ResourceVersion, ev.Type, ev.Object)
	}
}

// A consumer slower than the producer must get every event, not a silent
// close when the result buffer fills.
func TestNativeWatch_SlowConsumerGetsEveryEvent(t *testing.T) {
	r := nativeTestREST(t)
	const n = 300
	for i := 0; i < n; i++ {
		seedClaim(t, r, fmt.Sprintf("slow-%03d", i))
	}
	ctx, cancel := context.WithTimeout(nsCtx(), 60*time.Second)
	defer cancel()
	w, err := r.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	got := 0
	for got < n {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed after %d of %d events", got, n)
			}
			switch ev.Type {
			case watch.Added:
				got++
			case watch.Error:
				t.Fatalf("watch error after %d of %d events: %#v", got, n, ev.Object)
			}
			time.Sleep(200 * time.Microsecond)
		case <-ctx.Done():
			t.Fatalf("timed out after %d of %d events", got, n)
		}
	}
}

// A watch "from now" is positioned before Watch returns, so a write right
// after the call is delivered.
func TestNativeWatch_FromNowPositionedBeforeReturn(t *testing.T) {
	r := nativeTestREST(t)
	before := seedClaim(t, r, "now-before")
	ctx, cancel := context.WithTimeout(nsCtx(), 20*time.Second)
	defer cancel()
	w, err := r.Watch(ctx, fromNow())
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	nw, ok := w.(*nativeClaimWatch)
	if !ok {
		t.Fatalf("unexpected watch type %T", w)
	}
	beforeRV, _ := strconv.ParseInt(before.ResourceVersion, 10, 64)
	if nw.startRV < beforeRV {
		t.Fatalf("watch from now not positioned when Watch returned: startRV=%d, last write %d", nw.startRV, beforeRV)
	}
	seedClaim(t, r, "now-after")
	if ev := recvEvent(t, w, 10*time.Second); claimName(ev) != "now-after" {
		t.Fatalf("expected now-after, got %v %s", ev.Type, claimName(ev))
	}
}

// A watch from an RV above the committed one never replays rows at or
// below that RV.
func TestNativeWatch_FutureRVNeverMovesBackwards(t *testing.T) {
	r := nativeTestREST(t)
	current, err := r.currentResourceVersion(context.Background())
	if err != nil {
		t.Fatalf("current rv: %v", err)
	}
	start := current + 3
	ctx, cancel := context.WithTimeout(nsCtx(), 20*time.Second)
	defer cancel()
	w, err := r.Watch(ctx, &metainternalversion.ListOptions{ResourceVersion: strconv.FormatInt(start, 10)})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	time.Sleep(300 * time.Millisecond) // let the first drain run
	for i := 0; i < 4; i++ {
		seedClaim(t, r, fmt.Sprintf("fut-%d", i))
	}
	ev := recvEvent(t, w, 10*time.Second)
	rv, _ := strconv.ParseInt(ev.Object.(*quota.QuotaClaim).ResourceVersion, 10, 64)
	if rv <= start {
		t.Fatalf("watch from %d delivered an event at %d", start, rv)
	}
}

// Watches do not hold a database connection each: far more than any
// connection cap can run at once, and all of them see a write.
func TestNativeWatch_ManyConcurrentWatches(t *testing.T) {
	r := nativeTestREST(t)
	const n = 300
	ctx, cancel := context.WithTimeout(nsCtx(), 60*time.Second)
	defer cancel()

	watches := make([]watch.Interface, 0, n)
	defer func() {
		for _, w := range watches {
			w.Stop()
		}
	}()
	for i := 0; i < n; i++ {
		type result struct {
			w   watch.Interface
			err error
		}
		done := make(chan result, 1)
		go func() {
			w, err := r.Watch(ctx, fromNow())
			done <- result{w, err}
		}()
		select {
		case res := <-done:
			if res.err != nil {
				t.Fatalf("Watch %d: %v", i, res.err)
			}
			watches = append(watches, res.w)
		case <-time.After(5 * time.Second):
			t.Fatalf("Watch %d did not return", i)
		}
	}
	seedClaim(t, r, "many")
	var wg sync.WaitGroup
	errs := make(chan string, n)
	for i, w := range watches {
		wg.Add(1)
		go func(i int, w watch.Interface) {
			defer wg.Done()
			timer := time.NewTimer(15 * time.Second)
			defer timer.Stop()
			for {
				select {
				case ev, ok := <-w.ResultChan():
					if !ok {
						errs <- fmt.Sprintf("watch %d closed", i)
						return
					}
					if ev.Type == watch.Added {
						return
					}
				case <-timer.C:
					errs <- fmt.Sprintf("watch %d: no event", i)
					return
				}
			}
		}(i, w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
