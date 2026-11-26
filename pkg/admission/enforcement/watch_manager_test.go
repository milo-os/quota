package enforcement

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	k8stesting "k8s.io/client-go/testing"
)

func TestPredatesWaiter(t *testing.T) {
	start := time.Date(2025, 1, 1, 12, 0, 10, 500_000_000, time.UTC)
	waiter := &claimWaiter{startTime: start}
	obj := func(ts time.Time) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetCreationTimestamp(metav1.NewTime(ts))
		return u
	}
	if !predatesWaiter(obj(start.Add(-2*time.Second)), waiter) {
		t.Error("claim from an earlier attempt should be ignored")
	}
	if predatesWaiter(obj(start.Truncate(time.Second)), waiter) {
		t.Error("claim created in the registration second must not be ignored")
	}
	if predatesWaiter(&unstructured.Unstructured{}, waiter) {
		t.Error("claim without creationTimestamp must not be ignored")
	}
}

// TestStaleDeletionDoesNotFailWaiter verifies that the DELETED event of a
// stale claim (removed by the plugin just before registering) is ignored,
// and that a late event after the waiter is gone does not panic.
func TestStaleDeletionDoesNotFailWaiter(t *testing.T) {
	w := newWatchManagerWithConfig(nil, logr.Discard(), "", nil)
	w.started.Store(true)

	ch, cancel, err := w.RegisterClaimWaiter(context.Background(), "c1", "ns", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	stale := claimObj("c1", "ns", true, "True", "QuotaAvailable")
	stale.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-time.Minute)))
	w.handleClaimDeletion(stale)
	w.handleClaimEvent(stale)
	select {
	case r := <-ch:
		t.Fatalf("stale claim event delivered: %+v", r)
	default:
	}

	fresh := claimObj("c1", "ns", true, "True", "QuotaAvailable")
	fresh.SetCreationTimestamp(metav1.NewTime(time.Now()))
	w.handleClaimEvent(fresh)
	r, ok := <-ch
	if !ok || !r.Granted {
		t.Fatalf("expected granted result, got %+v ok=%v", r, ok)
	}

	// Waiter is unregistered now; a late event must be a no-op.
	w.handleClaimEvent(fresh)
	w.handleClaimDeletion(fresh)
	w.deliver(types.NamespacedName{Namespace: "ns", Name: "c1"}, &claimWaiter{}, ClaimResult{})
}

// TestWatchResumeAndRelist verifies the manager reads the current
// resource version with a list and watches from it (never RV=""), resumes
// from the last event after a dropped stream, and relists after 410 Gone,
// re-reading waiters' claims so a decision made in the gap still arrives.
func TestWatchResumeAndRelist(t *testing.T) {
	client := newFakeDynamic()
	var watchRVs []string
	var lists int
	streams := make(chan *watch.FakeWatcher, 4)
	client.PrependReactor("list", "quotaclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		lists++
		l := &unstructured.UnstructuredList{}
		l.SetResourceVersion(fmt.Sprintf("%d", 100*lists))
		return true, l, nil
	})
	client.PrependWatchReactor("quotaclaims", func(action k8stesting.Action) (bool, watch.Interface, error) {
		watchRVs = append(watchRVs, action.(k8stesting.WatchActionImpl).GetWatchRestrictions().ResourceVersion)
		fw := watch.NewFakeWithChanSize(10, false)
		streams <- fw
		return true, fw, nil
	})

	cfg := DefaultWatchManagerConfig()
	cfg.Retry.InitialDelay = time.Millisecond
	cfg.Retry.Jitter = 0
	w := newWatchManagerWithConfig(client, logr.Discard(), "", cfg)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Stream 1: an event at RV 150, then the stream drops.
	s1 := <-streams
	ev := claimObj("other", "ns", true, "", "")
	ev.SetResourceVersion("150")
	s1.Add(ev)
	s1.Stop()

	// Stream 2 resumes from 150, then the server answers 410.
	s2 := <-streams
	ch, cancel, err := w.RegisterClaimWaiter(context.Background(), "c1", "ns", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	granted := claimObj("c1", "ns", true, "True", "QuotaAvailable")
	granted.SetCreationTimestamp(metav1.NewTime(time.Now()))
	if _, err := client.Resource(quotaClaimGVR).Namespace("ns").Create(context.Background(), granted, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	s2.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired})

	select {
	case r := <-ch:
		if !r.Granted {
			t.Fatalf("expected grant from the relist, got %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("decision made while the stream was gone never reached the waiter")
	}
	<-streams

	if lists != 2 {
		t.Fatalf("expected a list at start and after 410, got %d", lists)
	}
	if want := []string{"100", "150", "200"}; fmt.Sprint(watchRVs) != fmt.Sprint(want) {
		t.Fatalf("watch resourceVersions = %v, want %v", watchRVs, want)
	}
}
