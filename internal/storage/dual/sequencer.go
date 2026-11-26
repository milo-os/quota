package dual

import (
	"sync"
	"time"
)

const (
	// sequencerRetention is how long the last-mirrored RV for an idle key is
	// remembered. It must comfortably exceed the time between a primary
	// commit and its shadow write (bounded by ShadowWriteTimeout), which is
	// the only window in which an out-of-order write can arrive.
	sequencerRetention = 5 * time.Minute
	// sequencerSweepEvery triggers an opportunistic prune every N releases so
	// the map stays bounded even when the reconciler is disabled.
	sequencerSweepEvery = 1024
)

// keyState serializes shadow writes for one storage key and remembers the
// newest primary RV that was mirrored for it.
type keyState struct {
	mu      sync.Mutex
	refs    int
	lastRV  uint64
	deleted bool
	touched time.Time
}

// sequencer gives per-key mutual exclusion for shadow writes and drops
// writes that carry an older primary RV than one already mirrored. Without
// it, two concurrent updates to the same key could commit to etcd as
// RV10 then RV11 but reach Postgres as RV11 then RV10, leaving the shadow
// permanently behind.
type sequencer struct {
	mu       sync.Mutex
	keys     map[string]*keyState
	releases int
	now      func() time.Time
}

func newSequencer() *sequencer {
	return &sequencer{keys: map[string]*keyState{}, now: time.Now}
}

// acquire locks key and returns its state. The caller must call release.
func (s *sequencer) acquire(key string) *keyState {
	s.mu.Lock()
	ks, ok := s.keys[key]
	if !ok {
		ks = &keyState{}
		s.keys[key] = ks
	}
	ks.refs++
	s.mu.Unlock()
	ks.mu.Lock()
	return ks
}

func (s *sequencer) release(ks *keyState) {
	ks.touched = s.now()
	ks.mu.Unlock()
	s.mu.Lock()
	ks.refs--
	s.releases++
	sweep := s.releases%sequencerSweepEvery == 0
	s.mu.Unlock()
	if sweep {
		s.prune()
	}
}

// stale reports whether a write of primary RV rv should be dropped because
// a newer (or, for tombstones, equal) state was already mirrored. rv == 0
// means the RV is unknown, and the write is never treated as stale.
func (ks *keyState) stale(rv uint64) bool {
	if rv == 0 || ks.lastRV == 0 {
		return false
	}
	if ks.deleted {
		// The delete tombstone records the RV of the object that was
		// deleted. Any write at or below it is from before the delete.
		return rv <= ks.lastRV
	}
	return rv < ks.lastRV
}

func (ks *keyState) recordWrite(rv uint64) {
	if rv == 0 {
		return
	}
	if rv >= ks.lastRV || ks.deleted {
		ks.lastRV = rv
		ks.deleted = false
	}
}

func (ks *keyState) recordDelete(rv uint64) {
	if rv > ks.lastRV {
		ks.lastRV = rv
	}
	ks.deleted = true
}

// prune drops idle keys that have not been touched within the retention.
func (s *sequencer) prune() {
	cutoff := s.now().Add(-sequencerRetention)
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, ks := range s.keys {
		// refs == 0 means nobody holds or waits on ks.mu, so reading
		// touched without the key lock is safe under s.mu.
		if ks.refs == 0 && ks.touched.Before(cutoff) {
			delete(s.keys, k)
		}
	}
}

func (s *sequencer) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}
