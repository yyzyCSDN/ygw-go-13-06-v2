package lease

import (
	"errors"
	"testing"
	"time"
)

// manualClock lets tests advance time deterministically without sleeping.
type manualClock struct{ now time.Time }

func newManualClock() *manualClock { return &manualClock{now: time.Unix(1_700_000_000, 0).UTC()} }

func (c *manualClock) Now() time.Time { return c.now }

func (c *manualClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func TestVerifyAcceptsCurrentHolder(t *testing.T) {
	clock := newManualClock()
	manager := NewManager(clock)
	lease, err := manager.Acquire("merge/file", "worker-a", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := manager.Verify(lease.Resource, lease.Owner, lease.Fence); err != nil {
		t.Fatalf("verify current holder: %v", err)
	}
}

func TestVerifyRejectsExpiredLease(t *testing.T) {
	clock := newManualClock()
	manager := NewManager(clock)
	lease, err := manager.Acquire("merge/file", "worker-a", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	clock.Advance(2 * time.Second)
	_, err = manager.Verify(lease.Resource, lease.Owner, lease.Fence)
	if !errors.Is(err, ErrExpired) {
		t.Fatalf("verify expired lease: got %v, want %v", err, ErrExpired)
	}
}

func TestVerifyRejectsSupersededFenceSameOwner(t *testing.T) {
	clock := newManualClock()
	manager := NewManager(clock)
	stale, err := manager.Acquire("merge/file", "worker-a", time.Second)
	if err != nil {
		t.Fatalf("acquire stale: %v", err)
	}
	clock.Advance(2 * time.Second) // stale lease expires
	fresh, err := manager.Acquire("merge/file", "worker-a", time.Second)
	if err != nil {
		t.Fatalf("acquire fresh: %v", err)
	}
	if fresh.Fence == stale.Fence {
		t.Fatalf("expected newer fence: stale=%d fresh=%d", stale.Fence, fresh.Fence)
	}
	_, err = manager.Verify(stale.Resource, stale.Owner, stale.Fence)
	if !errors.Is(err, ErrFenceMismatch) {
		t.Fatalf("verify superseded fence: got %v, want %v", err, ErrFenceMismatch)
	}
	// The fresh holder still verifies, even though the stale worker is blocked.
	if _, err := manager.Verify(fresh.Resource, fresh.Owner, fresh.Fence); err != nil {
		t.Fatalf("verify fresh holder: %v", err)
	}
}

func TestVerifyRejectsSupersededFenceDifferentOwner(t *testing.T) {
	clock := newManualClock()
	manager := NewManager(clock)
	stale, err := manager.Acquire("merge/file", "worker-a", time.Second)
	if err != nil {
		t.Fatalf("acquire stale: %v", err)
	}
	clock.Advance(2 * time.Second) // stale lease expires
	if _, err := manager.Acquire("merge/file", "worker-b", time.Second); err != nil {
		t.Fatalf("acquire fresh: %v", err)
	}
	_, err = manager.Verify(stale.Resource, stale.Owner, stale.Fence)
	if !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("verify superseded owner: got %v, want %v", err, ErrOwnerMismatch)
	}
}

func TestVerifyRejectsMissingLease(t *testing.T) {
	clock := newManualClock()
	manager := NewManager(clock)
	_, err := manager.Verify("merge/absent", "worker-a", 1)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("verify missing lease: got %v, want %v", err, ErrNotFound)
	}
}

func TestVerifyDoesNotMutateLeaseMap(t *testing.T) {
	clock := newManualClock()
	manager := NewManager(clock)
	lease, err := manager.Acquire("merge/file", "worker-a", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	clock.Advance(2 * time.Second) // lease now expired but unswept
	if _, err := manager.Verify(lease.Resource, lease.Owner, lease.Fence); !errors.Is(err, ErrExpired) {
		t.Fatalf("verify expired: got %v, want %v", err, ErrExpired)
	}
	// Verify is read-only: the expired entry remains, so a renewal that re-checks
	// expiry still observes the expired state rather than a missing lease.
	if _, err := manager.Renew(lease.Resource, lease.Owner, lease.Fence, time.Second); !errors.Is(err, ErrExpired) {
		t.Fatalf("renew after verify: got %v, want %v", err, ErrExpired)
	}
}
