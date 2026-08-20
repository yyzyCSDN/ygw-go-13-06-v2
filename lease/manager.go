package lease

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrBusy          = errors.New("lease already held")
	ErrNotFound      = errors.New("lease not found")
	ErrOwnerMismatch = errors.New("lease owner mismatch")
	ErrFenceMismatch = errors.New("lease fencing token mismatch")
	ErrExpired       = errors.New("lease expired")
)

// Clock keeps lease expiry deterministic in tests and allows the service to
// share one notion of time across admission, retry and ownership decisions.
type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type Lease struct {
	Resource  string    `json:"resource"`
	Owner     string    `json:"owner"`
	Fence     uint64    `json:"fence"`
	Acquired  time.Time `json:"acquired_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (l Lease) Expired(now time.Time) bool {
	return !now.Before(l.ExpiresAt)
}

func (l Lease) Remaining(now time.Time) time.Duration {
	if l.Expired(now) {
		return 0
	}
	return l.ExpiresAt.Sub(now)
}

type Manager struct {
	mu     sync.RWMutex
	clock  Clock
	next   uint64
	leases map[string]Lease
}

func NewManager(clock Clock) *Manager {
	if clock == nil {
		clock = SystemClock{}
	}
	return &Manager{clock: clock, leases: make(map[string]Lease)}
}

// Acquire creates a new fencing token. An expired holder never reuses its old
// token, which lets downstream commit paths reject delayed workers.
func (m *Manager) Acquire(resource, owner string, ttl time.Duration) (Lease, error) {
	if resource == "" || owner == "" {
		return Lease{}, fmt.Errorf("resource and owner are required")
	}
	if ttl <= 0 {
		return Lease{}, fmt.Errorf("ttl must be positive")
	}
	now := m.clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, ok := m.leases[resource]; ok && !current.Expired(now) {
		return Lease{}, fmt.Errorf("%w: resource=%s owner=%s", ErrBusy, resource, current.Owner)
	}
	m.next++
	created := Lease{Resource: resource, Owner: owner, Fence: m.next, Acquired: now, ExpiresAt: now.Add(ttl)}
	m.leases[resource] = created
	return created, nil
}

func (m *Manager) Renew(resource, owner string, fence uint64, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, fmt.Errorf("ttl must be positive")
	}
	now := m.clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.leases[resource]
	if !ok {
		return Lease{}, ErrNotFound
	}
	if current.Owner != owner {
		return Lease{}, ErrOwnerMismatch
	}
	if current.Fence != fence {
		return Lease{}, ErrFenceMismatch
	}
	if current.Expired(now) {
		delete(m.leases, resource)
		return Lease{}, ErrExpired
	}
	current.ExpiresAt = now.Add(ttl)
	m.leases[resource] = current
	return current, nil
}

func (m *Manager) Release(resource, owner string, fence uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.leases[resource]
	if !ok {
		return nil
	}
	if current.Owner != owner {
		return ErrOwnerMismatch
	}
	if current.Fence != fence {
		return ErrFenceMismatch
	}
	delete(m.leases, resource)
	return nil
}

// Verify reports whether owner/fence still holds an unexpired lease on resource.
// It is the publish-boundary check: a worker whose lease has expired or been
// superseded by a newer fencing token must not publish completed operations,
// artifacts or completion journal events. Unlike Renew, Verify is read-only and
// never sweeps the matched entry, so a concurrent holder observes no mutation.
func (m *Manager) Verify(resource, owner string, fence uint64) (Lease, error) {
	now := m.clock.Now()
	m.mu.RLock()
	defer m.mu.RUnlock()
	current, ok := m.leases[resource]
	if !ok {
		return Lease{}, ErrNotFound
	}
	if current.Owner != owner {
		return Lease{}, ErrOwnerMismatch
	}
	if current.Fence != fence {
		return Lease{}, ErrFenceMismatch
	}
	if current.Expired(now) {
		return Lease{}, ErrExpired
	}
	return current, nil
}

func (m *Manager) Get(resource string) (Lease, bool) {
	now := m.clock.Now()
	m.mu.RLock()
	current, ok := m.leases[resource]
	m.mu.RUnlock()
	if !ok || current.Expired(now) {
		return Lease{}, false
	}
	return current, true
}

func (m *Manager) SweepExpired() []Lease {
	now := m.clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	removed := make([]Lease, 0)
	for resource, current := range m.leases {
		if current.Expired(now) {
			removed = append(removed, current)
			delete(m.leases, resource)
		}
	}
	return removed
}

func (m *Manager) Snapshot() []Lease {
	now := m.clock.Now()
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Lease, 0, len(m.leases))
	for _, current := range m.leases {
		if !current.Expired(now) {
			result = append(result, current)
		}
	}
	return result
}
