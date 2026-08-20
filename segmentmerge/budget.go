package segmentmerge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ByteBudget limits logical bytes retained by workers and the service queue.
type ByteBudget struct {
	mu       sync.Mutex
	limit    int64
	reserved int64
	resident int64
	peak     int64
	changed  chan struct{}
}

func NewByteBudget(limit int64) *ByteBudget {
	if limit < 1 {
		limit = 1
	}
	return &ByteBudget{limit: limit, changed: make(chan struct{})}
}

func (budget *ByteBudget) Acquire(ctx context.Context, weight int64) (*BudgetLease, error) {
	if weight < 1 || weight > budget.limit {
		return nil, fmt.Errorf("%w: segment weight %d exceeds budget %d", ErrBudgetExceeded, weight, budget.limit)
	}
	for {
		budget.mu.Lock()
		if budget.reserved <= budget.limit-weight {
			budget.reserved += weight
			budget.mu.Unlock()
			return &BudgetLease{budget: budget, weight: weight}, nil
		}
		changed := budget.changed
		budget.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (budget *ByteBudget) PeakResident() int64 {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return budget.peak
}

func (budget *ByteBudget) Limit() int64 { return budget.limit }

type BudgetLease struct {
	budget   *ByteBudget
	weight   int64
	resident int64
	released atomic.Bool
}

func (lease *BudgetLease) MarkResident(actual int64) error {
	if lease == nil || actual < 0 || actual > lease.weight {
		return fmt.Errorf("%w: decoded bytes %d exceed reservation", ErrBudgetExceeded, actual)
	}
	lease.budget.mu.Lock()
	lease.resident = actual
	lease.budget.resident += actual
	if lease.budget.resident > lease.budget.peak {
		lease.budget.peak = lease.budget.resident
	}
	lease.budget.mu.Unlock()
	return nil
}

func (lease *BudgetLease) Release() bool {
	if lease == nil || !lease.released.CompareAndSwap(false, true) {
		return false
	}
	lease.budget.mu.Lock()
	lease.budget.reserved -= lease.weight
	lease.budget.resident -= lease.resident
	close(lease.budget.changed)
	lease.budget.changed = make(chan struct{})
	lease.budget.mu.Unlock()
	return true
}
