package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"example.com/segmentmerger/segmentmerge"
)

type State string

const (
	StateAccepted  State = "accepted"
	StateRunning   State = "running"
	StateRetrying  State = "retrying"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

var (
	ErrNotFound = errors.New("merge operation not found")
	ErrTerminal = errors.New("merge operation is terminal")
)

type Request struct {
	Manifest       segmentmerge.Manifest `json:"manifest"`
	Segments       map[string][]byte     `json:"segments"`
	IdempotencyKey string                `json:"idempotency_key"`
}

func (r Request) Validate() error {
	if r.IdempotencyKey == "" {
		return fmt.Errorf("idempotency key is required")
	}
	compiled, err := segmentmerge.CompileManifest(r.Manifest)
	if err != nil {
		return err
	}
	for _, segment := range compiled.Segments {
		data, ok := r.Segments[segment.ID]
		if !ok {
			return fmt.Errorf("segment payload %q is missing", segment.ID)
		}
		if int64(len(data)) != segment.StoredSize {
			return fmt.Errorf("segment payload %q stored size mismatch", segment.ID)
		}
	}
	return nil
}

type Progress struct {
	CompletedSegments int       `json:"completed_segments"`
	TotalSegments     int       `json:"total_segments"`
	LastSegmentID     string    `json:"last_segment_id,omitempty"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type Operation struct {
	ID             string                     `json:"id"`
	Partition      uint32                     `json:"partition"`
	IdempotencyKey string                     `json:"idempotency_key"`
	Manifest       segmentmerge.Manifest      `json:"manifest"`
	State          State                      `json:"state"`
	Attempt        int                        `json:"attempt"`
	Fence          uint64                     `json:"fence,omitempty"`
	Owner          string                     `json:"owner,omitempty"`
	Progress       Progress                   `json:"progress"`
	Result         *segmentmerge.MergeResult  `json:"result,omitempty"`
	LastError      string                     `json:"last_error,omitempty"`
	CreatedAt      time.Time                  `json:"created_at"`
	UpdatedAt      time.Time                  `json:"updated_at"`
	RetryAt        *time.Time                 `json:"retry_at,omitempty"`
	Metadata       map[string]json.RawMessage `json:"metadata,omitempty"`
}

func (o Operation) Terminal() bool {
	return o.State == StateCompleted || o.State == StateFailed || o.State == StateCancelled
}

func (o Operation) CanStart() bool {
	return o.State == StateAccepted || o.State == StateRetrying
}

// allowedTransitions enumerates the legal state-machine edges. Terminal states
// (completed, failed, cancelled) intentionally have no outgoing transitions, so
// any attempt to restart a finished operation or republish its terminal outcome
// is rejected by transition.
var allowedTransitions = map[State]map[State]bool{
	StateAccepted: {
		StateRunning:   true,
		StateFailed:    true,
		StateCancelled: true,
	},
	StateRunning: {
		StateCompleted: true,
		StateFailed:    true,
		StateRetrying:  true,
		StateCancelled: true,
	},
	StateRetrying: {
		StateRunning:   true,
		StateFailed:    true,
		StateCancelled: true,
	},
}

func (o Operation) transition(next State, now time.Time) (Operation, error) {
	if o.Terminal() {
		return Operation{}, fmt.Errorf("%w: cannot transition %q -> %q", ErrTerminal, o.State, next)
	}
	if !allowedTransitions[o.State][next] {
		return Operation{}, fmt.Errorf("invalid transition %q -> %q", o.State, next)
	}
	o.State = next
	o.UpdatedAt = now.UTC()
	return o, nil
}

type Snapshot struct {
	Operation Operation `json:"operation"`
	Revision  uint64    `json:"revision"`
	LeaseTTL  string    `json:"lease_ttl,omitempty"`
}
