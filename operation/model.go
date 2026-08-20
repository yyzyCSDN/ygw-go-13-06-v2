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
	ErrNotFound          = errors.New("merge operation not found")
	ErrTerminal          = errors.New("merge operation is terminal")
	ErrInvalidTransition = errors.New("invalid merge operation transition")
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

func (o Operation) transition(next State, now time.Time) (Operation, error) {
	allowed := false
	switch o.State {
	case StateAccepted:
		allowed = next == StateRunning || next == StateCancelled || next == StateFailed
	case StateRunning:
		allowed = next == StateCompleted || next == StateRetrying || next == StateFailed || next == StateCancelled
	case StateRetrying:
		allowed = next == StateRunning || next == StateCancelled || next == StateFailed
	}
	if !allowed {
		return Operation{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, o.State, next)
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
