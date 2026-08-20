package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrCorruptRecord = errors.New("corrupt journal record")
	ErrSequenceGap   = errors.New("journal sequence gap")
)

type Kind string

const (
	KindAccepted  Kind = "accepted"
	KindStarted   Kind = "started"
	KindProgress  Kind = "progress"
	KindRetrying  Kind = "retrying"
	KindCompleted Kind = "completed"
	KindFailed    Kind = "failed"
	KindCancelled Kind = "cancelled"
)

type Event struct {
	OperationID string          `json:"operation_id"`
	Kind        Kind            `json:"kind"`
	At          time.Time       `json:"at"`
	Attempt     int             `json:"attempt,omitempty"`
	Fence       uint64          `json:"fence,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

func NewEvent(operationID string, kind Kind, at time.Time, payload any) (Event, error) {
	if operationID == "" {
		return Event{}, fmt.Errorf("operation id is required")
	}
	if !validKind(kind) {
		return Event{}, fmt.Errorf("unsupported event kind %q", kind)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	if string(encoded) == "null" {
		encoded = nil
	}
	return Event{OperationID: operationID, Kind: kind, At: at.UTC(), Payload: encoded}, nil
}

func validKind(kind Kind) bool {
	switch kind {
	case KindAccepted, KindStarted, KindProgress, KindRetrying, KindCompleted, KindFailed, KindCancelled:
		return true
	default:
		return false
	}
}

type Envelope struct {
	Sequence uint64 `json:"sequence"`
	Checksum string `json:"checksum"`
	Event    Event  `json:"event"`
}

type Replay struct {
	Events       []Envelope `json:"events"`
	LastSequence uint64     `json:"last_sequence"`
	Truncated    bool       `json:"truncated"`
}

// checksumBytes returns the canonical bytes covered by the envelope checksum.
// The complete event identity is hashed — operation id, kind, timing, attempt,
// fence, and payload — so any tampering with a record's metadata (including a
// swapped operation id) is rejected when the journal is replayed.
func checksumBytes(event Event) ([]byte, error) {
	return json.Marshal(event)
}
