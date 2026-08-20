package journal

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

type Store interface {
	Append(context.Context, Event) (Envelope, error)
	Replay(context.Context, string) (Replay, error)
	All(context.Context) (Replay, error)
}

// MemoryStore preserves the encoded line format used by FileStore. Tests can
// therefore exercise checksum and replay behavior without filesystem timing.
type MemoryStore struct {
	mu    sync.RWMutex
	lines [][]byte
	next  uint64
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Append(ctx context.Context, event Event) (Envelope, error) {
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	if event.OperationID == "" || !validKind(event.Kind) {
		return Envelope{}, fmt.Errorf("invalid journal event")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	envelope, encoded, err := encodeEnvelope(s.next, event)
	if err != nil {
		s.next--
		return Envelope{}, err
	}
	s.lines = append(s.lines, encoded)
	return envelope, nil
}

func (s *MemoryStore) Replay(ctx context.Context, operationID string) (Replay, error) {
	if operationID == "" {
		return Replay{}, fmt.Errorf("operation id is required")
	}
	s.mu.RLock()
	data := joinLines(s.lines)
	s.mu.RUnlock()
	return decodeStream(ctx, bytes.NewReader(data), operationID)
}

func (s *MemoryStore) All(ctx context.Context) (Replay, error) {
	s.mu.RLock()
	data := joinLines(s.lines)
	s.mu.RUnlock()
	return decodeStream(ctx, bytes.NewReader(data), "")
}

func joinLines(lines [][]byte) []byte {
	var data bytes.Buffer
	for _, line := range lines {
		data.Write(line)
		data.WriteByte('\n')
	}
	return data.Bytes()
}

func encodeEnvelope(sequence uint64, event Event) (Envelope, []byte, error) {
	eventBytes, err := checksumBytes(event)
	if err != nil {
		return Envelope{}, nil, err
	}
	sum := sha256.Sum256(eventBytes)
	envelope := Envelope{Sequence: sequence, Checksum: hex.EncodeToString(sum[:]), Event: event}
	encoded, err := json.Marshal(envelope)
	return envelope, encoded, err
}

func decodeStream(ctx context.Context, reader io.Reader, operationID string) (Replay, error) {
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	result := Replay{}
	var expected uint64 = 1
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return Replay{}, err
		}
		line := append([]byte(nil), scanner.Bytes()...)
		var envelope Envelope
		if err := json.Unmarshal(line, &envelope); err != nil {
			return Replay{}, fmt.Errorf("%w: decode sequence %d: %v", ErrCorruptRecord, expected, err)
		}
		if envelope.Sequence != expected {
			return Replay{}, fmt.Errorf("%w: expected %d got %d", ErrSequenceGap, expected, envelope.Sequence)
		}
		if err := verifyEnvelope(envelope); err != nil {
			return Replay{}, err
		}
		result.LastSequence = envelope.Sequence
		if operationID == "" || envelope.Event.OperationID == operationID {
			result.Events = append(result.Events, envelope)
		}
		expected++
	}
	if err := scanner.Err(); err != nil {
		return Replay{}, err
	}
	return result, nil
}

func verifyEnvelope(envelope Envelope) error {
	encoded, err := checksumBytes(envelope.Event)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(encoded)
	actual := hex.EncodeToString(sum[:])
	if envelope.Checksum != actual {
		return fmt.Errorf("%w: sequence %d checksum mismatch", ErrCorruptRecord, envelope.Sequence)
	}
	if envelope.Event.OperationID == "" || !validKind(envelope.Event.Kind) {
		return fmt.Errorf("%w: sequence %d invalid event", ErrCorruptRecord, envelope.Sequence)
	}
	return nil
}
