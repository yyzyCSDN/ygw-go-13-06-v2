package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestEnvelopeChecksumCoversCompleteEventIdentity verifies that replay rejects
// a record whose metadata has been tampered with. The checksum must cover the
// whole event identity — most importantly the operation id, which a naive
// lifecycle-only checksum would leave unchecked.
func TestEnvelopeChecksumCoversCompleteEventIdentity(t *testing.T) {
	at := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	base, err := NewEvent("merge-01-0000000000000001", KindAccepted, at, map[string]any{"file_id": "f1", "partition": 3})
	if err != nil {
		t.Fatal(err)
	}
	envelope, _, err := encodeEnvelope(1, base)
	if err != nil {
		t.Fatal(err)
	}

	// A pristine record must still verify.
	if err := verifyEnvelope(envelope); err != nil {
		t.Fatalf("pristine record rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Event)
	}{
		{"operation id", func(e *Event) { e.OperationID = "merge-99-9999999999999999" }},
		{"kind", func(e *Event) { e.Kind = KindCompleted }},
		{"at", func(e *Event) { e.At = at.Add(time.Hour) }},
		{"attempt", func(e *Event) { e.Attempt = 7 }},
		{"fence", func(e *Event) { e.Fence = 42 }},
		{"payload", func(e *Event) { e.Payload = json.RawMessage(`{"file_id":"evil"}`) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tampered := envelope
			tc.mutate(&tampered.Event)
			encoded, err := json.Marshal(tampered)
			if err != nil {
				t.Fatal(err)
			}
			_, err = decodeStream(context.Background(), bytes.NewReader(append(encoded, '\n')), "")
			if err == nil {
				t.Fatalf("replay accepted a record with a replaced %s", tc.name)
			}
			if !strings.Contains(err.Error(), "checksum mismatch") {
				t.Fatalf("expected checksum mismatch for %s tampering, got %v", tc.name, err)
			}
		})
	}
}
