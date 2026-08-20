package journal_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"example.com/segmentmerger/journal"
)

func TestReplayRejectsEnvelopeMetadataTamperingV3(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.log")
	store, err := journal.OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	event, err := journal.NewEvent("operation-original", journal.KindAccepted, time.Unix(1700000000, 0), map[string]any{"file_id": "artifact-v2", "partition": 7})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var envelope journal.Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Event.OperationID = "operation-substituted"
	tampered, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	tampered = append(tampered, '\n')
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.OpenFileStore(path); !errors.Is(err, journal.ErrCorruptRecord) {
		t.Fatalf("expected metadata checksum rejection, got %v", err)
	}
}
