package operation_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/operation"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
)

func TestTerminalOperationCannotRestartAndRepublish(t *testing.T) {
	clock := lease.SystemClock{}
	events := journal.NewMemoryStore()
	service := operation.NewService(operation.Config{WorkerID: "worker-1", LeaseTTL: time.Minute, Clock: clock}, catalog.New(), events, lease.NewManager(clock), policy.NewAdmission(policy.DefaultLimits()))

	first, second := []byte("left"), []byte("right")
	request := operation.Request{
		IdempotencyKey: "terminal-restart",
		Segments:       map[string][]byte{"left": first, "right": second},
		Manifest: segmentmerge.Manifest{
			FileID:    "restart-artifact",
			Version:   "v1",
			TotalSize: 9,
			Segments: []segmentmerge.Segment{
				{ID: "left", Offset: 0, StoredSize: 4, LogicalSize: 4, Digest: segmentmerge.Digest(first), Encoding: segmentmerge.EncodingRaw},
				{ID: "right", Offset: 4, StoredSize: 5, LogicalSize: 5, Digest: segmentmerge.Digest(second), Encoding: segmentmerge.EncodingRaw},
			},
		},
	}
	submitted, created, err := service.Submit(context.Background(), request)
	if err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}
	completed, err := service.Run(context.Background(), submitted.ID)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if completed.State != operation.StateCompleted {
		t.Fatalf("first run state=%s", completed.State)
	}

	if _, err := service.Run(context.Background(), submitted.ID); !errors.Is(err, operation.ErrTerminal) {
		t.Fatalf("second run on terminal operation: expected ErrTerminal, got %v", err)
	}
	replayed, err := service.Replay(context.Background(), submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	completedEvents := 0
	for _, envelope := range replayed {
		if envelope.Event.Kind == journal.KindCompleted {
			completedEvents++
		}
	}
	if completedEvents != 1 {
		t.Fatalf("expected exactly one completed event, got %d", completedEvents)
	}
	artifact, err := service.Artifact(context.Background(), request.Manifest.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Receipt.Version != "v1" {
		t.Fatalf("unexpected artifact version %s", artifact.Receipt.Version)
	}
}
