package operation

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
)

func newTestService(t *testing.T) (*Service, journal.Store) {
	t.Helper()
	clock := lease.SystemClock{}
	events := journal.NewMemoryStore()
	service := NewService(
		Config{WorkerID: "test-worker", LeaseTTL: time.Minute, Clock: clock},
		catalog.New(),
		events,
		lease.NewManager(clock),
		policy.NewAdmission(policy.DefaultLimits()),
	)
	return service, events
}

func sampleRequest(key string) Request {
	first, second := []byte("hello "), []byte("state-machine")
	return Request{
		IdempotencyKey: key,
		Segments:       map[string][]byte{"first": first, "second": second},
		Manifest: segmentmerge.Manifest{
			FileID: "artifact-" + key, Version: "v1", TotalSize: int64(len(first) + len(second)),
			Segments: []segmentmerge.Segment{
				{ID: "first", Offset: 0, StoredSize: int64(len(first)), LogicalSize: int64(len(first)), Digest: segmentmerge.Digest(first), Encoding: segmentmerge.EncodingRaw},
				{ID: "second", Offset: int64(len(first)), StoredSize: int64(len(second)), LogicalSize: int64(len(second)), Digest: segmentmerge.Digest(second), Encoding: segmentmerge.EncodingRaw},
			},
		},
	}
}

// completedEvents returns the KindCompleted journal envelopes for the operation.
func completedEvents(t *testing.T, store journal.Store, id string) []journal.Envelope {
	t.Helper()
	replay, err := store.Replay(context.Background(), id)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return slices.DeleteFunc(replay.Events, func(e journal.Envelope) bool {
		return e.Event.Kind != journal.KindCompleted
	})
}

// TestRunRejectsRestartOfCompletedOperation guards the restored state-machine
// constraint: once an operation reaches the completed terminal state it must not
// be runnable again, and a second run must not republish a completed journal
// event.
func TestRunRejectsRestartOfCompletedOperation(t *testing.T) {
	ctx := context.Background()
	service, events := newTestService(t)

	op, created, err := service.Submit(ctx, sampleRequest("restart-completed"))
	if err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}
	if _, err := service.Run(ctx, op.ID); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if before := len(completedEvents(t, events, op.ID)); before != 1 {
		t.Fatalf("expected exactly one completed event before restart, got %d", before)
	}

	// Restarting the finished operation must be rejected as terminal.
	_, err = service.Run(ctx, op.ID)
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("expected ErrTerminal on restart of completed operation, got %v", err)
	}

	// State stays completed; no extra completed event is published.
	final, err := service.Get(ctx, op.ID)
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if final.State != StateCompleted {
		t.Fatalf("expected state to remain %q, got %q", StateCompleted, final.State)
	}
	if after := len(completedEvents(t, events, op.ID)); after != 1 {
		t.Fatalf("expected exactly one completed event after restart attempt, got %d", after)
	}
}

// TestRunRejectsRestartOfFailedAndCancelledOperations covers the remaining
// terminal states so none of them can be restarted either.
func TestRunRejectsRestartOfFailedAndCancelledOperations(t *testing.T) {
	ctx := context.Background()
	service, _ := newTestService(t)

	// Failed: persist a failed state through the catalog and confirm Run refuses
	// to restart it. Going through Submit (accepted) keeps the request payload
	// available so Get can materialize the operation during the restart attempt.
	op, created, err := service.Submit(ctx, sampleRequest("restart-failed"))
	if err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}
	record, err := service.catalog.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get record: %v", err)
	}
	failedRecord := toRecord(op)
	failedRecord.State = string(StateFailed)
	if _, err := service.catalog.Update(failedRecord, record.Revision); err != nil {
		t.Fatalf("persist failed state: %v", err)
	}
	if _, err := service.Run(ctx, op.ID); !errors.Is(err, ErrTerminal) {
		t.Fatalf("expected ErrTerminal on restart of failed operation, got %v", err)
	}

	// Cancelled: a fresh operation that is cancelled must also refuse to restart.
	op2, created, err := service.Submit(ctx, sampleRequest("restart-cancelled"))
	if err != nil || !created {
		t.Fatalf("submit op2: created=%v err=%v", created, err)
	}
	if _, err := service.Cancel(ctx, op2.ID); err != nil {
		t.Fatalf("cancel op2: %v", err)
	}
	if _, err := service.Run(ctx, op2.ID); !errors.Is(err, ErrTerminal) {
		t.Fatalf("expected ErrTerminal on restart of cancelled operation, got %v", err)
	}
}
