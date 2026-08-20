package operation_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/operation"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
)

// fixedClock never advances, so a lease acquired with a positive TTL stays
// valid for the duration of a synchronous Run. It keeps the happy path
// deterministic without depending on wall-clock speed.
type fixedClock struct{ now time.Time }

func newFixedClock() *fixedClock { return &fixedClock{now: time.Unix(1_700_000_000, 0).UTC()} }

func (c *fixedClock) Now() time.Time { return c.now }

// steppingClock advances time by a fixed step on every Now() call. A lease
// acquired with a TTL smaller than the step is already expired by the time the
// worker reaches the publish boundary, reproducing an expired worker that
// finishes its merge after another holder could have taken over.
type steppingClock struct {
	start time.Time
	step  time.Duration
	next  atomic.Int64
}

func newSteppingClock(step time.Duration) *steppingClock {
	return &steppingClock{start: time.Unix(1_700_000_000, 0).UTC(), step: step}
}

func (c *steppingClock) Now() time.Time {
	n := c.next.Add(1)
	return c.start.Add(time.Duration(n) * c.step)
}

func twoSegmentRequest(key, fileID string) operation.Request {
	first, second := []byte("hello "), []byte("boundary")
	return operation.Request{
		IdempotencyKey: key,
		Segments:      map[string][]byte{"first": first, "second": second},
		Manifest: segmentmerge.Manifest{
			FileID: fileID, Version: "v1", TotalSize: int64(len(first) + len(second)),
			Segments: []segmentmerge.Segment{
				{ID: "first", Offset: 0, StoredSize: int64(len(first)), LogicalSize: int64(len(first)), Digest: segmentmerge.Digest(first), Encoding: segmentmerge.EncodingRaw},
				{ID: "second", Offset: int64(len(first)), StoredSize: int64(len(second)), LogicalSize: int64(len(second)), Digest: segmentmerge.Digest(second), Encoding: segmentmerge.EncodingRaw},
			},
		},
	}
}

func TestRunCompletesWhenLeaseHeld(t *testing.T) {
	clock := newFixedClock()
	service := operation.NewService(operation.Config{WorkerID: "worker-a", LeaseTTL: time.Second, Clock: clock}, catalog.New(), journal.NewMemoryStore(), lease.NewManager(clock), policy.NewAdmission(policy.DefaultLimits()))
	ctx := context.Background()

	request := twoSegmentRequest("happy-1", "artifact-happy")
	op, created, err := service.Submit(ctx, request)
	if err != nil || !created {
		t.Fatalf("submit: err=%v created=%v", err, created)
	}
	completed, err := service.Run(ctx, op.ID)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if completed.State != operation.StateCompleted || completed.Progress.CompletedSegments != 2 {
		t.Fatalf("unexpected completed op: %+v", completed)
	}
	artifact, err := service.Artifact(ctx, request.Manifest.FileID)
	if err != nil || artifact.OperationID != op.ID {
		t.Fatalf("artifact: err=%v record=%+v", err, artifact)
	}
	replay, err := service.Replay(ctx, op.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !hasKind(replay, journal.KindCompleted) {
		t.Fatalf("missing completed journal event: %+v", replay)
	}
}

func TestRunPublishBoundaryBlocksExpiredLease(t *testing.T) {
	// Each clock.Now() call advances by 1s; the 1ms lease is therefore expired
	// by the time the worker reaches the publish boundary, even though the
	// in-memory merge itself succeeds.
	clock := newSteppingClock(time.Second)
	service := operation.NewService(operation.Config{WorkerID: "stale-worker", LeaseTTL: time.Millisecond, Clock: clock}, catalog.New(), journal.NewMemoryStore(), lease.NewManager(clock), policy.NewAdmission(policy.DefaultLimits()))
	ctx := context.Background()

	request := twoSegmentRequest("stale-1", "artifact-stale")
	op, created, err := service.Submit(ctx, request)
	if err != nil || !created {
		t.Fatalf("submit: err=%v created=%v", err, created)
	}
	if _, err := service.Run(ctx, op.ID); !errors.Is(err, lease.ErrExpired) {
		t.Fatalf("run: err=%v, want %v", err, lease.ErrExpired)
	}

	// The operation must not have transitioned to completed.
	stale, err := service.Get(ctx, op.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stale.State == operation.StateCompleted {
		t.Fatalf("expired worker published completed operation: %+v", stale)
	}
	if stale.State != operation.StateRetrying {
		t.Fatalf("unexpected state: got %s, want %s", stale.State, operation.StateRetrying)
	}

	// No artifact may exist for the stale worker's result.
	if _, err := service.Artifact(ctx, request.Manifest.FileID); !errors.Is(err, catalog.ErrArtifactNotFound) {
		t.Fatalf("artifact: err=%v, want %v", err, catalog.ErrArtifactNotFound)
	}

	// No completed journal event may have been appended.
	replay, err := service.Replay(ctx, op.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if hasKind(replay, journal.KindCompleted) {
		t.Fatalf("expired worker published completed journal event: %+v", replay)
	}
	if !hasKind(replay, journal.KindRetrying) {
		t.Fatalf("missing retrying journal event: %+v", replay)
	}
}

func hasKind(replay []journal.Envelope, kind journal.Kind) bool {
	for _, envelope := range replay {
		if envelope.Event.Kind == kind {
			return true
		}
	}
	return false
}
