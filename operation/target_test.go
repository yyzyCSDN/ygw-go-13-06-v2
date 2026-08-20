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

type fenceClock struct{ now time.Time }

func (clock *fenceClock) Now() time.Time { return clock.now }

type blockingJournal struct {
	journal.Store
	entered chan struct{}
	release chan struct{}
}

func (store *blockingJournal) Append(ctx context.Context, event journal.Event) (journal.Envelope, error) {
	if event.Kind == journal.KindStarted {
		select {
		case store.entered <- struct{}{}:
		default:
		}
		<-store.release
	}
	return store.Store.Append(ctx, event)
}

func TestExpiredWorkerCannotPublishAfterFenceReplacement(t *testing.T) {
	clock := &fenceClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	events := &blockingJournal{Store: journal.NewMemoryStore(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	records := catalog.New()
	leases := lease.NewManager(clock)
	service := operation.NewService(operation.Config{
		WorkerID:       "worker-1",
		PartitionCount: 8,
		LeaseTTL:       30 * time.Second,
		Clock:          clock,
	}, records, events, leases, policy.NewAdmission(policy.DefaultLimits()))

	first, second := []byte("a"), []byte("b")
	request := operation.Request{
		IdempotencyKey: "fence-case",
		Segments:       map[string][]byte{"a": first, "b": second},
		Manifest: segmentmerge.Manifest{
			FileID:    "fence-artifact",
			Version:   "v1",
			TotalSize: 2,
			Segments: []segmentmerge.Segment{
				{ID: "a", Offset: 0, StoredSize: 1, LogicalSize: 1, Digest: segmentmerge.Digest(first), Encoding: segmentmerge.EncodingRaw},
				{ID: "b", Offset: 1, StoredSize: 1, LogicalSize: 1, Digest: segmentmerge.Digest(second), Encoding: segmentmerge.EncodingRaw},
			},
		},
	}
	submitted, created, err := service.Submit(context.Background(), request)
	if err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}

	runDone := make(chan error, 1)
	go func() {
		_, err := service.Run(context.Background(), submitted.ID)
		runDone <- err
	}()
	<-events.entered

	clock.now = clock.now.Add(31 * time.Second)
	replacement, err := leases.Acquire("merge/"+request.Manifest.FileID, "worker-2", 30*time.Second)
	if err != nil {
		t.Fatalf("replacement acquisition: %v", err)
	}
	if replacement.Fence <= submitted.Fence {
		t.Fatalf("replacement fence did not advance: old=%d new=%d", submitted.Fence, replacement.Fence)
	}
	close(events.release)

	if runErr := <-runDone; runErr == nil {
		t.Fatalf("stale worker published terminal outcome after losing its fence")
	}
	after, err := service.Get(context.Background(), submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State == operation.StateCompleted {
		t.Fatalf("operation completed despite fence replacement: %+v", after)
	}
	if _, err := service.Artifact(context.Background(), request.Manifest.FileID); !errors.Is(err, catalog.ErrArtifactNotFound) {
		t.Fatalf("artifact published by stale worker: %v", err)
	}
	replayed, err := service.Replay(context.Background(), submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, envelope := range replayed {
		if envelope.Event.Kind == journal.KindCompleted {
			t.Fatalf("completed journal event published by stale worker")
		}
	}
}
