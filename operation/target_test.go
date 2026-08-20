package operation_test

import (
	"context"
	"testing"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/operation"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
)

func TestRetryDeadlineSurvivesCatalogRoundTrip(t *testing.T) {
	stored := []byte("not-a-gzip-stream")
	limits := policy.DefaultLimits()
	limits.BaseBackoff = time.Nanosecond
	limits.MaxBackoff = time.Nanosecond
	clock := lease.SystemClock{}
	service := operation.NewService(operation.Config{Clock: clock}, catalog.New(), journal.NewMemoryStore(), lease.NewManager(clock), policy.NewAdmission(limits))
	request := operation.Request{
		IdempotencyKey: "retry-round-trip-v2",
		Segments:      map[string][]byte{"broken-gzip": stored},
		Manifest: segmentmerge.Manifest{
			FileID: "retry-artifact-v2", Version: "v1", TotalSize: 64,
			Segments: []segmentmerge.Segment{{ID: "broken-gzip", Offset: 0, StoredSize: int64(len(stored)), LogicalSize: 64, Digest: segmentmerge.Digest(make([]byte, 64)), Encoding: segmentmerge.EncodingGzip}},
		},
	}
	op, created, err := service.Submit(context.Background(), request)
	if err != nil || !created {
		t.Fatalf("submit created=%v err=%v", created, err)
	}
	result, runErr := service.Run(context.Background(), op.ID)
	if runErr == nil {
		t.Fatal("expected transient decode failure")
	}
	if result.State != operation.StateRetrying || result.RetryAt == nil {
		t.Fatalf("run result state=%s retry_at=%v err=%v", result.State, result.RetryAt, runErr)
	}
	reloaded, err := service.Get(context.Background(), op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.State != operation.StateRetrying || reloaded.RetryAt == nil {
		t.Fatalf("catalog round trip lost retry schedule: state=%s retry_at=%v", reloaded.State, reloaded.RetryAt)
	}
	if err := service.WaitUntilRetry(context.Background(), op.ID); err != nil {
		t.Fatalf("retry dispatch rejected persisted schedule: %v", err)
	}
}
