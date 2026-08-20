package operation

import (
	"bytes"
	"compress/gzip"
	"context"
	"testing"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
)

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func newAdmissionService(t *testing.T, residentLimit int64) *Service {
	t.Helper()
	clock := lease.SystemClock{}
	admission := policy.NewAdmission(policy.Limits{MaxResidentBytes: residentLimit})
	return NewService(Config{WorkerID: "test", LeaseTTL: time.Minute, Clock: clock}, catalog.New(), journal.NewMemoryStore(), lease.NewManager(clock), admission)
}

// A gzip segment whose stored (compressed) size fits the resident budget but whose
// decoded (logical) size does not must be rejected at admission, not admitted and
// then failed with ErrBudgetExceeded on the worker.
func TestSubmitRejectsGzipSegmentWhoseDecodedSizeExceedsBudget(t *testing.T) {
	decoded := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB decoded
	compressed := gzipBytes(t, decoded)
	manifest := segmentmerge.Manifest{FileID: "f", Version: "v1", TotalSize: int64(len(decoded)), Segments: []segmentmerge.Segment{
		{ID: "s1", Offset: 0, StoredSize: int64(len(compressed)), LogicalSize: int64(len(decoded)), Digest: segmentmerge.Digest(decoded), Encoding: segmentmerge.EncodingGzip},
	}}
	service := newAdmissionService(t, 1<<19) // 512 KiB budget, below the 1 MiB decoded size
	_, _, err := service.Submit(context.Background(), Request{IdempotencyKey: "gzip-too-large", Segments: map[string][]byte{"s1": compressed}, Manifest: manifest})
	if err == nil {
		t.Fatal("expected admission to reject gzip segment whose decoded size exceeds the resident budget")
	}
}

// A gzip segment whose decoded size fits the resident budget is admitted and runs to
// completion, confirming the decoded-peak check does not over-reject valid work.
func TestSubmitAndRunCompleteForGzipSegmentWithinBudget(t *testing.T) {
	decoded := []byte("hello gzip world")
	compressed := gzipBytes(t, decoded)
	manifest := segmentmerge.Manifest{FileID: "g", Version: "v1", TotalSize: int64(len(decoded)), Segments: []segmentmerge.Segment{
		{ID: "s1", Offset: 0, StoredSize: int64(len(compressed)), LogicalSize: int64(len(decoded)), Digest: segmentmerge.Digest(decoded), Encoding: segmentmerge.EncodingGzip},
	}}
	service := newAdmissionService(t, 1<<20) // 1 MiB budget, decoded size fits
	op, created, err := service.Submit(context.Background(), Request{IdempotencyKey: "gzip-ok", Segments: map[string][]byte{"s1": compressed}, Manifest: manifest})
	if err != nil {
		t.Fatalf("expected admission, got %v", err)
	}
	if !created {
		t.Fatalf("expected operation to be created, got %+v", op)
	}
	if _, err := service.Run(context.Background(), op.ID); err != nil {
		t.Fatalf("run failed: %v", err)
	}
}
