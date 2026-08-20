package operation_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"testing"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/operation"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
)

func TestCompressedManifestRejectedWhenDecodedPeakExceedsBudget(t *testing.T) {
	logical := bytes.Repeat([]byte("A"), 128)
	var stored bytes.Buffer
	encoder := gzip.NewWriter(&stored)
	if _, err := encoder.Write(logical); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	limits := policy.DefaultLimits()
	limits.MaxResidentBytes = 64
	clock := lease.SystemClock{}
	service := operation.NewService(operation.Config{Clock: clock}, catalog.New(), journal.NewMemoryStore(), lease.NewManager(clock), policy.NewAdmission(limits))
	request := operation.Request{
		IdempotencyKey: "compressed-budget-v2",
		Segments:      map[string][]byte{"gzip-segment": stored.Bytes()},
		Manifest: segmentmerge.Manifest{
			FileID: "compressed-artifact-v2", Version: "v1", TotalSize: int64(len(logical)),
			Segments: []segmentmerge.Segment{{ID: "gzip-segment", Offset: 0, StoredSize: int64(stored.Len()), LogicalSize: int64(len(logical)), Digest: segmentmerge.Digest(logical), Encoding: segmentmerge.EncodingGzip}},
		},
	}
	_, created, err := service.Submit(context.Background(), request)
	if !errors.Is(err, policy.ErrRejected) {
		t.Fatalf("expected decoded peak admission rejection, got created=%v err=%v", created, err)
	}
	if created {
		t.Fatal("rejected compressed manifest created an operation")
	}
}
