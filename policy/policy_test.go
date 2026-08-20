package policy

import (
	"strings"
	"testing"

	"example.com/segmentmerger/segmentmerge"
)

func TestAdmissionRejectsGzipSegmentByDecodedPeak(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxResidentBytes = 1 << 20 // 1 MiB resident budget
	admission := NewAdmission(limits)
	// A gzip segment whose stored (compressed) size fits the budget but whose
	// decoded (logical) size does not. Admission must reject on the decoded peak,
	// matching the weight merger workers acquire from the byte budget.
	manifest := segmentmerge.Manifest{FileID: "f", Version: "v1", TotalSize: 2 << 20, Segments: []segmentmerge.Segment{
		{ID: "s1", Offset: 0, StoredSize: 1 << 10, LogicalSize: 2 << 20, Digest: strings.Repeat("00", 32), Encoding: segmentmerge.EncodingGzip},
	}}
	decision := admission.Evaluate(manifest, Load{})
	if decision.Allowed {
		t.Fatalf("expected rejection: decoded size %d exceeds resident budget %d; got %+v", 2<<20, limits.MaxResidentBytes, decision)
	}
	if !strings.Contains(decision.Reason, "decoded") {
		t.Fatalf("expected rejection reason to mention decoded segment, got %q", decision.Reason)
	}
}

func TestAdmissionEstimatesWeightByDecodedPeak(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxResidentBytes = 16 << 20 // 16 MiB
	admission := NewAdmission(limits)
	manifest := segmentmerge.Manifest{FileID: "f", Version: "v1", TotalSize: 6, Segments: []segmentmerge.Segment{
		{ID: "a", Offset: 0, StoredSize: 2, LogicalSize: 2, Digest: strings.Repeat("00", 32), Encoding: segmentmerge.EncodingRaw},
		{ID: "b", Offset: 2, StoredSize: 1, LogicalSize: 4, Digest: strings.Repeat("00", 32), Encoding: segmentmerge.EncodingGzip},
	}}
	decision := admission.Evaluate(manifest, Load{})
	if !decision.Allowed {
		t.Fatalf("expected admission, got %+v", decision)
	}
	if decision.EstimatedWeight != 4 {
		t.Fatalf("expected estimated weight to be the decoded peak 4, got %d", decision.EstimatedWeight)
	}
}
