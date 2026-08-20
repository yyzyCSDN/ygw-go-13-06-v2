package segmentmerge

import (
	"context"
	"testing"
)

func TestMergeRawSegments(t *testing.T) {
	left, right := []byte("left"), []byte("right")
	manifest := Manifest{FileID: "report", Version: "v1", TotalSize: 9, Segments: []Segment{
		{ID: "right", Offset: 4, StoredSize: int64(len(right)), LogicalSize: int64(len(right)), Digest: Digest(right), Encoding: EncodingRaw},
		{ID: "left", Offset: 0, StoredSize: int64(len(left)), LogicalSize: int64(len(left)), Digest: Digest(left), Encoding: EncodingRaw},
	}}
	writer := NewMemoryWriter()
	index := NewMemoryIndex()
	merger := NewMerger(NewMemorySource(map[string][]byte{"left": left, "right": right}), writer, NewMemoryCheckpoints(), index, 1)
	result, err := merger.Merge(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	data, receipt, ok := writer.Output("report")
	if !ok || string(data) != "leftright" || result.Size != int64(len(data)) || receipt.Size != result.Size {
		t.Fatalf("result=%+v receipt=%+v data=%v", result, receipt, data)
	}
	indexed, ok := index.Get("report")
	if !ok || indexed.FileID != receipt.FileID || indexed.Version != receipt.Version {
		t.Fatalf("index=%+v receipt=%+v", indexed, receipt)
	}
}

func TestCompileManifestDoesNotMutateCallerOrder(t *testing.T) {
	a, b := []byte("a"), []byte("b")
	input := Manifest{FileID: "f", Version: "v1", TotalSize: 2, Segments: []Segment{
		{ID: "b", Offset: 1, StoredSize: 1, LogicalSize: 1, Digest: Digest(b), Encoding: EncodingRaw},
		{ID: "a", Offset: 0, StoredSize: 1, LogicalSize: 1, Digest: Digest(a), Encoding: EncodingRaw},
	}}
	compiled, err := CompileManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	if input.Segments[0].ID != "b" || compiled.Segments[0].ID != "a" {
		t.Fatalf("input=%v compiled=%v", input.Segments, compiled.Segments)
	}
}
