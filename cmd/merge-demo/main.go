package main

import (
	"context"
	"fmt"

	"example.com/segmentmerger/segmentmerge"
)

func main() {
	first, second := []byte("hello "), []byte("world")
	manifest := segmentmerge.Manifest{FileID: "demo", Version: "v1", TotalSize: int64(len(first) + len(second)), Segments: []segmentmerge.Segment{
		{ID: "part-1", Offset: 0, StoredSize: int64(len(first)), LogicalSize: int64(len(first)), Digest: segmentmerge.Digest(first), Encoding: segmentmerge.EncodingRaw},
		{ID: "part-2", Offset: int64(len(first)), StoredSize: int64(len(second)), LogicalSize: int64(len(second)), Digest: segmentmerge.Digest(second), Encoding: segmentmerge.EncodingRaw},
	}}
	writer := segmentmerge.NewMemoryWriter()
	merger := segmentmerge.NewMerger(segmentmerge.NewMemorySource(map[string][]byte{"part-1": first, "part-2": second}), writer, segmentmerge.NewMemoryCheckpoints(), segmentmerge.NewMemoryIndex(), 2)
	result, err := merger.Merge(context.Background(), manifest)
	data, _, _ := writer.Output("demo")
	fmt.Printf("file=%s size=%d data=%q err=%v\n", result.FileID, result.Size, data, err)
}
