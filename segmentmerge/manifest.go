package segmentmerge

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// CompileManifest validates a detached, ordered manifest snapshot.
func CompileManifest(input Manifest) (Manifest, error) {
	manifest := input
	manifest.FileID = strings.TrimSpace(manifest.FileID)
	manifest.Version = strings.TrimSpace(manifest.Version)
	manifest.Segments = append([]Segment(nil), input.Segments...)
	if manifest.FileID == "" || manifest.Version == "" || manifest.TotalSize < 0 {
		return Manifest{}, fmt.Errorf("%w: file id, version and non-negative total size are required", ErrInvalidManifest)
	}
	sort.Slice(manifest.Segments, func(i, j int) bool {
		if manifest.Segments[i].Offset != manifest.Segments[j].Offset {
			return manifest.Segments[i].Offset < manifest.Segments[j].Offset
		}
		return manifest.Segments[i].ID < manifest.Segments[j].ID
	})
	seen := make(map[string]struct{}, len(manifest.Segments))
	var previousEnd int64
	for index, segment := range manifest.Segments {
		if segment.ID == "" || segment.Offset < 0 || segment.StoredSize <= 0 || segment.LogicalSize <= 0 {
			return Manifest{}, fmt.Errorf("%w: segment %d has invalid identity or bounds", ErrInvalidManifest, index)
		}
		if _, duplicate := seen[segment.ID]; duplicate {
			return Manifest{}, fmt.Errorf("%w: duplicate segment %q", ErrInvalidManifest, segment.ID)
		}
		seen[segment.ID] = struct{}{}
		if segment.Offset < previousEnd {
			return Manifest{}, fmt.Errorf("%w: segment %q overlaps its predecessor", ErrInvalidManifest, segment.ID)
		}
		if segment.Offset > manifest.TotalSize-segment.LogicalSize {
			return Manifest{}, fmt.Errorf("%w: segment %q exceeds total size", ErrInvalidManifest, segment.ID)
		}
		if segment.Encoding != EncodingRaw && segment.Encoding != EncodingGzip {
			return Manifest{}, fmt.Errorf("%w: segment %q has unsupported encoding", ErrInvalidManifest, segment.ID)
		}
		digest, err := hex.DecodeString(segment.Digest)
		if err != nil || len(digest) != 32 {
			return Manifest{}, fmt.Errorf("%w: segment %q has invalid SHA-256", ErrInvalidManifest, segment.ID)
		}
		previousEnd = segment.Offset + segment.LogicalSize
	}
	return manifest, nil
}
