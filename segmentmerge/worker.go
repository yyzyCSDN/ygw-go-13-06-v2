package segmentmerge

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
)

type Loader struct {
	source        Source
	budget        *ByteBudget
	beforeAcquire func(PlanEntry)
	afterDecode   func(PlanEntry)
}

func NewLoader(source Source, budget *ByteBudget) *Loader {
	return &Loader{source: source, budget: budget}
}

func (loader *Loader) Load(ctx context.Context, entry PlanEntry) (LoadedSegment, error) {
	if loader.beforeAcquire != nil {
		loader.beforeAcquire(entry)
	}
	lease, err := loader.budget.Acquire(ctx, entry.MemoryWeight)
	if err != nil {
		return LoadedSegment{}, err
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			lease.Release()
		}
	}()
	reader, err := loader.source.Open(ctx, entry.Segment.ID)
	if err != nil {
		return LoadedSegment{}, fmt.Errorf("open segment %s: %w", entry.Segment.ID, err)
	}
	stored, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return LoadedSegment{}, fmt.Errorf("read segment %s: %w", entry.Segment.ID, readErr)
	}
	if closeErr != nil {
		return LoadedSegment{}, fmt.Errorf("close segment %s: %w", entry.Segment.ID, closeErr)
	}
	if int64(len(stored)) != entry.Segment.StoredSize {
		return LoadedSegment{}, fmt.Errorf("%w: segment %s stored size=%d want=%d", ErrSegmentIntegrity, entry.Segment.ID, len(stored), entry.Segment.StoredSize)
	}
	logical := stored
	if entry.Segment.Encoding == EncodingGzip {
		gzipReader, err := gzip.NewReader(bytesReader(stored))
		if err != nil {
			return LoadedSegment{}, fmt.Errorf("decode segment %s: %w", entry.Segment.ID, err)
		}
		logical, readErr = io.ReadAll(gzipReader)
		closeErr = gzipReader.Close()
		if readErr != nil {
			return LoadedSegment{}, fmt.Errorf("decode segment %s: %w", entry.Segment.ID, readErr)
		}
		if closeErr != nil {
			return LoadedSegment{}, fmt.Errorf("close decoder %s: %w", entry.Segment.ID, closeErr)
		}
	}
	if err := verifyLogicalSegment(entry.Segment, logical); err != nil {
		return LoadedSegment{}, err
	}
	if err := lease.MarkResident(int64(len(logical))); err != nil {
		return LoadedSegment{}, err
	}
	if loader.afterDecode != nil {
		loader.afterDecode(entry)
	}
	releaseOnError = false
	return LoadedSegment{Entry: entry, Data: append([]byte(nil), logical...), lease: lease}, nil
}

type byteReader struct {
	data   []byte
	offset int
}

func bytesReader(data []byte) *byteReader { return &byteReader{data: data} }
func (reader *byteReader) Read(p []byte) (int, error) {
	if reader.offset >= len(reader.data) {
		return 0, io.EOF
	}
	n := copy(p, reader.data[reader.offset:])
	reader.offset += n
	return n, nil
}
