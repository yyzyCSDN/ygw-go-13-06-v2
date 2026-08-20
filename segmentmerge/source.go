package segmentmerge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

type Source interface {
	Open(context.Context, string) (io.ReadCloser, error)
}

type MemorySource struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemorySource(values map[string][]byte) *MemorySource {
	copyValues := make(map[string][]byte, len(values))
	for id, value := range values {
		copyValues[id] = append([]byte(nil), value...)
	}
	return &MemorySource{data: copyValues}
}

func (source *MemorySource) Open(ctx context.Context, id string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	source.mu.RLock()
	data, ok := source.data[id]
	source.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSegmentNotFound, id)
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), nil
}
