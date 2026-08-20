package segmentmerge

import (
	"context"
	"sync"
)

type Index interface {
	Put(context.Context, CommitReceipt) error
}

type MemoryIndex struct {
	mu      sync.RWMutex
	records map[string]CommitReceipt
}

func NewMemoryIndex() *MemoryIndex { return &MemoryIndex{records: make(map[string]CommitReceipt)} }
func (index *MemoryIndex) Put(ctx context.Context, receipt CommitReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateReceipt(receipt); err != nil {
		return err
	}
	index.mu.Lock()
	index.records[receipt.FileID] = receipt
	index.mu.Unlock()
	return nil
}
func (index *MemoryIndex) Get(fileID string) (CommitReceipt, bool) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	receipt, ok := index.records[fileID]
	return receipt, ok
}
