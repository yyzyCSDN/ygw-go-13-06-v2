package segmentmerge

import (
	"context"
	"sync"
)

type CheckpointStore interface {
	Load(context.Context, string) (Checkpoint, bool, error)
	Save(context.Context, Checkpoint) error
	Clear(context.Context, string) error
}

type MemoryCheckpoints struct {
	mu    sync.RWMutex
	items map[string]Checkpoint
}

func NewMemoryCheckpoints() *MemoryCheckpoints {
	return &MemoryCheckpoints{items: make(map[string]Checkpoint)}
}

func (store *MemoryCheckpoints) Load(ctx context.Context, fileID string) (Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	checkpoint, ok := store.items[fileID]
	return checkpoint, ok, nil
}

func (store *MemoryCheckpoints) Save(ctx context.Context, checkpoint Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	store.items[checkpoint.FileID] = checkpoint
	store.mu.Unlock()
	return nil
}

func (store *MemoryCheckpoints) Clear(ctx context.Context, fileID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	delete(store.items, fileID)
	store.mu.Unlock()
	return nil
}
