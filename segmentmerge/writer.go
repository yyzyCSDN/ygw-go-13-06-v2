package segmentmerge

import (
	"context"
	"fmt"
	"sync"
)

type WriteSession interface {
	ID() string
	AppendAt(context.Context, int64, []byte) error
	Commit(context.Context) (CommitReceipt, error)
	Abort(context.Context) error
}

type Writer interface {
	Begin(context.Context, MergePlan) (WriteSession, error)
}

type memoryDraft struct {
	fileID, version string
	data            []byte
}

type MemoryWriter struct {
	mu      sync.Mutex
	next    int
	drafts  map[string]*memoryDraft
	outputs map[string]CommitReceipt
	data    map[string][]byte
}

func NewMemoryWriter() *MemoryWriter {
	return &MemoryWriter{drafts: make(map[string]*memoryDraft), outputs: make(map[string]CommitReceipt), data: make(map[string][]byte)}
}

func (writer *MemoryWriter) Begin(ctx context.Context, plan MergePlan) (WriteSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if plan.SessionID != "" {
		draft, ok := writer.drafts[plan.SessionID]
		if !ok || draft.fileID != plan.FileID || draft.version != plan.Version || int64(len(draft.data)) != plan.TotalSize {
			return nil, fmt.Errorf("%w: resume session %q", ErrWriterState, plan.SessionID)
		}
		return &memorySession{owner: writer, id: plan.SessionID, draft: draft}, nil
	}
	writer.next++
	id := fmt.Sprintf("merge-%d", writer.next)
	draft := &memoryDraft{fileID: plan.FileID, version: plan.Version, data: make([]byte, plan.TotalSize)}
	writer.drafts[id] = draft
	return &memorySession{owner: writer, id: id, draft: draft}, nil
}

func (writer *MemoryWriter) Output(fileID string) ([]byte, CommitReceipt, bool) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	data, ok := writer.data[fileID]
	return append([]byte(nil), data...), writer.outputs[fileID], ok
}

type memorySession struct {
	owner  *MemoryWriter
	id     string
	draft  *memoryDraft
	closed bool
}

func (session *memorySession) ID() string { return session.id }
func (session *memorySession) AppendAt(ctx context.Context, offset int64, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session.owner.mu.Lock()
	defer session.owner.mu.Unlock()
	if session.closed || offset < 0 || offset > int64(len(session.draft.data))-int64(len(data)) {
		return ErrWriterState
	}
	copy(session.draft.data[offset:], data)
	return nil
}
func (session *memorySession) Commit(ctx context.Context) (CommitReceipt, error) {
	if err := ctx.Err(); err != nil {
		return CommitReceipt{}, err
	}
	session.owner.mu.Lock()
	defer session.owner.mu.Unlock()
	if session.closed {
		return CommitReceipt{}, ErrWriterState
	}
	session.closed = true
	receipt := CommitReceipt{FileID: session.draft.fileID, Version: session.draft.version, Size: int64(len(session.draft.data)), Digest: Digest(session.draft.data)}
	session.owner.outputs[receipt.FileID] = receipt
	session.owner.data[receipt.FileID] = append([]byte(nil), session.draft.data...)
	delete(session.owner.drafts, session.id)
	return receipt, nil
}
func (session *memorySession) Abort(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session.owner.mu.Lock()
	defer session.owner.mu.Unlock()
	if session.closed {
		return nil
	}
	session.closed = true
	delete(session.owner.drafts, session.id)
	return nil
}
