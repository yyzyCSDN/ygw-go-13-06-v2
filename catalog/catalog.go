package catalog

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"example.com/segmentmerger/segmentmerge"
)

var (
	ErrOperationNotFound = errors.New("operation not found")
	ErrArtifactNotFound  = errors.New("artifact not found")
	ErrRevisionConflict  = errors.New("catalog revision conflict")
)

type OperationRecord struct {
	ID             string                    `json:"id"`
	Partition      uint32                    `json:"partition"`
	FileID         string                    `json:"file_id"`
	IdempotencyKey string                    `json:"idempotency_key"`
	State          string                    `json:"state"`
	Revision       uint64                    `json:"revision"`
	Attempt        int                       `json:"attempt"`
	Fence          uint64                    `json:"fence,omitempty"`
	Owner          string                    `json:"owner,omitempty"`
	Completed      int                       `json:"completed_segments"`
	Total          int                       `json:"total_segments"`
	LastError      string                    `json:"last_error,omitempty"`
	RetryAt        *time.Time                `json:"retry_at,omitempty"`
	Result         *segmentmerge.MergeResult `json:"result,omitempty"`
	CreatedAt      time.Time                 `json:"created_at"`
	UpdatedAt      time.Time                 `json:"updated_at"`
}

type ArtifactRecord struct {
	Receipt     segmentmerge.CommitReceipt `json:"receipt"`
	OperationID string                     `json:"operation_id"`
	CommittedAt time.Time                  `json:"committed_at"`
}

type Catalog struct {
	mu          sync.RWMutex
	operations  map[string]OperationRecord
	idempotency map[string]string
	artifacts   map[string]ArtifactRecord
}

func New() *Catalog {
	return &Catalog{
		operations:  make(map[string]OperationRecord),
		idempotency: make(map[string]string),
		artifacts:   make(map[string]ArtifactRecord),
	}
}

func (c *Catalog) Create(record OperationRecord) (OperationRecord, bool, error) {
	if record.ID == "" || record.FileID == "" || record.IdempotencyKey == "" {
		return OperationRecord{}, false, fmt.Errorf("id, file id and idempotency key are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existingID, ok := c.idempotency[record.IdempotencyKey]; ok {
		existing := c.operations[existingID]
		if existing.FileID != record.FileID {
			return OperationRecord{}, false, fmt.Errorf("idempotency key belongs to file %s", existing.FileID)
		}
		return existing, false, nil
	}
	if _, exists := c.operations[record.ID]; exists {
		return OperationRecord{}, false, fmt.Errorf("operation %s already exists", record.ID)
	}
	record.Revision = 1
	c.operations[record.ID] = record
	c.idempotency[record.IdempotencyKey] = record.ID
	return record, true, nil
}

func (c *Catalog) GetOperation(id string) (OperationRecord, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	record, ok := c.operations[id]
	if !ok {
		return OperationRecord{}, ErrOperationNotFound
	}
	return record, nil
}

func (c *Catalog) Update(record OperationRecord, expectedRevision uint64) (OperationRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.operations[record.ID]
	if !ok {
		return OperationRecord{}, ErrOperationNotFound
	}
	if current.Revision != expectedRevision {
		return OperationRecord{}, fmt.Errorf("%w: expected %d got %d", ErrRevisionConflict, expectedRevision, current.Revision)
	}
	if current.FileID != record.FileID || current.IdempotencyKey != record.IdempotencyKey {
		return OperationRecord{}, fmt.Errorf("immutable operation identity changed")
	}
	record.Revision = current.Revision + 1
	c.operations[record.ID] = record
	return record, nil
}

func (c *Catalog) PutArtifact(record ArtifactRecord) error {
	if record.Receipt.FileID == "" || record.OperationID == "" {
		return fmt.Errorf("artifact identity is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.artifacts[record.Receipt.FileID]; ok {
		if current.Receipt.Version != record.Receipt.Version || current.Receipt.Digest != record.Receipt.Digest {
			return fmt.Errorf("artifact %s conflicts with committed version", record.Receipt.FileID)
		}
		return nil
	}
	c.artifacts[record.Receipt.FileID] = record
	return nil
}

func (c *Catalog) GetArtifact(fileID string) (ArtifactRecord, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	record, ok := c.artifacts[fileID]
	if !ok {
		return ArtifactRecord{}, ErrArtifactNotFound
	}
	return record, nil
}

func (c *Catalog) ListOperations(state string, limit int) []OperationRecord {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	c.mu.RLock()
	result := make([]OperationRecord, 0, len(c.operations))
	for _, record := range c.operations {
		if state == "" || record.State == state {
			result = append(result, record)
		}
	}
	c.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].UpdatedAt.After(result[j].UpdatedAt)
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

func (c *Catalog) Counts() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	counts := map[string]int{"operations": len(c.operations), "artifacts": len(c.artifacts)}
	for _, record := range c.operations {
		counts["state."+record.State]++
	}
	return counts
}
