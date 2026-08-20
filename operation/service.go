package operation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
	"github.com/cespare/xxhash/v2"
)

type Config struct {
	WorkerID       string
	PartitionCount uint32
	LeaseTTL       time.Duration
	Clock          lease.Clock
}

type Service struct {
	config    Config
	catalog   *catalog.Catalog
	journal   journal.Store
	leases    *lease.Manager
	admission *policy.Admission
	sequence  atomic.Uint64
	mu        sync.RWMutex
	requests  map[string]Request
	cancels   map[string]context.CancelFunc
}

func NewService(config Config, records *catalog.Catalog, events journal.Store, leases *lease.Manager, admission *policy.Admission) *Service {
	if config.WorkerID == "" {
		config.WorkerID = "worker-local"
	}
	if config.PartitionCount == 0 {
		config.PartitionCount = 64
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = 30 * time.Second
	}
	if config.Clock == nil {
		config.Clock = lease.SystemClock{}
	}
	if records == nil {
		records = catalog.New()
	}
	if events == nil {
		events = journal.NewMemoryStore()
	}
	if leases == nil {
		leases = lease.NewManager(config.Clock)
	}
	if admission == nil {
		admission = policy.NewAdmission(policy.DefaultLimits())
	}
	return &Service{config: config, catalog: records, journal: events, leases: leases, admission: admission, requests: make(map[string]Request), cancels: make(map[string]context.CancelFunc)}
}

func (s *Service) Submit(ctx context.Context, request Request) (Operation, bool, error) {
	if err := request.Validate(); err != nil {
		return Operation{}, false, err
	}
	admissionManifest := policy.ProjectStoredAdmissionInput(request.Manifest)
	decision := s.admission.Evaluate(admissionManifest, s.currentLoad())
	if !decision.Allowed {
		return Operation{}, false, fmt.Errorf("%w: %s", policy.ErrRejected, decision.Reason)
	}
	now := s.config.Clock.Now().UTC()
	partition := uint32(xxhash.Sum64String(request.IdempotencyKey) % uint64(s.config.PartitionCount))
	id := fmt.Sprintf("merge-%02d-%016x", partition, s.sequence.Add(1))
	op := Operation{ID: id, Partition: partition, IdempotencyKey: request.IdempotencyKey, Manifest: request.Manifest, State: StateAccepted, CreatedAt: now, UpdatedAt: now, Progress: Progress{TotalSegments: len(request.Manifest.Segments), UpdatedAt: now}}
	record := toRecord(op)
	stored, created, err := s.catalog.Create(record)
	if err != nil {
		return Operation{}, false, err
	}
	if !created {
		existing, err := s.Get(ctx, stored.ID)
		return existing, false, err
	}
	s.mu.Lock()
	s.requests[op.ID] = cloneRequest(request)
	s.mu.Unlock()
	event, _ := journal.NewEvent(op.ID, journal.KindAccepted, now, map[string]any{"file_id": op.Manifest.FileID, "partition": partition, "segments": len(op.Manifest.Segments)})
	if _, err := s.journal.Append(ctx, event); err != nil {
		failed, transitionErr := op.transition(StateFailed, now)
		if transitionErr == nil {
			failed.LastError = err.Error()
			_, _ = s.catalog.Update(toRecord(failed), stored.Revision)
		}
		return Operation{}, false, err
	}
	return op, true, nil
}

func (s *Service) Get(ctx context.Context, id string) (Operation, error) {
	if err := ctx.Err(); err != nil {
		return Operation{}, err
	}
	record, err := s.catalog.GetOperation(id)
	if errors.Is(err, catalog.ErrOperationNotFound) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, err
	}
	s.mu.RLock()
	request, ok := s.requests[id]
	s.mu.RUnlock()
	if !ok {
		return Operation{}, fmt.Errorf("%w: request payload unavailable", ErrNotFound)
	}
	return fromRecord(record, request.Manifest), nil
}

func (s *Service) Cancel(ctx context.Context, id string) (Operation, error) {
	op, err := s.Get(ctx, id)
	if err != nil {
		return Operation{}, err
	}
	if op.Terminal() {
		return Operation{}, ErrTerminal
	}
	now := s.config.Clock.Now().UTC()
	cancelled, err := op.transition(StateCancelled, now)
	if err != nil {
		return Operation{}, err
	}
	record, _ := s.catalog.GetOperation(id)
	updated, err := s.catalog.Update(toRecord(cancelled), record.Revision)
	if err != nil {
		return Operation{}, err
	}
	s.mu.Lock()
	if cancel := s.cancels[id]; cancel != nil {
		cancel()
	}
	s.mu.Unlock()
	event, _ := journal.NewEvent(id, journal.KindCancelled, now, map[string]any{"previous_state": op.State})
	if _, err := s.journal.Append(ctx, event); err != nil {
		return Operation{}, err
	}
	return fromRecord(updated, op.Manifest), nil
}

func (s *Service) Replay(ctx context.Context, id string) ([]journal.Envelope, error) {
	replay, err := s.journal.Replay(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(replay.Events) == 0 {
		return nil, ErrNotFound
	}
	return replay.Events, nil
}

func (s *Service) Artifact(ctx context.Context, fileID string) (catalog.ArtifactRecord, error) {
	if err := ctx.Err(); err != nil {
		return catalog.ArtifactRecord{}, err
	}
	return s.catalog.GetArtifact(fileID)
}

func (s *Service) List(state string, limit int) []catalog.OperationRecord {
	return s.catalog.ListOperations(state, limit)
}

func (s *Service) currentLoad() policy.Load {
	counts := s.catalog.Counts()
	active := counts["state."+string(StateAccepted)] + counts["state."+string(StateRunning)] + counts["state."+string(StateRetrying)]
	return policy.Load{ActiveOperations: active}
}

func cloneRequest(input Request) Request {
	copyRequest := input
	copyRequest.Manifest.Segments = append([]segmentmerge.Segment(nil), input.Manifest.Segments...)
	copyRequest.Segments = make(map[string][]byte, len(input.Segments))
	for id, data := range input.Segments {
		copyRequest.Segments[id] = append([]byte(nil), data...)
	}
	return copyRequest
}

func toRecord(op Operation) catalog.OperationRecord {
	return catalog.OperationRecord{ID: op.ID, Partition: op.Partition, FileID: op.Manifest.FileID, IdempotencyKey: op.IdempotencyKey, State: string(op.State), Attempt: op.Attempt, Fence: op.Fence, Owner: op.Owner, Completed: op.Progress.CompletedSegments, Total: op.Progress.TotalSegments, LastError: op.LastError, RetryAt: op.RetryAt, Result: op.Result, CreatedAt: op.CreatedAt, UpdatedAt: op.UpdatedAt}
}

func fromRecord(record catalog.OperationRecord, manifest segmentmerge.Manifest) Operation {
	return Operation{ID: record.ID, Partition: record.Partition, IdempotencyKey: record.IdempotencyKey, Manifest: manifest, State: State(record.State), Attempt: record.Attempt, Fence: record.Fence, Owner: record.Owner, Progress: Progress{CompletedSegments: record.Completed, TotalSegments: record.Total, UpdatedAt: record.UpdatedAt}, Result: record.Result, LastError: record.LastError, RetryAt: record.RetryAt, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt}
}

func marshalPayload(value any) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}
