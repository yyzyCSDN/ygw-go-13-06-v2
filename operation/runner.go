package operation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
)

func (s *Service) Run(ctx context.Context, id string) (Operation, error) {
	op, err := s.Get(ctx, id)
	if err != nil {
		return Operation{}, err
	}
	if !op.CanStart() {
		if op.Terminal() {
			return Operation{}, ErrTerminal
		}
		return Operation{}, fmt.Errorf("%w: cannot start from %s", ErrInvalidTransition, op.State)
	}
	leaseValue, err := s.leases.Acquire("merge/"+op.Manifest.FileID, s.config.WorkerID, s.config.LeaseTTL)
	if err != nil {
		return Operation{}, err
	}
	defer s.leases.Release(leaseValue.Resource, leaseValue.Owner, leaseValue.Fence)
	now := s.config.Clock.Now().UTC()
	running, err := op.transition(StateRunning, now)
	if err != nil {
		return Operation{}, err
	}
	running.Attempt++
	running.Fence = leaseValue.Fence
	running.Owner = leaseValue.Owner
	record, err := s.catalog.GetOperation(id)
	if err != nil {
		return Operation{}, err
	}
	updated, err := s.catalog.Update(toRecord(running), record.Revision)
	if err != nil {
		return Operation{}, err
	}
	started, _ := journal.NewEvent(id, journal.KindStarted, now, map[string]any{"attempt": running.Attempt, "fence": running.Fence})
	started.Attempt, started.Fence = running.Attempt, running.Fence
	if _, err := s.journal.Append(ctx, started); err != nil {
		return Operation{}, err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels[id] = cancel
	request := cloneRequest(s.requests[id])
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.cancels, id)
		s.mu.Unlock()
	}()
	decision := s.admission.Evaluate(request.Manifest, s.currentLoad())
	if !decision.Allowed {
		return s.finishFailure(ctx, running, updated.Revision, fmt.Errorf("%w: %s", policy.ErrRejected, decision.Reason), policy.FailurePermanent)
	}
	writer := segmentmerge.NewMemoryWriter()
	index := segmentmerge.NewMemoryIndex()
	merger := segmentmerge.NewMergerWithBudget(segmentmerge.NewMemorySource(request.Segments), writer, segmentmerge.NewMemoryCheckpoints(), index, decision.Concurrency, decision.MemoryLimit)
	result, mergeErr := merger.Merge(workerCtx, request.Manifest)
	if mergeErr != nil {
		class := classifyFailure(mergeErr)
		return s.finishFailure(context.Background(), running, updated.Revision, mergeErr, class)
	}
	if err := s.leases.Validate(leaseValue.Resource, leaseValue.Owner, leaseValue.Fence); err != nil {
		return s.finishFailure(context.Background(), running, updated.Revision, err, policy.FailureTransient)
	}
	completed, err := running.transition(StateCompleted, s.config.Clock.Now().UTC())
	if err != nil {
		return Operation{}, err
	}
	completed.Result = &result
	completed.Progress.CompletedSegments = len(request.Manifest.Segments)
	completed.Progress.UpdatedAt = completed.UpdatedAt
	completedRecord, err := s.catalog.Update(toRecord(completed), updated.Revision)
	if err != nil {
		return Operation{}, err
	}
	receipt := segmentmerge.CommitReceipt{FileID: result.FileID, Version: result.Version, Size: result.Size, Digest: result.Digest}
	if err := s.catalog.PutArtifact(catalog.ArtifactRecord{Receipt: receipt, OperationID: id, CommittedAt: completed.UpdatedAt}); err != nil {
		return Operation{}, err
	}
	event, _ := journal.NewEvent(id, journal.KindCompleted, completed.UpdatedAt, map[string]any{"receipt": receipt, "peak_resident_bytes": result.PeakResidentBytes})
	event.Attempt, event.Fence = completed.Attempt, completed.Fence
	if _, err := s.journal.Append(ctx, event); err != nil {
		return Operation{}, err
	}
	final := fromRecord(completedRecord, request.Manifest)
	final.Result = &result
	final.Fence, final.Owner = completed.Fence, completed.Owner
	return final, nil
}

func (s *Service) finishFailure(ctx context.Context, running Operation, revision uint64, cause error, class policy.FailureClass) (Operation, error) {
	now := s.config.Clock.Now().UTC()
	decision := s.admission.Retry(running.Attempt, class)
	next := StateFailed
	kind := journal.KindFailed
	if decision.Retry {
		next = StateRetrying
		kind = journal.KindRetrying
	}
	failed, transitionErr := running.transition(next, now)
	if transitionErr != nil {
		return Operation{}, transitionErr
	}
	failed.LastError = cause.Error()
	if decision.Retry {
		retryAt := now.Add(decision.After)
		failed.RetryAt = &retryAt
	}
	updated, updateErr := s.catalog.Update(toRecord(failed), revision)
	if updateErr != nil {
		return Operation{}, updateErr
	}
	event, _ := journal.NewEvent(running.ID, kind, now, map[string]any{"class": decision.Class, "reason": decision.Reason, "error": cause.Error(), "retry_after": decision.After.String()})
	event.Attempt, event.Fence = running.Attempt, running.Fence
	if _, appendErr := s.journal.Append(ctx, event); appendErr != nil {
		return Operation{}, appendErr
	}
	result := fromRecord(updated, running.Manifest)
	result.RetryAt = failed.RetryAt
	result.Fence, result.Owner = running.Fence, running.Owner
	return result, cause
}

func classifyFailure(err error) policy.FailureClass {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return policy.FailureCancelled
	}
	if errors.Is(err, segmentmerge.ErrInvalidManifest) || errors.Is(err, segmentmerge.ErrSegmentIntegrity) {
		return policy.FailurePermanent
	}
	return policy.FailureTransient
}

func (s *Service) RunReady(ctx context.Context, limit int) []error {
	if limit <= 0 {
		limit = 1
	}
	records := s.catalog.ListOperations(string(StateAccepted), limit)
	errorsOut := make([]error, 0)
	for _, record := range records {
		if _, err := s.Run(ctx, record.ID); err != nil {
			errorsOut = append(errorsOut, fmt.Errorf("run %s: %w", record.ID, err))
		}
	}
	return errorsOut
}

func (s *Service) WaitUntilRetry(ctx context.Context, id string) error {
	op, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if op.State != StateRetrying || op.RetryAt == nil {
		return fmt.Errorf("operation is not scheduled for retry")
	}
	delay := time.Until(*op.RetryAt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
