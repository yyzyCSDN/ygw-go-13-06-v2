package segmentmerge

import (
	"context"
	"fmt"
)

type Merger struct {
	loader      *Loader
	writer      Writer
	checkpoints CheckpointStore
	index       Index
	concurrency int
	budget      *ByteBudget
}

func NewMerger(source Source, writer Writer, checkpoints CheckpointStore, index Index, concurrency int) *Merger {
	return NewMergerWithBudget(source, writer, checkpoints, index, concurrency, 1<<30)
}

func NewMergerWithBudget(source Source, writer Writer, checkpoints CheckpointStore, index Index, concurrency int, memoryLimit int64) *Merger {
	if concurrency < 1 {
		concurrency = 1
	}
	budget := NewByteBudget(memoryLimit)
	return &Merger{loader: NewLoader(source, budget), writer: writer, checkpoints: checkpoints, index: index, concurrency: concurrency, budget: budget}
}

type loadResult struct {
	loaded LoadedSegment
	err    error
}

func (merger *Merger) Merge(ctx context.Context, manifest Manifest) (MergeResult, error) {
	compiled, err := CompileManifest(manifest)
	if err != nil {
		return MergeResult{}, err
	}
	checkpoint, found, err := merger.checkpoints.Load(ctx, compiled.FileID)
	if err != nil {
		return MergeResult{}, err
	}
	var resume *Checkpoint
	if found {
		resume = &checkpoint
	}
	plan, err := BuildPlan(compiled, resume)
	if err != nil {
		return MergeResult{}, err
	}
	session, err := merger.writer.Begin(ctx, plan)
	if err != nil {
		return MergeResult{}, err
	}
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	results := merger.loadAll(workerCtx, plan.Entries)
	pending := make(map[int]LoadedSegment)
	next := plan.StartOrdinal
	for result := range results {
		if result.err != nil {
			cancelWorkers()
			releasePending(pending)
			drainLoadResults(results)
			_ = session.Abort(context.Background())
			_ = merger.checkpoints.Clear(context.Background(), compiled.FileID)
			return MergeResult{}, result.err
		}
		pending[result.loaded.Entry.Ordinal] = result.loaded
		for {
			loaded, ok := pending[next]
			if !ok {
				break
			}
			if err := session.AppendAt(ctx, loaded.Entry.Segment.Offset, loaded.Data); err != nil {
				loaded.release()
				cancelWorkers()
				releasePending(pending)
				drainLoadResults(results)
				_ = session.Abort(context.Background())
				_ = merger.checkpoints.Clear(context.Background(), compiled.FileID)
				return MergeResult{}, fmt.Errorf("write segment %s: %w", loaded.Entry.Segment.ID, err)
			}
			next++
			loaded.release()
			if err := merger.checkpoints.Save(ctx, Checkpoint{FileID: compiled.FileID, Version: compiled.Version, SessionID: session.ID(), NextOrdinal: next, OutputSize: loaded.Entry.Segment.Offset + int64(len(loaded.Data))}); err != nil {
				cancelWorkers()
				releasePending(pending)
				drainLoadResults(results)
				_ = session.Abort(context.Background())
				_ = merger.checkpoints.Clear(context.Background(), compiled.FileID)
				return MergeResult{}, err
			}
			delete(pending, loaded.Entry.Ordinal)
		}
	}
	if next != len(compiled.Segments) {
		releasePending(pending)
		_ = session.Abort(context.Background())
		_ = merger.checkpoints.Clear(context.Background(), compiled.FileID)
		return MergeResult{}, fmt.Errorf("%w: incomplete worker results", ErrWriterState)
	}
	receipt, err := session.Commit(ctx)
	if err != nil {
		return MergeResult{}, err
	}
	if err := validateReceipt(receipt); err != nil {
		return MergeResult{}, err
	}
	if err := merger.index.Put(ctx, receipt); err != nil {
		return MergeResult{}, err
	}
	if err := merger.checkpoints.Clear(ctx, compiled.FileID); err != nil {
		return MergeResult{}, err
	}
	return MergeResult{FileID: receipt.FileID, Version: receipt.Version, Size: receipt.Size, Digest: receipt.Digest, SegmentCount: len(compiled.Segments), Resumed: found, BudgetLimit: merger.budget.Limit(), PeakResidentBytes: merger.budget.PeakResident()}, nil
}

func drainLoadResults(results <-chan loadResult) {
	for result := range results {
		result.loaded.release()
	}
}

func releasePending(pending map[int]LoadedSegment) {
	for ordinal, loaded := range pending {
		loaded.release()
		delete(pending, ordinal)
	}
}

func (merger *Merger) loadAll(ctx context.Context, entries []PlanEntry) <-chan loadResult {
	jobs := make(chan PlanEntry)
	results := make(chan loadResult, len(entries))
	remaining := merger.concurrency
	if remaining > len(entries) {
		remaining = len(entries)
	}
	if remaining == 0 {
		close(results)
		return results
	}
	done := make(chan struct{}, remaining)
	for worker := 0; worker < remaining; worker++ {
		go func() {
			for entry := range jobs {
				loaded, err := merger.loader.Load(ctx, entry)
				results <- loadResult{loaded: loaded, err: err}
			}
			done <- struct{}{}
		}()
	}
	go func() {
		for _, entry := range entries {
			jobs <- entry
		}
		close(jobs)
		for i := 0; i < remaining; i++ {
			<-done
		}
		close(results)
	}()
	return results
}
