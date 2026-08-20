package segmentmerge

import "fmt"

func BuildPlan(manifest Manifest, checkpoint *Checkpoint) (MergePlan, error) {
	compiled, err := CompileManifest(manifest)
	if err != nil {
		return MergePlan{}, err
	}
	plan := MergePlan{FileID: compiled.FileID, Version: compiled.Version, TotalSize: compiled.TotalSize}
	if checkpoint != nil {
		if !checkpoint.Matches(compiled) || checkpoint.NextOrdinal < 0 || checkpoint.NextOrdinal > len(compiled.Segments) {
			return MergePlan{}, fmt.Errorf("%w: file=%s version=%s", ErrCheckpointMismatch, checkpoint.FileID, checkpoint.Version)
		}
		plan.StartOrdinal = checkpoint.NextOrdinal
		plan.SessionID = checkpoint.SessionID
	}
	plan.Entries = make([]PlanEntry, 0, len(compiled.Segments)-plan.StartOrdinal)
	for ordinal := plan.StartOrdinal; ordinal < len(compiled.Segments); ordinal++ {
		segment := compiled.Segments[ordinal]
		plan.Entries = append(plan.Entries, PlanEntry{Ordinal: ordinal, MemoryWeight: segment.LogicalSize, Segment: segment})
	}
	return plan, nil
}
