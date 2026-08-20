package segmentmerge

import "errors"

var (
	ErrInvalidManifest    = errors.New("invalid segment manifest")
	ErrSegmentNotFound    = errors.New("segment not found")
	ErrSegmentIntegrity   = errors.New("segment integrity check failed")
	ErrCheckpointMismatch = errors.New("checkpoint does not match manifest")
	ErrWriterState        = errors.New("invalid merge writer state")
	ErrBudgetExceeded     = errors.New("logical byte budget exceeded")
)
