package segmentmerge

type Encoding string

const (
	EncodingRaw  Encoding = "raw"
	EncodingGzip Encoding = "gzip"
)

// Segment describes logical output placement and stored representation.
type Segment struct {
	ID          string   `json:"id"`
	Offset      int64    `json:"offset"`
	StoredSize  int64    `json:"stored_size"`
	LogicalSize int64    `json:"logical_size"`
	Digest      string   `json:"digest"`
	Encoding    Encoding `json:"encoding"`
}

type Manifest struct {
	FileID    string    `json:"file_id"`
	Version   string    `json:"version"`
	TotalSize int64     `json:"total_size"`
	Segments  []Segment `json:"segments"`
}

type PlanEntry struct {
	Ordinal      int
	MemoryWeight int64
	Segment      Segment
}

type MergePlan struct {
	FileID       string
	Version      string
	TotalSize    int64
	StartOrdinal int
	SessionID    string
	Entries      []PlanEntry
}

type LoadedSegment struct {
	Entry PlanEntry
	Data  []byte
	lease *BudgetLease
}

type Checkpoint struct {
	FileID      string
	Version     string
	SessionID   string
	NextOrdinal int
	OutputSize  int64
}

func (checkpoint Checkpoint) Matches(manifest Manifest) bool {
	return checkpoint.FileID == manifest.FileID && checkpoint.Version == manifest.Version
}

type CommitReceipt struct {
	FileID  string
	Version string
	Size    int64
	Digest  string
}

type MergeResult struct {
	FileID            string
	Version           string
	Size              int64
	Digest            string
	SegmentCount      int
	Resumed           bool
	BudgetLimit       int64
	PeakResidentBytes int64
}

func (loaded *LoadedSegment) release() {
	if loaded != nil && loaded.lease != nil {
		loaded.lease.Release()
		loaded.lease = nil
	}
}
