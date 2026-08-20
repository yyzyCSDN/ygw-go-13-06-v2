package segmentmerge

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func verifyLogicalSegment(segment Segment, data []byte) error {
	if int64(len(data)) != segment.LogicalSize {
		return fmt.Errorf("%w: segment %s size=%d want=%d", ErrSegmentIntegrity, segment.ID, len(data), segment.LogicalSize)
	}
	if Digest(data) != segment.Digest {
		return fmt.Errorf("%w: segment %s digest mismatch", ErrSegmentIntegrity, segment.ID)
	}
	return nil
}

func validateReceipt(receipt CommitReceipt) error {
	decoded, err := hex.DecodeString(receipt.Digest)
	if receipt.FileID == "" || receipt.Version == "" || receipt.Size < 0 || err != nil || len(decoded) != 32 {
		return fmt.Errorf("%w: invalid commit receipt", ErrWriterState)
	}
	return nil
}
