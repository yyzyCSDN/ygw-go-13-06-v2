package policy

import (
	"errors"
	"fmt"
	"math"
	"time"

	"example.com/segmentmerger/segmentmerge"
)

var ErrRejected = errors.New("merge admission rejected")

type Load struct {
	ActiveOperations int   `json:"active_operations"`
	ResidentBytes    int64 `json:"resident_bytes"`
}

type Limits struct {
	MaxActiveOperations int
	MaxManifestBytes    int64
	MaxSegments         int
	MaxResidentBytes    int64
	MaxAttempts         int
	BaseBackoff         time.Duration
	MaxBackoff          time.Duration
}

func DefaultLimits() Limits {
	return Limits{
		MaxActiveOperations: 16,
		MaxManifestBytes:    4 << 30,
		MaxSegments:         4096,
		MaxResidentBytes:    512 << 20,
		MaxAttempts:         4,
		BaseBackoff:         100 * time.Millisecond,
		MaxBackoff:          5 * time.Second,
	}
}

type Decision struct {
	Allowed         bool   `json:"allowed"`
	Reason          string `json:"reason"`
	Concurrency     int    `json:"concurrency"`
	MemoryLimit     int64  `json:"memory_limit"`
	EstimatedWeight int64  `json:"estimated_weight"`
}

type Admission struct{ limits Limits }

func NewAdmission(limits Limits) *Admission {
	defaults := DefaultLimits()
	if limits.MaxActiveOperations <= 0 {
		limits.MaxActiveOperations = defaults.MaxActiveOperations
	}
	if limits.MaxManifestBytes <= 0 {
		limits.MaxManifestBytes = defaults.MaxManifestBytes
	}
	if limits.MaxSegments <= 0 {
		limits.MaxSegments = defaults.MaxSegments
	}
	if limits.MaxResidentBytes <= 0 {
		limits.MaxResidentBytes = defaults.MaxResidentBytes
	}
	if limits.MaxAttempts <= 0 {
		limits.MaxAttempts = defaults.MaxAttempts
	}
	if limits.BaseBackoff <= 0 {
		limits.BaseBackoff = defaults.BaseBackoff
	}
	if limits.MaxBackoff <= 0 {
		limits.MaxBackoff = defaults.MaxBackoff
	}
	return &Admission{limits: limits}
}

func (a *Admission) Evaluate(manifest segmentmerge.Manifest, load Load) Decision {
	decision := Decision{Allowed: false, Concurrency: 1, MemoryLimit: a.limits.MaxResidentBytes}
	if load.ActiveOperations >= a.limits.MaxActiveOperations {
		decision.Reason = "active operation limit reached"
		return decision
	}
	if load.ResidentBytes >= a.limits.MaxResidentBytes {
		decision.Reason = "resident byte limit reached"
		return decision
	}
	if manifest.TotalSize <= 0 || manifest.TotalSize > a.limits.MaxManifestBytes {
		decision.Reason = "manifest size outside admission range"
		return decision
	}
	if len(manifest.Segments) == 0 || len(manifest.Segments) > a.limits.MaxSegments {
		decision.Reason = "segment count outside admission range"
		return decision
	}
	var peak int64
	for _, segment := range manifest.Segments {
		if segment.LogicalSize > peak {
			peak = segment.LogicalSize
		}
	}
	available := a.limits.MaxResidentBytes - load.ResidentBytes
	if peak > available {
		decision.Reason = "largest segment exceeds available byte budget"
		return decision
	}
	decision.Allowed = true
	decision.Reason = "admitted"
	decision.EstimatedWeight = peak
	decision.MemoryLimit = available
	byMemory := int(available / max64(peak, 1))
	bySegments := int(math.Ceil(math.Sqrt(float64(len(manifest.Segments)))))
	decision.Concurrency = minInt(8, maxInt(1, minInt(byMemory, bySegments)))
	return decision
}

type FailureClass string

const (
	FailureTransient FailureClass = "transient"
	FailurePermanent FailureClass = "permanent"
	FailureCancelled FailureClass = "cancelled"
)

type RetryDecision struct {
	Retry  bool          `json:"retry"`
	After  time.Duration `json:"after"`
	Class  FailureClass  `json:"class"`
	Reason string        `json:"reason"`
}

func (a *Admission) Retry(attempt int, class FailureClass) RetryDecision {
	if class == FailureCancelled {
		return RetryDecision{Class: class, Reason: "caller cancelled"}
	}
	if class == FailurePermanent {
		return RetryDecision{Class: class, Reason: "permanent validation or integrity failure"}
	}
	if attempt >= a.limits.MaxAttempts {
		return RetryDecision{Class: class, Reason: fmt.Sprintf("attempt limit %d reached", a.limits.MaxAttempts)}
	}
	delay := a.limits.BaseBackoff * time.Duration(1<<maxInt(0, attempt-1))
	if delay > a.limits.MaxBackoff {
		delay = a.limits.MaxBackoff
	}
	return RetryDecision{Retry: true, After: delay, Class: class, Reason: "transient failure"}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
