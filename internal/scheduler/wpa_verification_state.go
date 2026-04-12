package scheduler

import (
	"strings"
	"sync"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
)

const (
	VerificationStateRequired State = "required"
	VerificationStatePending  State = "pending"
	VerificationStateFresh    State = "fresh"
	VerificationStateStale    State = "stale"
	VerificationStateDeferred State = "deferred"
	VerificationStateFailed   State = "failed"
)

type VerificationSnapshot struct {
	LastVerifiedEpochID        string
	LastVerifiedAt             int64
	VerificationState          string
	VerificationDeferredReason string
}

type wpaVerificationState struct {
	mu sync.RWMutex

	lastVerifiedEpochID        string
	lastVerifiedAt             int64
	verificationState          string
	verificationDeferredReason string
	requiredEpochID            string
}

func newWPAVerificationState() *wpaVerificationState {
	return &wpaVerificationState{verificationState: string(VerificationStateFresh)}
}

func (s *wpaVerificationState) snapshot() VerificationSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return VerificationSnapshot{
		LastVerifiedEpochID:        s.lastVerifiedEpochID,
		LastVerifiedAt:             s.lastVerifiedAt,
		VerificationState:          s.verificationState,
		VerificationDeferredReason: s.verificationDeferredReason,
	}
}

func (s *wpaVerificationState) markRequirement(epochID string, hasActiveTask bool) {
	epochID = strings.TrimSpace(epochID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if epochID != "" {
		s.requiredEpochID = epochID
	}

	if hasActiveTask {
		s.verificationState = string(VerificationStateDeferred)
		s.verificationDeferredReason = "active_task_non_interruptible"
		return
	}

	if epochID != "" && s.lastVerifiedEpochID != "" && s.lastVerifiedEpochID != epochID {
		s.verificationState = string(VerificationStateStale)
		s.verificationDeferredReason = ""
		return
	}

	if s.verificationState == string(VerificationStateFresh) && s.lastVerifiedEpochID == epochID {
		return
	}

	s.verificationState = string(VerificationStateRequired)
	s.verificationDeferredReason = ""
}

func (s *wpaVerificationState) markFresh(epochID string) {
	epochID = strings.TrimSpace(epochID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if epochID == "" {
		epochID = s.requiredEpochID
	}

	s.lastVerifiedEpochID = epochID
	s.lastVerifiedAt = time.Now().Unix()
	s.verificationState = string(VerificationStateFresh)
	s.verificationDeferredReason = ""
	if epochID != "" {
		s.requiredEpochID = epochID
	}
}

func (s *wpaVerificationState) markFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verificationState = string(VerificationStateFailed)
	s.verificationDeferredReason = "verification_failed"
}

func (s *wpaVerificationState) shouldBlockJob(job *pool.JobGPUMessage, hasActiveTask bool) (bool, string) {
	if hasActiveTask {
		return true, "verification_deferred"
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state := strings.TrimSpace(s.verificationState)
	if state == "" {
		return true, "verification_state_invalid"
	}

	if s.requiredEpochID == "" && state == string(VerificationStateFresh) {
		return false, ""
	}

	isVerificationJob := isVerificationChallengeJob(job, s.requiredEpochID)

	switch state {
	case string(VerificationStateFresh):
		if s.requiredEpochID != "" && s.lastVerifiedEpochID != s.requiredEpochID {
			if isVerificationJob {
				s.verificationState = string(VerificationStatePending)
				return false, ""
			}
			return true, "verification_required"
		}
		return false, ""
	case string(VerificationStateDeferred):
		s.verificationState = string(VerificationStatePending)
		s.verificationDeferredReason = ""
		return true, "verification_deferred"
	case string(VerificationStateRequired), string(VerificationStateStale), string(VerificationStatePending):
		if isVerificationJob {
			s.verificationState = string(VerificationStatePending)
			s.verificationDeferredReason = ""
			return false, ""
		}
		return true, "verification_required"
	case string(VerificationStateFailed):
		if isVerificationJob {
			s.verificationState = string(VerificationStatePending)
			s.verificationDeferredReason = ""
			return false, ""
		}
		return true, "verification_failed"
	default:
		return true, "verification_state_invalid"
	}
}

func (s *wpaVerificationState) currentRequiredEpoch() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return strings.TrimSpace(s.requiredEpochID)
}

func isVerificationChallengeJob(job *pool.JobGPUMessage, requiredEpochID string) bool {
	if job == nil {
		return false
	}

	if job.VerificationRequired {
		return true
	}

	jobEpoch := strings.TrimSpace(job.VerificationEpochID)
	if jobEpoch != "" {
		if requiredEpochID == "" {
			return true
		}
		return jobEpoch == strings.TrimSpace(requiredEpochID)
	}

	if strings.TrimSpace(job.ChallengeID) != "" {
		return true
	}

	return false
}
