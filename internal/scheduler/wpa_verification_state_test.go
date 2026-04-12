package scheduler

import (
	"testing"

	"github.com/0xforce/xfo-miner/internal/pool"
)

func TestWPAVerificationStateBlocksNonVerificationWhenRequired(t *testing.T) {
	state := newWPAVerificationState()
	state.markRequirement("epoch-1", false)

	blocked, reason := state.shouldBlockJob(&pool.JobGPUMessage{JobID: "job-1", Target: "a.hc22000"}, false)
	if !blocked {
		t.Fatalf("expected job to be blocked while verification is required")
	}
	if reason != "verification_required" {
		t.Fatalf("unexpected block reason: %q", reason)
	}
}

func TestWPAVerificationStateAllowsVerificationChallengeWhenRequired(t *testing.T) {
	state := newWPAVerificationState()
	state.markRequirement("epoch-1", false)

	blocked, reason := state.shouldBlockJob(&pool.JobGPUMessage{
		JobID:                "job-verification",
		Target:               "a.hc22000",
		VerificationRequired: true,
		VerificationEpochID:  "epoch-1",
	}, false)
	if blocked {
		t.Fatalf("expected verification challenge to pass gate, reason=%q", reason)
	}

	state.markFresh("epoch-1")
	snapshot := state.snapshot()
	if snapshot.VerificationState != string(VerificationStateFresh) {
		t.Fatalf("expected fresh state after verification, got %q", snapshot.VerificationState)
	}
	if snapshot.LastVerifiedEpochID != "epoch-1" {
		t.Fatalf("expected last verified epoch epoch-1, got %q", snapshot.LastVerifiedEpochID)
	}
}

func TestWPAVerificationStateDeferredWithActiveTask(t *testing.T) {
	state := newWPAVerificationState()
	state.markRequirement("epoch-2", true)

	snapshot := state.snapshot()
	if snapshot.VerificationState != string(VerificationStateDeferred) {
		t.Fatalf("expected deferred state, got %q", snapshot.VerificationState)
	}
	if snapshot.VerificationDeferredReason != "active_task_non_interruptible" {
		t.Fatalf("unexpected deferred reason: %q", snapshot.VerificationDeferredReason)
	}

	blocked, reason := state.shouldBlockJob(&pool.JobGPUMessage{JobID: "job-2", Target: "a.hc22000"}, false)
	if !blocked || reason != "verification_deferred" {
		t.Fatalf("expected deferred gate block, blocked=%v reason=%q", blocked, reason)
	}
}
