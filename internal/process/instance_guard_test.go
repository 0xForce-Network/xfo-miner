package process

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewInstanceGuardRejectsEmptyAnchor(t *testing.T) {
	t.Parallel()

	guard, err := NewInstanceGuard("   ")
	if err == nil {
		t.Fatalf("expected error for empty anchor path")
	}
	if guard != nil {
		t.Fatalf("expected nil guard for empty anchor path")
	}
}

func TestInstanceGuardAcquireAndRelease(t *testing.T) {
	t.Parallel()

	anchor := filepath.Join(t.TempDir(), "config.json.identity_state.json")
	guard, err := NewInstanceGuard(anchor)
	if err != nil {
		t.Fatalf("NewInstanceGuard() error = %v", err)
	}

	if err := guard.Acquire(500 * time.Millisecond); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	if err := guard.Release(); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestInstanceGuardRejectsConcurrentAcquire(t *testing.T) {
	t.Parallel()

	anchor := filepath.Join(t.TempDir(), "config.json.identity_state.json")
	first, err := NewInstanceGuard(anchor)
	if err != nil {
		t.Fatalf("NewInstanceGuard(first) error = %v", err)
	}
	second, err := NewInstanceGuard(anchor)
	if err != nil {
		t.Fatalf("NewInstanceGuard(second) error = %v", err)
	}

	if err := first.Acquire(500 * time.Millisecond); err != nil {
		t.Fatalf("first.Acquire() error = %v", err)
	}
	t.Cleanup(func() {
		_ = first.Release()
	})

	err = second.Acquire(300 * time.Millisecond)
	if err == nil {
		t.Fatalf("expected second acquire to fail while first guard is active")
	}
	if !errors.Is(err, errInstanceAlreadyRunning) {
		t.Fatalf("expected errInstanceAlreadyRunning, got %v", err)
	}
	if !strings.Contains(err.Error(), "owner_pid=") {
		t.Fatalf("expected lock owner pid in error, got %q", err.Error())
	}

	if err := first.Release(); err != nil {
		t.Fatalf("first.Release() error = %v", err)
	}

	if err := second.Acquire(500 * time.Millisecond); err != nil {
		t.Fatalf("second.Acquire() after release error = %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("second.Release() error = %v", err)
	}
}

func TestInstanceGuardOTAHandoffUnixExecStyle(t *testing.T) {
	t.Parallel()

	anchor := filepath.Join(t.TempDir(), "config.json.identity_state.json")
	oldInstance, err := NewInstanceGuard(anchor)
	if err != nil {
		t.Fatalf("NewInstanceGuard(old) error = %v", err)
	}
	if err := oldInstance.Acquire(500 * time.Millisecond); err != nil {
		t.Fatalf("oldInstance.Acquire() error = %v", err)
	}

	// Unix OTA swap uses exec-style handoff. The previous process image exits
	// and the new image must be able to reacquire the same instance ownership.
	if err := oldInstance.Release(); err != nil {
		t.Fatalf("oldInstance.Release() error = %v", err)
	}

	newInstance, err := NewInstanceGuard(anchor)
	if err != nil {
		t.Fatalf("NewInstanceGuard(new) error = %v", err)
	}
	if err := newInstance.Acquire(500 * time.Millisecond); err != nil {
		t.Fatalf("newInstance.Acquire() after exec-style handoff error = %v", err)
	}
	if err := newInstance.Release(); err != nil {
		t.Fatalf("newInstance.Release() error = %v", err)
	}
}

func TestInstanceGuardOTAHandoffWindowsStartProcessStyle(t *testing.T) {
	t.Parallel()

	anchor := filepath.Join(t.TempDir(), "config.json.identity_state.json")
	oldInstance, err := NewInstanceGuard(anchor)
	if err != nil {
		t.Fatalf("NewInstanceGuard(old) error = %v", err)
	}
	if err := oldInstance.Acquire(500 * time.Millisecond); err != nil {
		t.Fatalf("oldInstance.Acquire() error = %v", err)
	}

	newInstance, err := NewInstanceGuard(anchor)
	if err != nil {
		t.Fatalf("NewInstanceGuard(new) error = %v", err)
	}

	releaseErrCh := make(chan error, 1)
	go func() {
		time.Sleep(350 * time.Millisecond)
		releaseErrCh <- oldInstance.Release()
	}()

	// Windows OTA swap starts a new process first, then exits the old one.
	// Acquire timeout/retry must allow the new process to claim ownership after
	// the old process releases the lock.
	if err := newInstance.Acquire(2 * time.Second); err != nil {
		t.Fatalf("newInstance.Acquire() during windows-style handoff error = %v", err)
	}
	if err := <-releaseErrCh; err != nil {
		t.Fatalf("oldInstance.Release() error = %v", err)
	}
	if err := newInstance.Release(); err != nil {
		t.Fatalf("newInstance.Release() error = %v", err)
	}
}
