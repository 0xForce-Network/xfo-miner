//go:build !windows

package process

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStopDetachedTerminatesProcessGroupChildren(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "child-pid")
	detached, err := StartDetached("sh", []string{"-c", "sleep 60 & echo $! > " + markerPath + "; wait"})
	if err != nil {
		t.Fatalf("StartDetached() error = %v", err)
	}

	childPID := waitForPIDFile(t, markerPath)
	if childPID <= 0 {
		t.Fatalf("expected child pid")
	}
	if !IsProcessRunning(childPID) {
		t.Fatalf("expected child process %d to be running before stop", childPID)
	}

	if err := StopDetached(detached.Pid, 1); err != nil {
		t.Fatalf("StopDetached() error = %v", err)
	}
	if isLiveProcess(childPID) {
		t.Fatalf("expected process-group child %d to be terminated", childPID)
	}
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := parsePID(string(raw))
			if parseErr != nil {
				t.Fatalf("parse child pid: %v", parseErr)
			}
			return pid
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for pid file %s", path)
	return 0
}

func parsePID(raw string) (int, error) {
	return strconv.Atoi(strings.TrimSpace(raw))
}

func isLiveProcess(pid int) bool {
	if pid <= 0 {
		return false
	}
	_, state, ok := readLinuxProcStat(pid)
	if ok && state == detachedZombieState {
		return false
	}
	return IsProcessRunning(pid)
}
