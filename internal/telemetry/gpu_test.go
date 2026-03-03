package telemetry

import (
	"context"
	"errors"
	"os/exec"
	"testing"
)

func TestScanGPUsGracefulWhenMissingNvidiaSMI(t *testing.T) {
	origCmd := execCommandContext
	origLookPath := lookPath
	defer func() {
		execCommandContext = origCmd
		lookPath = origLookPath
	}()

	execCommandContext = func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		return exec.Command("/bin/sh", "-c", "exit 1")
	}
	lookPath = func(_ string) (string, error) { return "", errors.New("not found") }

	devices, err := ScanGPUs()
	if err != nil {
		t.Fatalf("ScanGPUs() error = %v", err)
	}
	if len(devices) != 0 {
		t.Fatalf("expected 0 devices when nvidia-smi missing, got %d", len(devices))
	}
}
