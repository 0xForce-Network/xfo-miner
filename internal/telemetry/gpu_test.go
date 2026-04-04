package telemetry

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func TestScanGPUsFailsWhenOpenCLUnavailable(t *testing.T) {
	origCmd := execCommandContext
	defer func() {
		execCommandContext = origCmd
	}()

	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		if name == "clinfo" {
			return exec.Command("/bin/sh", "-c", "exit 1")
		}
		return exec.Command("/bin/sh", "-c", "exit 1")
	}

	_, err := ScanGPUs()
	if err == nil {
		t.Fatalf("expected ScanGPUs() error when clinfo is missing")
	}
	if !strings.Contains(err.Error(), "OpenCL runtime not detected") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScanGPUsUsesOpenCLWhenNvidiaSMIMissing(t *testing.T) {
	origCmd := execCommandContext
	origLookPath := lookPath
	defer func() {
		execCommandContext = origCmd
		lookPath = origLookPath
	}()

	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		if name == "clinfo" {
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\nDevice Name : RTX 4090\nDevice UUID : 1234-ABCD\nVendor ID : 10DE\nDevice ID : 2684\nPCI bus info : 0000:01:00.0\nEOF")
		}
		return exec.Command("/bin/sh", "-c", "exit 1")
	}
	lookPath = func(_ string) (string, error) { return "", errors.New("not found") }

	devices, err := ScanGPUs()
	if err != nil {
		t.Fatalf("ScanGPUs() error = %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("expected one device, got %d", len(devices))
	}
	if devices[0].GPUUUID == "" || devices[0].UUIDSource != "opencl_uuid_khr" {
		t.Fatalf("expected opencl identity fields, got %+v", devices[0])
	}
}
