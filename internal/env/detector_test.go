package env

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

func TestDetectGPUWindowsFallbackFiltersVirtualAdapters(t *testing.T) {
	origCmd := execCommandContext
	origGOOS := currentGOOS
	defer func() {
		execCommandContext = origCmd
		currentGOOS = origGOOS
	}()

	currentGOOS = "windows"
	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		switch name {
		case "nvidia-smi":
			return exec.Command("/bin/sh", "-c", "exit 1")
		case "clinfo":
			return exec.Command("/bin/sh", "-c", "exit 1")
		case "powershell":
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\n\"PNPDeviceID\",\"AdapterCompatibility\",\"Name\",\"AdapterRAM\"\n\"ROOT\\DISPLAY\\0000\",\"Microsoft\",\"OrayIddDriver Device\",\"0\"\n\"PCI\\VEN_1002&DEV_66AF&SUBSYS_0B0E1002&REV_C1\",\"Advanced Micro Devices, Inc.\",\"AMD Radeon Pro VII\",\"17163091968\"\n\"PCI\\VEN_1A03&DEV_2000&SUBSYS_20001A03&REV_41\",\"ASPEED Technology, Inc.\",\"ASPEED Graphics Family(WDDM)\",\"16777216\"\nEOF")
		default:
			return exec.Command("/bin/sh", "-c", "exit 1")
		}
	}

	gpus, err := DetectGPU(context.Background())
	if err != nil {
		t.Fatalf("DetectGPU() error = %v", err)
	}
	if len(gpus) != 1 {
		t.Fatalf("expected one physical gpu after filtering virtual adapters, got %d (%+v)", len(gpus), gpus)
	}
	if gpus[0].Name != "AMD Radeon Pro VII" {
		t.Fatalf("expected AMD Radeon Pro VII, got %+v", gpus[0])
	}
	if gpus[0].Backend != "windows-pnp" {
		t.Fatalf("expected windows-pnp backend, got %+v", gpus[0])
	}
	if gpus[0].MemoryTotalMB <= 0 {
		t.Fatalf("expected MemoryTotalMB from AdapterRAM, got %+v", gpus[0])
	}
}

func TestDetectGPUWindowsFallbackReturnsJoinedErrorWhenNoPhysicalGPU(t *testing.T) {
	origCmd := execCommandContext
	origGOOS := currentGOOS
	defer func() {
		execCommandContext = origCmd
		currentGOOS = origGOOS
	}()

	currentGOOS = "windows"
	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		switch name {
		case "nvidia-smi", "clinfo":
			return exec.Command("/bin/sh", "-c", "exit 1")
		case "powershell":
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\n\"PNPDeviceID\",\"AdapterCompatibility\",\"Name\",\"AdapterRAM\"\n\"ROOT\\DISPLAY\\0000\",\"Microsoft\",\"OrayIddDriver Device\",\"0\"\nEOF")
		default:
			return exec.Command("/bin/sh", "-c", "exit 1")
		}
	}

	_, err := DetectGPU(context.Background())
	if err == nil {
		t.Fatalf("expected error when only virtual adapters are present")
	}
	if !strings.Contains(err.Error(), "windows_pnp") {
		t.Fatalf("expected joined windows_pnp error, got %v", err)
	}
}

func TestParseWindowsAdapterRAMMB(t *testing.T) {
	if got := parseWindowsAdapterRAMMB("17163091968"); got != 16368 {
		t.Fatalf("expected MB conversion from raw bytes, got %d", got)
	}
	if got := parseWindowsAdapterRAMMB(`"25,769,803,776"`); got != 24576 {
		t.Fatalf("expected MB conversion from comma-delimited bytes, got %d", got)
	}
	if got := parseWindowsAdapterRAMMB("0"); got != 0 {
		t.Fatalf("expected zero to remain zero, got %d", got)
	}
}
