package env

import "testing"

func TestParseBenchmarkOutput(t *testing.T) {
	t.Parallel()

	output := `hashcat (v6.2.6) starting in benchmark mode
Device #1: NVIDIA GeForce RTX 4090
Speed.#1.........:  950.5 MH/s (95.05ms) @ Accel:32 Loops:1024 Thr:64 Vec:1
Speed.#2.........:  100.0 kH/s (1.00ms) @ Accel:32 Loops:1024 Thr:64 Vec:1`

	result, err := parseBenchmarkOutput(output)
	if err != nil {
		t.Fatalf("parseBenchmarkOutput() error = %v", err)
	}

	const expectedKHs = 950600.0
	if result.SpeedKHs != expectedKHs {
		t.Fatalf("unexpected SpeedKHs: got %v want %v", result.SpeedKHs, expectedKHs)
	}
	if result.GPUName == "" {
		t.Fatalf("expected GPUName to be parsed")
	}
}

func TestParseBenchmarkOutputMissingSpeed(t *testing.T) {
	t.Parallel()

	_, err := parseBenchmarkOutput("hashcat benchmark without speed")
	if err == nil {
		t.Fatalf("expected error when speed lines are missing")
	}
}

func TestParseBenchmarkOutputMachineReadable(t *testing.T) {
	t.Parallel()

	output := `version: v6.2.5
option: --optimized-kernel-enable
1:22000:-1:-1:53.63:149901
Started: Sun Feb 22 04:44:09 2026
Stopped: Sun Feb 22 04:44:12 2026`

	result, err := parseBenchmarkOutput(output)
	if err != nil {
		t.Fatalf("parseBenchmarkOutput() error = %v", err)
	}

	const expectedKHs = 149.901
	if result.SpeedKHs != expectedKHs {
		t.Fatalf("unexpected SpeedKHs: got %v want %v", result.SpeedKHs, expectedKHs)
	}
}
