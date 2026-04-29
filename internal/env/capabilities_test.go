package env

import (
	"context"
	"testing"
)

func TestProbeAllPromotesWindowsFallbackGPUToHashcatOnlyMode(t *testing.T) {
	origDetectGPU := detectGPUFn
	origDetectHashcat := detectHashcatFn
	origDetectXMRig := detectXMRigFn
	origDetectDocker := detectDockerFn
	origBenchmark := runHashcatBenchmarkFn
	origDetectNvidiaDocker := detectNvidiaDockerFn
	defer func() {
		detectGPUFn = origDetectGPU
		detectHashcatFn = origDetectHashcat
		detectXMRigFn = origDetectXMRig
		detectDockerFn = origDetectDocker
		runHashcatBenchmarkFn = origBenchmark
		detectNvidiaDockerFn = origDetectNvidiaDocker
	}()

	detectGPUFn = func(context.Context) ([]GPUInfo, error) {
		return []GPUInfo{{Available: true, Name: "AMD Radeon Pro VII", Backend: "windows-pnp"}}, nil
	}
	detectHashcatFn = func(context.Context, string) (*HashcatInfo, error) {
		return &HashcatInfo{Available: true, Version: "v6.2.5"}, nil
	}
	detectXMRigFn = func(context.Context, string) (*XMRigInfo, error) {
		return &XMRigInfo{Available: true, Version: "6.21.0"}, nil
	}
	detectDockerFn = func(context.Context) (*DockerInfo, error) {
		return &DockerInfo{Available: false}, nil
	}
	runHashcatBenchmarkFn = func(context.Context, string) (*BenchmarkResult, error) {
		return &BenchmarkResult{GPUName: "AMD Radeon Pro VII", SpeedKHs: 149.9}, nil
	}
	detectNvidiaDockerFn = func(context.Context) bool { return false }

	caps, err := ProbeAll(context.Background(), "hashcat.exe", "xmrig.exe")
	if err != nil {
		t.Fatalf("ProbeAll() error = %v", err)
	}
	if !caps.HasGPU || len(caps.GPUs) != 1 {
		t.Fatalf("expected one detected gpu, got %+v", caps)
	}
	if !caps.HasHashcat {
		t.Fatalf("expected hashcat to be available, got %+v", caps)
	}
	if caps.RunMode != RunModeGPUHashcatOnly {
		t.Fatalf("expected run mode %s, got %+v", RunModeGPUHashcatOnly, caps)
	}
	if caps.BenchmarkKHs != 149.9 {
		t.Fatalf("expected benchmark to be recorded, got %+v", caps)
	}
}