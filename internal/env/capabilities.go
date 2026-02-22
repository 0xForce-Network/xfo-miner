package env

import (
	"context"
	"errors"
)

const (
	RunModeGPUFull        = "GPU_FULL"
	RunModeGPUHashcatOnly = "GPU_HASHCAT_ONLY"
	RunModeCPUOnly        = "CPU_ONLY"
)

type SystemCapabilities struct {
	HasGPU         bool
	GPUs           []GPUInfo
	HasHashcat     bool
	HashcatVersion string
	HasXMRig       bool
	XMRigVersion   string
	HasDocker      bool
	AIReady        bool
	BenchmarkKHs   float64
	RunMode        string
}

func ProbeAll(ctx context.Context) (*SystemCapabilities, error) {
	caps := &SystemCapabilities{RunMode: RunModeCPUOnly}
	warnings := make([]error, 0)

	gpus, gpuErr := DetectGPU(ctx)
	if gpuErr != nil {
		warnings = append(warnings, gpuErr)
	}
	if len(gpus) > 0 {
		caps.HasGPU = true
		caps.GPUs = gpus
	}

	hashcatInfo, hashcatErr := DetectHashcat(ctx)
	if hashcatErr != nil {
		warnings = append(warnings, hashcatErr)
	}
	if hashcatInfo != nil {
		caps.HasHashcat = hashcatInfo.Available
		caps.HashcatVersion = hashcatInfo.Version
	}

	xmrigInfo, xmrigErr := DetectXMRig(ctx)
	if xmrigErr != nil {
		warnings = append(warnings, xmrigErr)
	}
	if xmrigInfo != nil {
		caps.HasXMRig = xmrigInfo.Available
		caps.XMRigVersion = xmrigInfo.Version
	}

	dockerInfo, dockerErr := DetectDocker(ctx)
	if dockerErr != nil {
		warnings = append(warnings, dockerErr)
	}
	if dockerInfo != nil {
		caps.HasDocker = dockerInfo.Available
	}

	if caps.HasGPU && caps.HasHashcat {
		benchmark, benchErr := RunHashcatBenchmark(ctx)
		if benchErr != nil {
			warnings = append(warnings, benchErr)
		} else {
			caps.BenchmarkKHs = benchmark.SpeedKHs
		}
	}

	if caps.HasGPU && caps.HasDocker {
		caps.AIReady = DetectNvidiaDocker(ctx)
	}

	switch {
	case caps.HasGPU && caps.HasHashcat && caps.HasDocker && caps.AIReady:
		caps.RunMode = RunModeGPUFull
	case caps.HasGPU && caps.HasHashcat:
		caps.RunMode = RunModeGPUHashcatOnly
	default:
		caps.RunMode = RunModeCPUOnly
	}

	return caps, errors.Join(warnings...)
}
