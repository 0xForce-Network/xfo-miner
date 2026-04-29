package env

import (
	"context"
	"errors"

	"github.com/0xforce/xfo-miner/internal/debuglog"
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
	IsRoot         bool
	RunMode        string
}

var detectGPUFn = DetectGPU
var detectHashcatFn = DetectHashcat
var detectXMRigFn = DetectXMRig
var detectDockerFn = DetectDocker
var runHashcatBenchmarkFn = RunHashcatBenchmark
var detectNvidiaDockerFn = DetectNvidiaDocker

func ProbeAll(ctx context.Context, hashcatPath string, xmrigPath string) (*SystemCapabilities, error) {
	caps := &SystemCapabilities{RunMode: RunModeCPUOnly}
	warnings := make([]error, 0)
	caps.IsRoot = CheckRoot()

	gpus, gpuErr := detectGPUFn(ctx)
	if gpuErr != nil {
		warnings = append(warnings, gpuErr)
	}
	if len(gpus) > 0 {
		caps.HasGPU = true
		caps.GPUs = gpus
	}

	hashcatInfo, hashcatErr := detectHashcatFn(ctx, hashcatPath)
	if hashcatErr != nil {
		warnings = append(warnings, hashcatErr)
	}
	if hashcatInfo != nil {
		caps.HasHashcat = hashcatInfo.Available
		caps.HashcatVersion = hashcatInfo.Version
		debuglog.Log("hashcat_probe_summary",
			"hashcat_path", hashcatPath,
			"available", hashcatInfo.Available,
			"version", hashcatInfo.Version,
		)
	}

	xmrigInfo, xmrigErr := detectXMRigFn(ctx, xmrigPath)
	if xmrigErr != nil {
		warnings = append(warnings, xmrigErr)
	}
	if xmrigInfo != nil {
		caps.HasXMRig = xmrigInfo.Available
		caps.XMRigVersion = xmrigInfo.Version
	}

	dockerInfo, dockerErr := detectDockerFn(ctx)
	if dockerErr != nil {
		warnings = append(warnings, dockerErr)
	}
	if dockerInfo != nil {
		caps.HasDocker = dockerInfo.Available
	}

	if caps.HasGPU && caps.HasHashcat {
		benchmark, benchErr := runHashcatBenchmarkFn(ctx, hashcatPath)
		if benchErr != nil {
			warnings = append(warnings, benchErr)
			debuglog.Log("hashcat_benchmark_failed", "hashcat_path", hashcatPath, "error", benchErr)
		} else {
			caps.BenchmarkKHs = benchmark.SpeedKHs
			if benchmark.GPUName != "" {
				debuglog.SetHashcatVisibleModels([]string{benchmark.GPUName})
			}
			debuglog.Log("hashcat_benchmark_summary",
				"hashcat_path", hashcatPath,
				"gpu_name", benchmark.GPUName,
				"speed_khs", benchmark.SpeedKHs,
			)
		}
	}

	if caps.HasGPU && caps.HasDocker {
		caps.AIReady = detectNvidiaDockerFn(ctx)
	}

	switch {
	case caps.HasGPU && caps.HasHashcat && caps.HasDocker && caps.AIReady:
		caps.RunMode = RunModeGPUFull
	case caps.HasGPU && caps.HasHashcat:
		caps.RunMode = RunModeGPUHashcatOnly
	default:
		caps.RunMode = RunModeCPUOnly
	}
	debuglog.Log("system_capabilities_summary",
		"has_gpu", caps.HasGPU,
		"gpu_count", len(caps.GPUs),
		"has_hashcat", caps.HasHashcat,
		"hashcat_version", caps.HashcatVersion,
		"has_xmrig", caps.HasXMRig,
		"xmrig_version", caps.XMRigVersion,
		"has_docker", caps.HasDocker,
		"ai_ready", caps.AIReady,
		"benchmark_khs", caps.BenchmarkKHs,
		"run_mode", caps.RunMode,
	)

	return caps, errors.Join(warnings...)
}
