package env

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const commandTimeout = 15 * time.Second

type GPUInfo struct {
	Available     bool
	Name          string
	MemoryTotalMB int
	DriverVersion string
	Backend       string
}

type HashcatInfo struct {
	Available bool
	Version   string
}

type DockerInfo struct {
	Available bool
	Version   string
}

type XMRigInfo struct {
	Available bool
	Version   string
}

func DetectGPU(ctx context.Context) ([]GPUInfo, error) {
	gpus, err := detectNvidia(ctx)
	if err == nil && len(gpus) > 0 {
		return gpus, nil
	}

	amdGPUs, amdErr := detectCLInfo(ctx)
	if amdErr == nil && len(amdGPUs) > 0 {
		return amdGPUs, nil
	}

	if err != nil && amdErr != nil {
		return []GPUInfo{}, fmt.Errorf("gpu detection failed: nvidia-smi: %w; clinfo: %w", err, amdErr)
	}

	return []GPUInfo{}, nil
}

func detectNvidia(ctx context.Context) ([]GPUInfo, error) {
	output, err := runCommand(ctx, commandTimeout, "nvidia-smi", "--query-gpu=name,memory.total,driver_version", "--format=csv,noheader")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	gpus := make([]GPUInfo, 0, len(lines))
	for _, line := range lines {
		parts := strings.Split(line, ",")
		if len(parts) < 3 {
			continue
		}
		memoryMB := parseMemoryMB(parts[1])
		gpus = append(gpus, GPUInfo{
			Available:     true,
			Name:          strings.TrimSpace(parts[0]),
			MemoryTotalMB: memoryMB,
			DriverVersion: strings.TrimSpace(parts[2]),
			Backend:       "nvidia-smi",
		})
	}

	if len(gpus) == 0 {
		return nil, fmt.Errorf("nvidia-smi returned no GPU rows")
	}

	return gpus, nil
}

func detectCLInfo(ctx context.Context) ([]GPUInfo, error) {
	output, err := runCommand(ctx, commandTimeout, "clinfo")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(output, "\n")
	gpus := make([]GPUInfo, 0)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)
		if strings.Contains(lower, "device name") {
			name := trimmed
			if idx := strings.Index(trimmed, ":"); idx >= 0 {
				name = strings.TrimSpace(trimmed[idx+1:])
			}
			if name != "" {
				gpus = append(gpus, GPUInfo{Available: true, Name: name, Backend: "clinfo"})
			}
		}
	}

	if len(gpus) == 0 {
		return nil, fmt.Errorf("clinfo returned no GPU device names")
	}

	return gpus, nil
}

func DetectHashcat(ctx context.Context, hashcatPath string) (*HashcatInfo, error) {
	if strings.TrimSpace(hashcatPath) == "" {
		hashcatPath = "hashcat"
	}

	output, err := runCommand(ctx, commandTimeout, hashcatPath, "--version")
	if err != nil {
		return &HashcatInfo{Available: false}, err
	}

	version := strings.TrimSpace(output)
	if i := strings.Index(version, "\n"); i >= 0 {
		version = version[:i]
	}

	return &HashcatInfo{Available: true, Version: version}, nil
}

func DetectDocker(ctx context.Context) (*DockerInfo, error) {
	output, err := runCommand(ctx, commandTimeout, "docker", "info", "--format", "{{.ServerVersion}}")
	if err != nil {
		return &DockerInfo{Available: false}, err
	}

	return &DockerInfo{Available: true, Version: strings.TrimSpace(output)}, nil
}

func DetectXMRig(ctx context.Context) (*XMRigInfo, error) {
	output, err := runCommand(ctx, commandTimeout, "xmrig", "--version")
	if err != nil {
		return &XMRigInfo{Available: false}, err
	}

	version := strings.TrimSpace(output)
	if i := strings.Index(version, "\n"); i >= 0 {
		version = version[:i]
	}

	return &XMRigInfo{Available: true, Version: version}, nil
}

func DetectNvidiaDocker(ctx context.Context) bool {
	_, err := runCommand(ctx, 45*time.Second, "docker", "run", "--rm", "--gpus", "all", "nvidia/cuda:12.1.0-base-ubuntu22.04", "nvidia-smi")
	return err == nil
}

func runCommand(ctx context.Context, timeout time.Duration, name string, args ...string) (string, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, name, args...)
	output, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(output))
	if err != nil {
		if out == "" {
			return "", fmt.Errorf("%s failed: %w", name, err)
		}
		return "", fmt.Errorf("%s failed: %w: %s", name, err, out)
	}

	return out, nil
}

func parseMemoryMB(raw string) int {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return 0
	}
	value, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0
	}
	return value
}
