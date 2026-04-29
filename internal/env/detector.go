package env

import (
	"context"
	"encoding/csv"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/0xforce/xfo-miner/internal/debuglog"
)

const commandTimeout = 15 * time.Second

var currentGOOS = runtime.GOOS
var execCommandContext = exec.CommandContext

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

	if currentGOOS == "windows" {
		windowsGPUs, windowsErr := detectWindowsVideoControllers(ctx)
		if windowsErr == nil && len(windowsGPUs) > 0 {
			return windowsGPUs, nil
		}
		if err != nil && amdErr != nil && windowsErr != nil {
			return []GPUInfo{}, fmt.Errorf("gpu detection failed: nvidia-smi: %w; clinfo: %w; windows_pnp: %w", err, amdErr, windowsErr)
		}
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

func DetectXMRig(ctx context.Context, xmrigPath string) (*XMRigInfo, error) {
	if strings.TrimSpace(xmrigPath) == "" {
		xmrigPath = "xmrig"
	}

	output, err := runCommand(ctx, commandTimeout, xmrigPath, "--version")
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

func detectWindowsVideoControllers(ctx context.Context) ([]GPUInfo, error) {
	output, err := runCommand(ctx, commandTimeout, "powershell", "-NoProfile", "-Command", "Get-CimInstance Win32_VideoController | Select-Object PNPDeviceID,AdapterCompatibility,Name,AdapterRAM | ConvertTo-Csv -NoTypeInformation")
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(strings.NewReader(output))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse windows video controller csv: %w", err)
	}
	if len(records) <= 1 {
		return nil, fmt.Errorf("windows video controller csv returned no rows")
	}

	headers := make(map[string]int)
	for i, h := range records[0] {
		headers[strings.ToLower(strings.TrimSpace(h))] = i
	}
	idxPNP, okPNP := headers["pnpdeviceid"]
	idxName, okName := headers["name"]
	idxAdapterRAM, hasAdapterRAM := headers["adapterram"]
	if !okPNP || !okName {
		return nil, fmt.Errorf("windows video controller csv missing required columns")
	}

	gpus := make([]GPUInfo, 0, len(records)-1)
	for _, row := range records[1:] {
		if idxPNP >= len(row) || idxName >= len(row) {
			continue
		}
		pnpID := strings.TrimSpace(row[idxPNP])
		name := strings.TrimSpace(row[idxName])
		if pnpID == "" || name == "" {
			continue
		}
		if isVirtual, _ := debuglog.ClassifyVirtualAdapter(name, pnpID); isVirtual {
			continue
		}
		memoryMB := 0
		if hasAdapterRAM && idxAdapterRAM < len(row) {
			memoryMB = parseWindowsAdapterRAMMB(row[idxAdapterRAM])
		}
		gpus = append(gpus, GPUInfo{Available: true, Name: name, MemoryTotalMB: memoryMB, Backend: "windows-pnp"})
	}

	if len(gpus) == 0 {
		return nil, fmt.Errorf("windows video controller probe returned no physical gpu rows")
	}

	return gpus, nil
}

func runCommand(ctx context.Context, timeout time.Duration, name string, args ...string) (string, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := execCommandContext(cmdCtx, name, args...)
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

func parseWindowsAdapterRAMMB(raw string) int {
	trimmed := strings.TrimSpace(strings.Trim(raw, `"`))
	trimmed = strings.ReplaceAll(trimmed, ",", "")
	if trimmed == "" {
		return 0
	}
	bytes, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || bytes <= 0 {
		return 0
	}
	return int(bytes / (1024 * 1024))
}
