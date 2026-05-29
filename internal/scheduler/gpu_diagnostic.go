package scheduler

import (
	"bytes"
	"context"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/telemetry"
)

const gpuDiagnosticCommandTimeout = 8 * time.Second

func (s *Scheduler) handleGPUDiagnosticRequest(ctx context.Context, req *pool.GPUDiagnosticRequestMessage) {
	requestID := strings.TrimSpace(req.RequestID)
	if requestID == "" {
		requestID = "gpu_diag_missing_request_id"
	}
	report, status, summary := s.buildGPUDiagnosticReport(ctx, req)
	msg := &pool.GPUDiagnosticReportMessage{
		Type:         "gpu_diagnostic_report",
		RequestID:    requestID,
		Status:       status,
		Report:       report,
		ErrorSummary: summary,
	}
	if err := s.poolClient.SendGPUDiagnosticReport(msg); err != nil {
		s.logger.Error("failed to send GPU diagnostic report", "request_id", requestID, "error", err)
	}
}

func (s *Scheduler) buildGPUDiagnosticReport(ctx context.Context, req *pool.GPUDiagnosticRequestMessage) (map[string]any, string, string) {
	if ctx == nil {
		ctx = context.Background()
	}
	requestID := strings.TrimSpace(req.RequestID)
	report := map[string]any{
		"request_id":   requestID,
		"reason":       strings.TrimSpace(req.Reason),
		"requested_at": req.RequestedAt,
		"generated_at": time.Now().Unix(),
		"miner": map[string]any{
			"version":     strings.TrimSpace(s.version),
			"node_id":     strings.TrimSpace(s.cfg.NodeID),
			"worker_name": strings.TrimSpace(s.cfg.WorkerName),
			"os":          runtime.GOOS + "-" + runtime.GOARCH,
			"run_mode":    strings.TrimSpace(s.capabilities.RunMode),
		},
		"capabilities": map[string]any{
			"has_gpu":         s.capabilities.HasGPU,
			"gpu_count":       len(s.capabilities.GPUs),
			"has_hashcat":     s.capabilities.HasHashcat,
			"hashcat_version": s.capabilities.HashcatVersion,
			"has_xmrig":       s.capabilities.HasXMRig,
			"xmrig_version":   s.capabilities.XMRigVersion,
			"has_docker":      s.capabilities.HasDocker,
			"ai_ready":        s.capabilities.AIReady,
			"benchmark_khs":   s.capabilities.BenchmarkKHs,
		},
		"startup_gpus": s.capabilities.GPUs,
		"commands":     map[string]any{},
	}

	if devices, err := telemetry.ScanGPUs(); err == nil {
		report["scan_gpus"] = map[string]any{"ok": true, "devices": devices}
	} else {
		report["scan_gpus"] = map[string]any{"ok": false, "error": safeDiagnosticText(err.Error(), 512)}
	}

	commands := report["commands"].(map[string]any)
	commands["nvidia_smi_query"] = runDiagnosticCommand(ctx, "nvidia-smi", "--query-gpu=index,name,memory.total,driver_version,pci.bus_id", "--format=csv,noheader")
	commands["nvidia_smi_list_gpus"] = runDiagnosticCommand(ctx, "nvidia-smi", "-L")
	commands["clinfo"] = runDiagnosticCommand(ctx, "clinfo")
	commands["hashcat_version"] = runDiagnosticCommand(ctx, s.cfg.HashcatPath, "--version")
	commands["hashcat_info"] = runDiagnosticCommand(ctx, s.cfg.HashcatPath, "-I")

	return report, "ok", ""
}

func runDiagnosticCommand(parent context.Context, name string, args ...string) map[string]any {
	name = strings.TrimSpace(name)
	if name == "" {
		return map[string]any{"ok": false, "error": "empty_command"}
	}
	ctx, cancel := context.WithTimeout(parent, gpuDiagnosticCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := map[string]any{
		"command": name,
		"args":    args,
		"stdout":  safeDiagnosticText(out.String(), 8192),
		"stderr":  safeDiagnosticText(stderr.String(), 4096),
	}
	if ctx.Err() == context.DeadlineExceeded {
		result["ok"] = false
		result["error"] = "timeout"
		return result
	}
	if err != nil {
		result["ok"] = false
		result["error"] = safeDiagnosticText(err.Error(), 512)
		return result
	}
	result["ok"] = true
	return result
}

func safeDiagnosticText(text string, maxLen int) string {
	cleaned := strings.ReplaceAll(StringRedact(text), "\x00", "")
	return truncateForLog(cleaned, maxLen)
}

func StringRedact(text string) string {
	cleaned := strings.TrimSpace(text)
	if cleaned == "" {
		return ""
	}
	cleaned = strings.ReplaceAll(cleaned, "\r", "")
	return cleaned
}
