package debuglog

import (
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type DeviceSummary struct {
	GPUModel          string  `json:"gpu_model"`
	GPUUUID           string  `json:"gpu_uuid,omitempty"`
	UUIDSource        string  `json:"uuid_source,omitempty"`
	DeviceFingerprint string  `json:"device_fingerprint,omitempty"`
	PCIBusID          string  `json:"pci_bus_id,omitempty"`
	VendorID          string  `json:"vendor_id,omitempty"`
	DeviceID          string  `json:"device_id,omitempty"`
	DeviceIndex       int     `json:"device_index"`
	VRAMGB            float64 `json:"vram_gb,omitempty"`
	VRAMBytes         int64   `json:"vram_bytes,omitempty"`
	IdentityStable    bool    `json:"identity_stable"`
	IsVirtual         bool    `json:"is_virtual"`
	VirtualReason     string  `json:"virtual_reason,omitempty"`
}

type PayloadSummary struct {
	WorkerName           string          `json:"worker_name"`
	MinerVersion         string          `json:"miner_version"`
	DevicesCount         int             `json:"devices_count"`
	PrimaryModel         string          `json:"primary_model"`
	PrimaryUUIDSource    string          `json:"primary_uuid_source"`
	HasOpenCLUUIDKHR     bool            `json:"has_opencl_uuid_khr"`
	FilteredVirtualCount int             `json:"filtered_virtual_count"`
	SubmittedDevices     []DeviceSummary `json:"submitted_devices,omitempty"`
}

var state struct {
	mu                 sync.RWMutex
	logger             *slog.Logger
	file               *os.File
	verbose            bool
	path               string
	minerID            string
	hashcatVisibleList []string
	loginSummary       *PayloadSummary
	telemetrySummary   *PayloadSummary
}

func Enable(path string, verbose bool) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(trimmed), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(trimmed, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.file != nil {
		_ = state.file.Close()
	}
	state.file = f
	state.logger = slog.New(slog.NewJSONHandler(f, &slog.HandlerOptions{Level: slog.LevelInfo}))
	state.verbose = verbose
	state.path = trimmed
	state.minerID = ""
	state.hashcatVisibleList = nil
	state.loginSummary = nil
	state.telemetrySummary = nil
	return nil
}

func Close() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.file == nil {
		return nil
	}
	err := state.file.Close()
	state.file = nil
	state.logger = nil
	state.verbose = false
	state.path = ""
	state.minerID = ""
	state.hashcatVisibleList = nil
	state.loginSummary = nil
	state.telemetrySummary = nil
	return err
}

func Enabled() bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.logger != nil
}

func Verbose() bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.logger != nil && state.verbose
}

func Path() string {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.path
}

func SetMinerID(minerID string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.minerID = strings.TrimSpace(minerID)
	emitGPUIdentitySummaryLocked()
}

func CurrentMinerID() string {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.minerID
}

func SetHashcatVisibleModels(models []string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.hashcatVisibleList = append([]string(nil), models...)
	emitGPUIdentitySummaryLocked()
}

func CurrentHashcatVisibleModels() []string {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return append([]string(nil), state.hashcatVisibleList...)
}

func UpdateLoginPayloadSummary(summary PayloadSummary) {
	state.mu.Lock()
	defer state.mu.Unlock()
	cp := summary
	cp.SubmittedDevices = append([]DeviceSummary(nil), summary.SubmittedDevices...)
	state.loginSummary = &cp
	emitGPUIdentitySummaryLocked()
}

func UpdateTelemetryPayloadSummary(summary PayloadSummary) {
	state.mu.Lock()
	defer state.mu.Unlock()
	cp := summary
	cp.SubmittedDevices = append([]DeviceSummary(nil), summary.SubmittedDevices...)
	state.telemetrySummary = &cp
	emitGPUIdentitySummaryLocked()
}

func Log(event string, attrs ...any) {
	state.mu.RLock()
	logger := state.logger
	state.mu.RUnlock()
	if logger == nil {
		return
	}
	base := []any{"debug_event", strings.TrimSpace(event)}
	logger.Info("debug", append(base, attrs...)...)
}

func LogVerbose(event string, attrs ...any) {
	if !Verbose() {
		return
	}
	Log(event, attrs...)
}

func LogPayloadDiagnostics(kind string, workerName string, minerVersion string, devices []DeviceSummary) PayloadSummary {
	annotated := annotateDevices(devices)
	summary := buildPayloadSummary(workerName, minerVersion, annotated)
	Log(kind+"_payload",
		"worker_name", summary.WorkerName,
		"miner_version", summary.MinerVersion,
		"devices_count", summary.DevicesCount,
		"primary_gpu_model", summary.PrimaryModel,
		"primary_uuid_source", summary.PrimaryUUIDSource,
		"has_opencl_uuid_khr", summary.HasOpenCLUUIDKHR,
		"filtered_virtual_count", summary.FilteredVirtualCount,
		"submitted_devices", summary.SubmittedDevices,
	)
	if kind == "login" {
		UpdateLoginPayloadSummary(summary)
	} else if kind == "telemetry_l2" {
		UpdateTelemetryPayloadSummary(summary)
	}
	if Verbose() {
		filtered := make([]DeviceSummary, 0)
		afterFilter := make([]DeviceSummary, 0)
		for _, device := range annotated {
			if device.IsVirtual {
				filtered = append(filtered, device)
				continue
			}
			afterFilter = append(afterFilter, device)
		}
		sortedCandidates := append([]DeviceSummary(nil), afterFilter...)
		if len(sortedCandidates) == 0 {
			sortedCandidates = append([]DeviceSummary(nil), annotated...)
		}
		sort.SliceStable(sortedCandidates, func(i, j int) bool {
			return candidateLess(sortedCandidates[i], sortedCandidates[j])
		})
		finalPrimary := DeviceSummary{}
		if len(sortedCandidates) > 0 {
			finalPrimary = sortedCandidates[0]
		}
		payloadPrimary := DeviceSummary{}
		if len(annotated) > 0 {
			payloadPrimary = annotated[0]
		}
		LogVerbose(kind+"_candidate_analysis",
			"candidates_before_filter", annotated,
			"filtered_devices", filtered,
			"candidates_after_filter", afterFilter,
			"candidates_sorted", sortedCandidates,
			"final_primary_candidate", finalPrimary,
			"payload_primary_device", payloadPrimary,
		)
	}
	return summary
}

func emitGPUIdentitySummaryLocked() {
	if state.logger == nil {
		return
	}
	workerName := ""
	minerVersion := ""
	primaryModelLogin := ""
	primaryUUIDSourceLogin := ""
	primaryModelTelemetry := ""
	primaryUUIDSourceTelemetry := ""
	hasOpenCLUUIDKHR := false
	devicesCount := 0
	filteredVirtualCount := 0
	submittedDevices := make([]DeviceSummary, 0)
	if state.loginSummary != nil {
		workerName = state.loginSummary.WorkerName
		minerVersion = state.loginSummary.MinerVersion
		primaryModelLogin = state.loginSummary.PrimaryModel
		primaryUUIDSourceLogin = state.loginSummary.PrimaryUUIDSource
		hasOpenCLUUIDKHR = hasOpenCLUUIDKHR || state.loginSummary.HasOpenCLUUIDKHR
		devicesCount = state.loginSummary.DevicesCount
		filteredVirtualCount = state.loginSummary.FilteredVirtualCount
		submittedDevices = append(submittedDevices, state.loginSummary.SubmittedDevices...)
	}
	if state.telemetrySummary != nil {
		if workerName == "" {
			workerName = state.telemetrySummary.WorkerName
		}
		if minerVersion == "" {
			minerVersion = state.telemetrySummary.MinerVersion
		}
		primaryModelTelemetry = state.telemetrySummary.PrimaryModel
		primaryUUIDSourceTelemetry = state.telemetrySummary.PrimaryUUIDSource
		hasOpenCLUUIDKHR = hasOpenCLUUIDKHR || state.telemetrySummary.HasOpenCLUUIDKHR
		if state.telemetrySummary.DevicesCount > 0 {
			devicesCount = state.telemetrySummary.DevicesCount
		}
		if state.telemetrySummary.FilteredVirtualCount > filteredVirtualCount {
			filteredVirtualCount = state.telemetrySummary.FilteredVirtualCount
		}
		if len(state.telemetrySummary.SubmittedDevices) > 0 {
			submittedDevices = append([]DeviceSummary(nil), state.telemetrySummary.SubmittedDevices...)
		}
	}
	state.logger.Info("debug",
		"debug_event", "gpu_identity_summary",
		"worker_name", workerName,
		"miner_version", minerVersion,
		"miner_id", state.minerID,
		"devices_count", devicesCount,
		"primary_model_login", primaryModelLogin,
		"primary_uuid_source_login", primaryUUIDSourceLogin,
		"primary_model_telemetry", primaryModelTelemetry,
		"primary_uuid_source_telemetry", primaryUUIDSourceTelemetry,
		"has_opencl_uuid_khr", hasOpenCLUUIDKHR,
		"filtered_virtual_count", filteredVirtualCount,
		"hashcat_visible_models", state.hashcatVisibleList,
		"submitted_devices", submittedDevices,
	)
}

func ClassifyVirtualAdapter(model string, busID string) (bool, string) {
	joined := strings.ToLower(strings.TrimSpace(model) + " " + strings.TrimSpace(busID))
	patterns := []struct {
		needle string
		reason string
	}{
		{needle: "orayidddriver", reason: "OrayIddDriver matched virtual adapter pattern"},
		{needle: "oray idd", reason: "OrayIddDriver matched virtual adapter pattern"},
		{needle: `root\\display`, reason: `ROOT\\DISPLAY matched virtual adapter pattern`},
		{needle: "aspeed graphics family", reason: "ASPEED graphics family matched virtual adapter pattern"},
		{needle: "indirect display", reason: "indirect display matched virtual adapter pattern"},
		{needle: "remote display", reason: "remote display matched virtual adapter pattern"},
		{needle: "virtual display", reason: "virtual display matched virtual adapter pattern"},
		{needle: "microsoft basic display", reason: "basic display adapter matched virtual adapter pattern"},
		{needle: "parsec", reason: "Parsec virtual display matched virtual adapter pattern"},
	}
	for _, pattern := range patterns {
		if strings.Contains(joined, pattern.needle) {
			return true, pattern.reason
		}
	}
	return false, ""
}

func annotateDevices(devices []DeviceSummary) []DeviceSummary {
	annotated := make([]DeviceSummary, 0, len(devices))
	for _, device := range devices {
		cp := device
		isVirtual, reason := ClassifyVirtualAdapter(cp.GPUModel, cp.PCIBusID)
		cp.IsVirtual = isVirtual
		if cp.VirtualReason == "" {
			cp.VirtualReason = reason
		}
		cp.IdentityStable = strings.TrimSpace(cp.GPUUUID) != "" || strings.TrimSpace(cp.DeviceFingerprint) != ""
		annotated = append(annotated, cp)
	}
	return annotated
}

func buildPayloadSummary(workerName string, minerVersion string, devices []DeviceSummary) PayloadSummary {
	summary := PayloadSummary{
		WorkerName:       strings.TrimSpace(workerName),
		MinerVersion:     strings.TrimSpace(minerVersion),
		DevicesCount:     len(devices),
		SubmittedDevices: append([]DeviceSummary(nil), devices...),
	}
	for _, device := range devices {
		if device.UUIDSource == "opencl_uuid_khr" {
			summary.HasOpenCLUUIDKHR = true
		}
		if device.IsVirtual {
			summary.FilteredVirtualCount++
		}
	}
	if len(devices) > 0 {
		summary.PrimaryModel = devices[0].GPUModel
		summary.PrimaryUUIDSource = devices[0].UUIDSource
	}
	return summary
}

func candidateLess(a DeviceSummary, b DeviceSummary) bool {
	if hashcatVisible(a) != hashcatVisible(b) {
		return hashcatVisible(a)
	}
	if openclPreferred(a) != openclPreferred(b) {
		return openclPreferred(a)
	}
	if nonVirtualPreferred(a) != nonVirtualPreferred(b) {
		return nonVirtualPreferred(a)
	}
	if a.VRAMBytes != b.VRAMBytes {
		return a.VRAMBytes > b.VRAMBytes
	}
	if a.VRAMGB != b.VRAMGB {
		return a.VRAMGB > b.VRAMGB
	}
	if hasPCIBus(a) != hasPCIBus(b) {
		return hasPCIBus(a)
	}
	return a.DeviceIndex < b.DeviceIndex
}

func hashcatVisible(device DeviceSummary) bool {
	model := strings.ToLower(strings.TrimSpace(device.GPUModel))
	for _, visible := range CurrentHashcatVisibleModels() {
		if strings.Contains(strings.ToLower(visible), model) || strings.Contains(model, strings.ToLower(visible)) {
			return true
		}
	}
	return false
}

func openclPreferred(device DeviceSummary) bool {
	return strings.TrimSpace(device.UUIDSource) == "opencl_uuid_khr"
}

func nonVirtualPreferred(device DeviceSummary) bool {
	return !device.IsVirtual
}

func hasPCIBus(device DeviceSummary) bool {
	return strings.TrimSpace(device.PCIBusID) != ""
}