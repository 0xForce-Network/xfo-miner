package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var execCommandContext = exec.CommandContext
var lookPath = exec.LookPath
var currentGOOS = runtime.GOOS

type GPUDevice struct {
	DeviceID          string  `json:"device_id"`
	DeviceIndex       int     `json:"device_index,omitempty"`
	VendorID          string  `json:"vendor_id,omitempty"`
	UUIDSource        string  `json:"uuid_source,omitempty"`
	GPUUUID           string  `json:"gpu_uuid,omitempty"`
	DeviceFingerprint string  `json:"device_fingerprint,omitempty"`
	GPUModel          string  `json:"gpu_model"`
	VRAMGB            float64 `json:"vram_gb"`
	Status            string  `json:"status"`
	ReputationScore   float64 `json:"reputation_score"`
	PCIBusID          string  `json:"pci_bus_id"`
	Temperature       int     `json:"temperature_c,omitempty"`
	Utilization       int     `json:"utilization_pct,omitempty"`
}

type openCLIdentity struct {
	DeviceIndex int
	DeviceName  string
	GPUUUID     string
	VendorID    string
	DeviceID    string
	PCIBusID    string
	VRAMBytes   int64
}

func ScanGPUs() ([]GPUDevice, error) {
	identities, uuidSource, err := detectGPUIdentities()
	if err != nil {
		return nil, err
	}
	if len(identities) == 0 {
		return nil, errors.New("gpu_identity_not_supported_on_platform: no compatible GPU identity source available for L2 mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := execCommandContext(ctx, "nvidia-smi", "--query-gpu=index,name,memory.total,pci.bus_id,temperature.gpu,utilization.gpu", "--format=csv,noheader,nounits")
	out, err := cmd.Output()
	if err != nil {
		if _, lookErr := lookPath("nvidia-smi"); lookErr != nil {
			devices := make([]GPUDevice, 0, len(identities))
			for _, identity := range identities {
				devices = append(devices, GPUDevice{
					DeviceID:          strconv.Itoa(identity.DeviceIndex),
					DeviceIndex:       identity.DeviceIndex,
					VendorID:          identity.VendorID,
					UUIDSource:        uuidSource,
					GPUUUID:           identity.GPUUUID,
					DeviceFingerprint: buildDeviceFingerprint(identity.VendorID, identity.DeviceID, identity.PCIBusID, identity.GPUUUID, identity.DeviceName),
					GPUModel:          identity.DeviceName,
					VRAMGB:            vramBytesToGB(identity.VRAMBytes),
					Status:            "idle",
					ReputationScore:   50.0,
					PCIBusID:          identity.PCIBusID,
				})
			}
			return devices, nil
		}
		return nil, err
	}

	identityByIndex := make(map[int]openCLIdentity, len(identities))
	for _, identity := range identities {
		identityByIndex[identity.DeviceIndex] = identity
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	devices := make([]GPUDevice, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 6 {
			continue
		}

		idx := strings.TrimSpace(parts[0])
		name := strings.TrimSpace(parts[1])
		memMB, _ := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		temp, _ := strconv.Atoi(strings.TrimSpace(parts[4]))
		util, _ := strconv.Atoi(strings.TrimSpace(parts[5]))
		idxInt, _ := strconv.Atoi(idx)
		identity, ok := identityByIndex[idxInt]
		if !ok || strings.TrimSpace(identity.GPUUUID) == "" {
			return nil, fmt.Errorf("partial_gpu_identity_detected: missing stable gpu identity for index=%d source=%s", idxInt, uuidSource)
		}

		devices = append(devices, GPUDevice{
			DeviceID:          idx,
			DeviceIndex:       idxInt,
			VendorID:          identity.VendorID,
			UUIDSource:        uuidSource,
			GPUUUID:           identity.GPUUUID,
			DeviceFingerprint: buildDeviceFingerprint(identity.VendorID, identity.DeviceID, strings.TrimSpace(parts[3]), identity.GPUUUID, name),
			GPUModel:          name,
			VRAMGB:            memMB / 1024.0,
			Status:            "idle",
			ReputationScore:   50.0,
			PCIBusID:          strings.TrimSpace(parts[3]),
			Temperature:       temp,
			Utilization:       util,
		})
	}

	return devices, nil
}

func detectGPUIdentities() ([]openCLIdentity, string, error) {
	if identities, err := detectOpenCLIdentities(); err == nil && len(identities) > 0 {
		return identities, "opencl_uuid_khr", nil
	}

	if currentGOOS == "windows" {
		identities, err := detectWindowsPNPIdentities()
		if err != nil {
			return nil, "", fmt.Errorf("gpu_identity_not_supported_on_platform: Windows GPU identity probe failed (opencl_unavailable, platform_uuid_probe_failed): %w", err)
		}
		return identities, "windows_pnp_device_id", nil
	}

	if currentGOOS == "darwin" {
		identities, err := detectMacPCIIdentities()
		if err != nil {
			return nil, "", fmt.Errorf("gpu_identity_not_supported_on_platform: macOS GPU identity probe failed (opencl_unavailable, platform_uuid_probe_failed): %w", err)
		}
		return identities, "mac_pci_fingerprint", nil
	}

	return nil, "", errors.New("opencl_unavailable: OpenCL runtime not detected. Linux L2 mode requires OpenCL with CL_DEVICE_UUID_KHR support")
}

func detectOpenCLIdentities() ([]openCLIdentity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := execCommandContext(ctx, "clinfo")
	out, err := cmd.Output()
	if err != nil {
		return nil, errors.New("OpenCL runtime not detected. L2 mode requires OpenCL with CL_DEVICE_UUID_KHR support.")
	}

	lines := strings.Split(string(out), "\n")
	identities := make([]openCLIdentity, 0)
	current := openCLIdentity{DeviceIndex: -1}

	flush := func() {
		if strings.TrimSpace(current.GPUUUID) == "" {
			return
		}
		if current.DeviceIndex < 0 {
			current.DeviceIndex = len(identities)
		}
		identities = append(identities, current)
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)
		if trimmed == "" {
			continue
		}

		if strings.Contains(lower, "device #") {
			if strings.TrimSpace(current.GPUUUID) != "" {
				flush()
				current = openCLIdentity{DeviceIndex: -1}
			}
			continue
		}
		if strings.Contains(lower, "device name") {
			current.DeviceName = extractValue(trimmed)
			continue
		}
		if strings.Contains(lower, "device uuid") {
			current.GPUUUID = normalizeHexString(extractValue(trimmed))
			continue
		}
		if strings.Contains(lower, "vendor id") {
			current.VendorID = normalizeHexString(extractValue(trimmed))
			continue
		}
		if strings.Contains(lower, "device id") && !strings.Contains(lower, "name") {
			current.DeviceID = normalizeHexString(extractValue(trimmed))
			continue
		}
		if strings.Contains(lower, "pci bus") {
			current.PCIBusID = strings.TrimSpace(extractValue(trimmed))
			continue
		}
		if strings.Contains(lower, "global memory size") {
			if vramBytes := parseOpenCLMemoryBytes(trimmed); vramBytes > 0 {
				current.VRAMBytes = vramBytes
			}
			continue
		}
	}
	flush()

	if len(identities) == 0 {
		return nil, errors.New("OpenCL runtime not detected. L2 mode requires OpenCL with CL_DEVICE_UUID_KHR support.")
	}

	for i := range identities {
		identities[i].DeviceIndex = i
	}

	return identities, nil
}

func detectWindowsPNPIdentities() ([]openCLIdentity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := execCommandContext(ctx, "powershell", "-NoProfile", "-Command", "Get-CimInstance Win32_VideoController | Select-Object PNPDeviceID,AdapterCompatibility,Name | ConvertTo-Csv -NoTypeInformation")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("windows_pnp_probe_failed: %w", err)
	}

	reader := csv.NewReader(strings.NewReader(string(out)))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("windows_pnp_csv_parse_failed: %w", err)
	}
	if len(records) <= 1 {
		return nil, errors.New("windows_pnp_no_devices")
	}

	headers := make(map[string]int)
	for i, h := range records[0] {
		headers[strings.ToLower(strings.TrimSpace(h))] = i
	}
	idxPNP, okPNP := headers["pnpdeviceid"]
	idxVendor, okVendor := headers["adaptercompatibility"]
	idxName, okName := headers["name"]
	if !okPNP || !okVendor || !okName {
		return nil, errors.New("windows_pnp_missing_required_columns")
	}

	identities := make([]openCLIdentity, 0, len(records)-1)
	for rowIdx, row := range records[1:] {
		if idxPNP >= len(row) || idxName >= len(row) {
			continue
		}
		pnp := strings.TrimSpace(row[idxPNP])
		name := strings.TrimSpace(row[idxName])
		if pnp == "" || name == "" {
			continue
		}
		vendor := ""
		if idxVendor < len(row) {
			vendor = strings.TrimSpace(row[idxVendor])
		}

		parsedVendorID, parsedDeviceID := parsePNPIDs(pnp)
		vendorFallbackID := parseLikelyHexID(vendor, 4)
		identities = append(identities, openCLIdentity{
			DeviceIndex: rowIdx,
			DeviceName:  name,
			GPUUUID:     makeCompatibilityGPUUUID("windows_pnp_device_id", pnp, name, vendor),
			VendorID:    firstNonEmpty(parsedVendorID, vendorFallbackID),
			DeviceID:    parsedDeviceID,
			PCIBusID:    pnp,
		})
	}

	if len(identities) == 0 {
		return nil, errors.New("windows_pnp_no_usable_gpu_identity")
	}

	return identities, nil
}

func detectMacPCIIdentities() ([]openCLIdentity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := execCommandContext(ctx, "system_profiler", "SPDisplaysDataType", "-json")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("mac_system_profiler_probe_failed: %w", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(out, &payload); err != nil {
		return nil, fmt.Errorf("mac_system_profiler_json_parse_failed: %w", err)
	}

	itemsRaw, ok := payload["SPDisplaysDataType"]
	if !ok {
		return nil, errors.New("mac_system_profiler_missing_spdisplays")
	}
	items, ok := itemsRaw.([]any)
	if !ok || len(items) == 0 {
		return nil, errors.New("mac_system_profiler_no_devices")
	}

	identities := make([]openCLIdentity, 0, len(items))
	for idx, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := firstNonEmpty(asString(obj["sppci_model"]), asString(obj["_name"]))
		vendorRaw := asString(obj["spdisplays_vendor"])
		vendorIDRaw := asString(obj["spdisplays_vendor-id"])
		deviceRaw := asString(obj["spdisplays_device-id"])
		bus := asString(obj["spdisplays_bus"])
		if strings.TrimSpace(name) == "" {
			continue
		}

		vendorID := parseLikelyHexID(vendorIDRaw, 4)
		if vendorID == "" {
			vendorID = parseLikelyHexID(vendorRaw, 4)
		}
		deviceID := parseLikelyHexID(deviceRaw, 4)
		seedBus := firstNonEmpty(bus, asString(obj["spdisplays_pcie_device_type"]))
		compatUUID := makeCompatibilityGPUUUID("mac_pci_fingerprint", name, vendorRaw, deviceRaw, seedBus)

		identities = append(identities, openCLIdentity{
			DeviceIndex: idx,
			DeviceName:  name,
			GPUUUID:     compatUUID,
			VendorID:    vendorID,
			DeviceID:    deviceID,
			PCIBusID:    seedBus,
		})
	}

	if len(identities) == 0 {
		return nil, errors.New("mac_system_profiler_no_usable_gpu_identity")
	}

	return identities, nil
}

func extractValue(line string) string {
	if idx := strings.Index(line, ":"); idx >= 0 {
		return strings.TrimSpace(line[idx+1:])
	}
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[len(parts)-1])
}

func normalizeHexString(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	b := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			b = append(b, c)
		}
	}
	return string(b)
}

func parseOpenCLMemoryBytes(value string) int64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	if idx := strings.Index(trimmed, ":"); idx >= 0 {
		trimmed = strings.TrimSpace(trimmed[idx+1:])
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return 0
	}
	for _, field := range fields {
		parsed, err := strconv.ParseInt(strings.Trim(field, "(),"), 10, 64)
		if err == nil && parsed > 0 {
			return parsed
		}
	}
	return 0
}

func vramBytesToGB(bytes int64) float64 {
	if bytes <= 0 {
		return 0
	}
	return float64(bytes) / (1024.0 * 1024.0 * 1024.0)
}

func buildDeviceFingerprint(vendorID, deviceID, pciBusID, gpuUUID, gpuModel string) string {
	seed := strings.ToLower(strings.TrimSpace(vendorID)) + ":" +
		strings.ToLower(strings.TrimSpace(deviceID)) + ":" +
		strings.ToLower(strings.TrimSpace(pciBusID)) + ":" +
		strings.ToLower(strings.TrimSpace(gpuUUID)) + ":" +
		strings.ToLower(strings.TrimSpace(gpuModel))
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:16])
}

func makeCompatibilityGPUUUID(source string, parts ...string) string {
	seedParts := []string{strings.ToLower(strings.TrimSpace(source))}
	for _, p := range parts {
		seedParts = append(seedParts, strings.ToLower(strings.TrimSpace(p)))
	}
	seed := strings.Join(seedParts, ":")
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:16])
}

func parsePNPIDs(value string) (string, string) {
	v := strings.ToUpper(strings.TrimSpace(value))
	vendorID := extractHexTokenAfterMarker(v, "VEN_", 4)
	deviceID := extractHexTokenAfterMarker(v, "DEV_", 4)
	return vendorID, deviceID
}

func extractHexTokenAfterMarker(value, marker string, minLen int) string {
	idx := strings.Index(value, marker)
	if idx < 0 {
		return ""
	}
	start := idx + len(marker)
	end := start
	for end < len(value) {
		c := value[end]
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')) {
			break
		}
		end++
	}
	if end-start < minLen {
		return ""
	}
	return strings.ToLower(value[start:end])
}

func parseLikelyHexID(value string, minLen int) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return ""
	}

	normalized := normalizeHexString(v)
	if len(normalized) < minLen {
		return ""
	}

	hasDigit := false
	for i := 0; i < len(normalized); i++ {
		if normalized[i] >= '0' && normalized[i] <= '9' {
			hasDigit = true
			break
		}
	}
	if !hasDigit {
		return ""
	}

	if strings.Contains(v, "0x") || strings.Contains(v, "0X") {
		if idx := strings.Index(strings.ToLower(v), "0x"); idx >= 0 {
			if token := extractHexTokenAfterMarker(strings.ToUpper(v[idx:]), "0X", minLen); token != "" {
				return token
			}
		}
	}

	return normalized
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
