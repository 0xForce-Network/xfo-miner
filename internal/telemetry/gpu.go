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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/0xforce/xfo-miner/internal/debuglog"
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
	VRAMBytes         int64   `json:"vram_bytes,omitempty"`
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
					VRAMBytes:         identity.VRAMBytes,
					Status:            "idle",
					ReputationScore:   50.0,
					PCIBusID:          identity.PCIBusID,
				})
			}
			devices = normalizeGPUDevices(devices)
			logGPUDevices("opencl_fallback_devices", devices)
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
			VRAMBytes:         int64(memMB * 1024.0 * 1024.0),
			Status:            "idle",
			ReputationScore:   50.0,
			PCIBusID:          strings.TrimSpace(parts[3]),
			Temperature:       temp,
			Utilization:       util,
		})
	}
	devices = normalizeGPUDevices(devices)
	logGPUDevices("nvidia_smi_devices", devices)

	return devices, nil
}

func normalizeGPUDevices(devices []GPUDevice) []GPUDevice {
	if len(devices) <= 1 {
		return devices
	}
	normalized := append([]GPUDevice(nil), devices...)
	sort.SliceStable(normalized, func(i, j int) bool {
		return gpuDeviceLess(normalized[i], normalized[j])
	})
	return normalized
}

func gpuDeviceLess(a GPUDevice, b GPUDevice) bool {
	if hashcatVisibleGPUDevice(a) != hashcatVisibleGPUDevice(b) {
		return hashcatVisibleGPUDevice(a)
	}
	if openCLPreferredGPUDevice(a) != openCLPreferredGPUDevice(b) {
		return openCLPreferredGPUDevice(a)
	}
	if nonVirtualGPUDevice(a) != nonVirtualGPUDevice(b) {
		return nonVirtualGPUDevice(a)
	}
	if a.VRAMBytes != b.VRAMBytes {
		return a.VRAMBytes > b.VRAMBytes
	}
	if a.VRAMGB != b.VRAMGB {
		return a.VRAMGB > b.VRAMGB
	}
	if hasPCIBusGPUDevice(a) != hasPCIBusGPUDevice(b) {
		return hasPCIBusGPUDevice(a)
	}
	return a.DeviceIndex < b.DeviceIndex
}

func hashcatVisibleGPUDevice(device GPUDevice) bool {
	model := strings.ToLower(strings.TrimSpace(device.GPUModel))
	for _, visible := range debuglog.CurrentHashcatVisibleModels() {
		visibleLower := strings.ToLower(strings.TrimSpace(visible))
		if visibleLower == "" {
			continue
		}
		if strings.Contains(visibleLower, model) || strings.Contains(model, visibleLower) {
			return true
		}
	}
	return false
}

func openCLPreferredGPUDevice(device GPUDevice) bool {
	return strings.TrimSpace(device.UUIDSource) == "opencl_uuid_khr"
}

func nonVirtualGPUDevice(device GPUDevice) bool {
	isVirtual, _ := debuglog.ClassifyVirtualAdapter(device.GPUModel, device.PCIBusID)
	return !isVirtual
}

func hasPCIBusGPUDevice(device GPUDevice) bool {
	return strings.TrimSpace(device.PCIBusID) != ""
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
	logOpenCLPlatforms(lines)
	identities := make([]openCLIdentity, 0)
	current := openCLIdentity{DeviceIndex: -1}

	flush := func() {
		if strings.TrimSpace(current.GPUUUID) == "" {
			current.GPUUUID = buildOpenCLCompatibilityUUID(current)
		}
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
			if openCLIdentityHasData(current) {
				flush()
				current = openCLIdentity{DeviceIndex: -1}
			}
			continue
		}
		if strings.Contains(lower, "device name") {
			if openCLIdentityHasData(current) {
				flush()
				current = openCLIdentity{DeviceIndex: -1}
			}
			current.DeviceName = extractValue(trimmed)
			continue
		}
		if strings.Contains(lower, "board name") && strings.TrimSpace(current.DeviceName) == "" {
			current.DeviceName = extractValue(trimmed)
			continue
		}
		if isOpenCLUUIDLine(lower) {
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
			current.PCIBusID = parseOpenCLPCIBusID(extractValue(trimmed))
			continue
		}
		if strings.Contains(lower, "topology (amd)") || strings.Contains(lower, "device topology (amd)") || strings.Contains(lower, "device topology") {
			if busID := parseOpenCLPCIBusID(extractValue(trimmed)); busID != "" {
				current.PCIBusID = busID
			}
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
	debuglog.LogVerbose("opencl_identity_devices", "devices", identities)

	return identities, nil
}

func detectWindowsPNPIdentities() ([]openCLIdentity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := execCommandContext(ctx, "powershell", "-NoProfile", "-Command", "Get-CimInstance Win32_VideoController | Select-Object PNPDeviceID,AdapterCompatibility,Name,AdapterRAM | ConvertTo-Csv -NoTypeInformation")
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
	idxAdapterRAM, hasAdapterRAM := headers["adapterram"]
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
		vramBytes := int64(0)
		if hasAdapterRAM && idxAdapterRAM < len(row) {
			vramBytes = parseWindowsAdapterRAMBytes(row[idxAdapterRAM])
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
			VRAMBytes:   vramBytes,
		})
	}

	if len(identities) == 0 {
		return nil, errors.New("windows_pnp_no_usable_gpu_identity")
	}
	debuglog.LogVerbose("windows_pnp_devices", "devices", identities)

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
	debuglog.LogVerbose("mac_pci_devices", "devices", identities)

	return identities, nil
}

func logOpenCLPlatforms(lines []string) {
	if !debuglog.Verbose() {
		return
	}
	type platformSummary struct {
		PlatformIndex int    `json:"platform_index"`
		PlatformName  string `json:"platform_name,omitempty"`
		Vendor        string `json:"vendor,omitempty"`
		Version       string `json:"version,omitempty"`
	}
	platforms := make([]platformSummary, 0)
	current := platformSummary{PlatformIndex: -1}
	flush := func() {
		if current.PlatformIndex < 0 && current.PlatformName == "" && current.Vendor == "" && current.Version == "" {
			return
		}
		platforms = append(platforms, current)
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)
		if strings.Contains(lower, "platform #") {
			if current.PlatformIndex >= 0 || current.PlatformName != "" || current.Vendor != "" || current.Version != "" {
				flush()
				current = platformSummary{PlatformIndex: -1}
			}
			if idx := strings.Index(lower, "platform #"); idx >= 0 {
				num := strings.TrimSpace(lower[idx+len("platform #"):])
				if parsed, err := strconv.Atoi(strings.Fields(num)[0]); err == nil {
					current.PlatformIndex = parsed
				}
			}
			continue
		}
		if strings.Contains(lower, "platform name") {
			current.PlatformName = extractValue(trimmed)
			continue
		}
		if strings.Contains(lower, "platform vendor") {
			current.Vendor = extractValue(trimmed)
			continue
		}
		if strings.Contains(lower, "platform version") {
			current.Version = extractValue(trimmed)
			continue
		}
	}
	flush()
	if len(platforms) > 0 {
		debuglog.LogVerbose("opencl_platforms", "platforms", platforms)
	}
}

func logGPUDevices(event string, devices []GPUDevice) {
	if !debuglog.Enabled() {
		return
	}
	for _, device := range devices {
		isVirtual, virtualReason := debuglog.ClassifyVirtualAdapter(device.GPUModel, device.PCIBusID)
		debuglog.Log("gpu_device_identity",
			"source_event", event,
			"gpu_model", device.GPUModel,
			"gpu_uuid", device.GPUUUID,
			"uuid_source", device.UUIDSource,
			"device_fingerprint", device.DeviceFingerprint,
			"pci_bus_id", device.PCIBusID,
			"vendor_id", device.VendorID,
			"device_id", device.DeviceID,
			"device_index", device.DeviceIndex,
			"vram_gb", device.VRAMGB,
			"vram_bytes", device.VRAMBytes,
			"identity_stable", strings.TrimSpace(device.GPUUUID) != "" || strings.TrimSpace(device.DeviceFingerprint) != "",
			"is_virtual", isVirtual,
			"virtual_reason", virtualReason,
		)
	}
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

func isOpenCLUUIDLine(lower string) bool {
	if strings.Contains(lower, "driver uuid") {
		return false
	}
	return strings.Contains(lower, "device uuid") ||
		strings.Contains(lower, "cl_device_uuid_khr") ||
		strings.Contains(lower, "uuid (amd)") ||
		strings.HasPrefix(strings.TrimSpace(lower), "uuid:") ||
		strings.HasPrefix(strings.TrimSpace(lower), "uuid ")
}

func buildOpenCLCompatibilityUUID(identity openCLIdentity) string {
	if strings.TrimSpace(identity.GPUUUID) != "" {
		return identity.GPUUUID
	}
	if strings.TrimSpace(identity.DeviceName) == "" {
		return ""
	}
	if strings.TrimSpace(identity.PCIBusID) == "" && strings.TrimSpace(identity.VendorID) == "" && strings.TrimSpace(identity.DeviceID) == "" {
		return ""
	}
	return makeCompatibilityGPUUUID("opencl_topology_fingerprint", identity.DeviceName, identity.VendorID, identity.DeviceID, identity.PCIBusID)
}

func openCLIdentityHasData(identity openCLIdentity) bool {
	return strings.TrimSpace(identity.DeviceName) != "" ||
		strings.TrimSpace(identity.GPUUUID) != "" ||
		strings.TrimSpace(identity.VendorID) != "" ||
		strings.TrimSpace(identity.DeviceID) != "" ||
		strings.TrimSpace(identity.PCIBusID) != "" ||
		identity.VRAMBytes > 0
}

func parseOpenCLPCIBusID(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return ""
	}
	if busID := parseOpenCLTopologyNotation(trimmed); busID != "" {
		return busID
	}
	if idx := strings.LastIndex(trimmed, ","); idx >= 0 {
		trimmed = strings.TrimSpace(trimmed[idx+1:])
	}
	trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "pci-e"))
	trimmed = strings.Trim(trimmed, " ,")
	if busID := parseOpenCLTopologyNotation(trimmed); busID != "" {
		return busID
	}
	if len(trimmed) == len("00:00.0") {
		return "0000:" + trimmed
	}
	return trimmed
}

func parseOpenCLTopologyNotation(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	trimmed = strings.ReplaceAll(trimmed, " ", "")
	trimmed = strings.TrimPrefix(trimmed, "pci[")
	trimmed = strings.TrimSuffix(trimmed, "]")
	if !strings.Contains(trimmed, "b#") || !strings.Contains(trimmed, "d#") || !strings.Contains(trimmed, "f#") {
		return ""
	}
	bus := parseDecimalTokenAfterMarker(trimmed, "b#")
	device := parseDecimalTokenAfterMarker(trimmed, "d#")
	function := parseDecimalTokenAfterMarker(trimmed, "f#")
	if bus < 0 || device < 0 || function < 0 {
		return ""
	}
	return fmt.Sprintf("0000:%02x:%02x.%d", bus, device, function)
}

func parseDecimalTokenAfterMarker(value, marker string) int {
	idx := strings.Index(value, marker)
	if idx < 0 {
		return -1
	}
	start := idx + len(marker)
	end := start
	for end < len(value) {
		c := value[end]
		if c < '0' || c > '9' {
			break
		}
		end++
	}
	if end == start {
		return -1
	}
	parsed, err := strconv.Atoi(value[start:end])
	if err != nil {
		return -1
	}
	return parsed
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

func parseWindowsAdapterRAMBytes(value string) int64 {
	trimmed := strings.TrimSpace(strings.Trim(value, `"`))
	trimmed = strings.ReplaceAll(trimmed, ",", "")
	if trimmed == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
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
