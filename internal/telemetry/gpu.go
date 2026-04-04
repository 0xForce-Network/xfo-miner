package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var execCommandContext = exec.CommandContext
var lookPath = exec.LookPath

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
}

func ScanGPUs() ([]GPUDevice, error) {
	identities, err := detectOpenCLIdentities()
	if err != nil {
		return nil, err
	}
	if len(identities) == 0 {
		return nil, errors.New("OpenCL runtime not detected. L2 mode requires OpenCL with CL_DEVICE_UUID_KHR support.")
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
					UUIDSource:        "opencl_uuid_khr",
					GPUUUID:           identity.GPUUUID,
					DeviceFingerprint: buildDeviceFingerprint(identity.VendorID, identity.DeviceID, identity.PCIBusID, identity.GPUUUID, identity.DeviceName),
					GPUModel:          identity.DeviceName,
					VRAMGB:            0,
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
			return nil, fmt.Errorf("OpenCL runtime not detected. L2 mode requires OpenCL with CL_DEVICE_UUID_KHR support. missing UUID for gpu index=%d", idxInt)
		}

		devices = append(devices, GPUDevice{
			DeviceID:          idx,
			DeviceIndex:       idxInt,
			VendorID:          identity.VendorID,
			UUIDSource:        "opencl_uuid_khr",
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

func buildDeviceFingerprint(vendorID, deviceID, pciBusID, gpuUUID, gpuModel string) string {
	seed := strings.ToLower(strings.TrimSpace(vendorID)) + ":" +
		strings.ToLower(strings.TrimSpace(deviceID)) + ":" +
		strings.ToLower(strings.TrimSpace(pciBusID)) + ":" +
		strings.ToLower(strings.TrimSpace(gpuUUID)) + ":" +
		strings.ToLower(strings.TrimSpace(gpuModel))
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:16])
}
