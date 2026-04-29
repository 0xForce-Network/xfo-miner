package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

func TestScanGPUsFailsWhenOpenCLUnavailable(t *testing.T) {
	origCmd := execCommandContext
	defer func() {
		execCommandContext = origCmd
	}()

	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		if name == "clinfo" {
			return exec.Command("/bin/sh", "-c", "exit 1")
		}
		return exec.Command("/bin/sh", "-c", "exit 1")
	}

	_, err := ScanGPUs()
	if err == nil {
		t.Fatalf("expected ScanGPUs() error when clinfo is missing")
	}
	if !strings.Contains(err.Error(), "OpenCL runtime not detected") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScanGPUsUsesOpenCLWhenNvidiaSMIMissing(t *testing.T) {
	origCmd := execCommandContext
	origLookPath := lookPath
	defer func() {
		execCommandContext = origCmd
		lookPath = origLookPath
	}()

	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		if name == "clinfo" {
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\nDevice Name : RTX 4090\nDevice UUID : 1234-ABCD\nVendor ID : 10DE\nDevice ID : 2684\nPCI bus info : 0000:01:00.0\nEOF")
		}
		return exec.Command("/bin/sh", "-c", "exit 1")
	}
	lookPath = func(_ string) (string, error) { return "", errors.New("not found") }

	devices, err := ScanGPUs()
	if err != nil {
		t.Fatalf("ScanGPUs() error = %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("expected one device, got %d", len(devices))
	}
	if devices[0].GPUUUID == "" || devices[0].UUIDSource != "opencl_uuid_khr" {
		t.Fatalf("expected opencl identity fields, got %+v", devices[0])
	}
}

func TestScanGPUsWindowsFallbackPNPIdentity(t *testing.T) {
	origCmd := execCommandContext
	origLookPath := lookPath
	origGOOS := currentGOOS
	defer func() {
		execCommandContext = origCmd
		lookPath = origLookPath
		currentGOOS = origGOOS
	}()

	currentGOOS = "windows"
	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		switch name {
		case "clinfo":
			return exec.Command("/bin/sh", "-c", "exit 1")
		case "powershell":
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\n\"PNPDeviceID\",\"AdapterCompatibility\",\"Name\",\"AdapterRAM\"\n\"PCI\\VEN_10DE&DEV_2684&SUBSYS_12AA10DE&REV_A1\",\"NVIDIA\",\"NVIDIA RTX 4090\",\"25769803776\"\nEOF")
		default:
			return exec.Command("/bin/sh", "-c", "exit 1")
		}
	}
	lookPath = func(_ string) (string, error) { return "", errors.New("not found") }

	devices, err := ScanGPUs()
	if err != nil {
		t.Fatalf("ScanGPUs() error = %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("expected one device, got %d", len(devices))
	}
	if devices[0].UUIDSource != "windows_pnp_device_id" {
		t.Fatalf("expected windows_pnp_device_id uuid source, got %+v", devices[0])
	}
	if devices[0].GPUUUID == "" {
		t.Fatalf("expected compatibility gpu uuid, got %+v", devices[0])
	}
	if devices[0].VendorID != "10de" {
		t.Fatalf("expected vendor_id=10de from VEN_ token, got %+v", devices[0])
	}
	if devices[0].DeviceID != "0" {
		t.Fatalf("expected device_id to reflect identity device index for fallback, got %+v", devices[0])
	}
	if !strings.Contains(strings.ToUpper(devices[0].PCIBusID), "PCI\\VEN_10DE") {
		t.Fatalf("expected pci_bus_id to preserve pnp id, got %+v", devices[0])
	}
	if devices[0].VRAMBytes != 25769803776 {
		t.Fatalf("expected VRAMBytes from AdapterRAM, got %+v", devices[0])
	}
}

func TestScanGPUsWindowsFallbackPNPDemotesVirtualAdapters(t *testing.T) {
	origCmd := execCommandContext
	origLookPath := lookPath
	origGOOS := currentGOOS
	defer func() {
		execCommandContext = origCmd
		lookPath = origLookPath
		currentGOOS = origGOOS
	}()

	currentGOOS = "windows"
	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		switch name {
		case "clinfo":
			return exec.Command("/bin/sh", "-c", "exit 1")
		case "powershell":
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\n\"PNPDeviceID\",\"AdapterCompatibility\",\"Name\",\"AdapterRAM\"\n\"ROOT\\DISPLAY\\0000\",\"Microsoft\",\"OrayIddDriver Device\",\"0\"\n\"PCI\\VEN_1002&DEV_66AF&SUBSYS_0B0E1002&REV_C1\",\"Advanced Micro Devices, Inc.\",\"AMD Radeon Pro VII\",\"17163091968\"\n\"PCI\\VEN_1A03&DEV_2000&SUBSYS_20001A03&REV_41\",\"ASPEED Technology, Inc.\",\"ASPEED Graphics Family(WDDM)\",\"16777216\"\nEOF")
		default:
			return exec.Command("/bin/sh", "-c", "exit 1")
		}
	}
	lookPath = func(_ string) (string, error) { return "", errors.New("not found") }

	devices, err := ScanGPUs()
	if err != nil {
		t.Fatalf("ScanGPUs() error = %v", err)
	}
	if len(devices) != 3 {
		t.Fatalf("expected three devices, got %d", len(devices))
	}
	if devices[0].GPUModel != "AMD Radeon Pro VII" {
		t.Fatalf("expected physical AMD GPU to be primary after normalization, got %+v", devices)
	}
	if devices[0].UUIDSource != "windows_pnp_device_id" {
		t.Fatalf("expected windows_pnp_device_id fallback source, got %+v", devices[0])
	}
	if devices[0].VRAMBytes != 17163091968 {
		t.Fatalf("expected AMD VRAMBytes from AdapterRAM, got %+v", devices[0])
	}
}

func TestDetectOpenCLIdentitiesAcceptsAMDUUIDAndTopology(t *testing.T) {
	origCmd := execCommandContext
	defer func() {
		execCommandContext = origCmd
	}()

	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		if name == "clinfo" {
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\nDevice Name : AMD Radeon Pro VII\nUUID (AMD) : 1234-ABCD-5678-EF90\nVendor ID : 1002\nDevice ID : 66AF\nTopology (AMD) : PCI-E, 03:00.0\nGlobal memory size : 17163091968\nEOF")
		}
		return exec.Command("/bin/sh", "-c", "exit 1")
	}

	identities, err := detectOpenCLIdentities()
	if err != nil {
		t.Fatalf("detectOpenCLIdentities() error = %v", err)
	}
	if len(identities) != 1 {
		t.Fatalf("expected one identity, got %d", len(identities))
	}
	if identities[0].GPUUUID != "1234abcd5678ef90" {
		t.Fatalf("expected normalized AMD UUID, got %+v", identities[0])
	}
	if identities[0].PCIBusID != "0000:03:00.0" {
		t.Fatalf("expected normalized AMD topology bus id, got %+v", identities[0])
	}
	if identities[0].VRAMBytes != 17163091968 {
		t.Fatalf("expected global memory size to populate VRAM, got %+v", identities[0])
	}
}

func TestDetectOpenCLIdentitiesBuildsCompatibilityUUIDFromTopology(t *testing.T) {
	origCmd := execCommandContext
	defer func() {
		execCommandContext = origCmd
	}()

	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		if name == "clinfo" {
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\nPlatform Name: AMD Accelerated Parallel Processing\nNumber of devices: 1\nDevice Type: CL_DEVICE_TYPE_GPU\nVendor ID: 1002h\nBoard name: AMD Radeon Pro VII\nDevice Topology: PCI[ B#39, D#0, F#0 ]\nGlobal memory size: 17163091968\nEOF")
		}
		return exec.Command("/bin/sh", "-c", "exit 1")
	}

	identities, err := detectOpenCLIdentities()
	if err != nil {
		t.Fatalf("detectOpenCLIdentities() error = %v", err)
	}
	if len(identities) != 1 {
		t.Fatalf("expected one identity, got %d", len(identities))
	}
	if identities[0].GPUUUID == "" {
		t.Fatalf("expected compatibility UUID from topology when explicit UUID is absent, got %+v", identities[0])
	}
	if identities[0].DeviceName != "AMD Radeon Pro VII" {
		t.Fatalf("expected board name to populate device name, got %+v", identities[0])
	}
	if identities[0].PCIBusID != "0000:27:00.0" {
		t.Fatalf("expected normalized topology bus id, got %+v", identities[0])
	}
	if identities[0].VRAMBytes != 17163091968 {
		t.Fatalf("expected global memory size to populate VRAM, got %+v", identities[0])
	}
}

func TestScanGPUsWindowsUsesOpenCLTopologyIdentityBeforePNPFallback(t *testing.T) {
	origCmd := execCommandContext
	origLookPath := lookPath
	origGOOS := currentGOOS
	defer func() {
		execCommandContext = origCmd
		lookPath = origLookPath
		currentGOOS = origGOOS
	}()

	currentGOOS = "windows"
	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		switch name {
		case "clinfo":
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\nPlatform Name: AMD Accelerated Parallel Processing\nNumber of devices: 1\nDevice Type: CL_DEVICE_TYPE_GPU\nVendor ID: 1002h\nBoard name: AMD Radeon Pro VII\nDevice Topology: PCI[ B#39, D#0, F#0 ]\nGlobal memory size: 17163091968\nEOF")
		case "powershell":
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\n\"PNPDeviceID\",\"AdapterCompatibility\",\"Name\",\"AdapterRAM\"\n\"ROOT\\DISPLAY\\0000\",\"Microsoft\",\"OrayIddDriver Device\",\"0\"\n\"PCI\\VEN_1002&DEV_66A1&SUBSYS_081E1002&REV_06\",\"Advanced Micro Devices, Inc.\",\"AMD Radeon Pro VII\",\"4293918720\"\nEOF")
		default:
			return exec.Command("/bin/sh", "-c", "exit 1")
		}
	}
	lookPath = func(_ string) (string, error) { return "", errors.New("not found") }

	devices, err := ScanGPUs()
	if err != nil {
		t.Fatalf("ScanGPUs() error = %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("expected one opencl-correlated device, got %d (%+v)", len(devices), devices)
	}
	if devices[0].UUIDSource != "opencl_uuid_khr" {
		t.Fatalf("expected OpenCL source to win when topology identity is available, got %+v", devices[0])
	}
	if devices[0].VRAMBytes != 17163091968 {
		t.Fatalf("expected OpenCL VRAM to beat mismatched PnP AdapterRAM, got %+v", devices[0])
	}
	if devices[0].PCIBusID != "0000:27:00.0" {
		t.Fatalf("expected normalized topology bus id from clinfo, got %+v", devices[0])
	}
}

func TestParseWindowsAdapterRAMBytes(t *testing.T) {
	if got := parseWindowsAdapterRAMBytes("17163091968"); got != 17163091968 {
		t.Fatalf("expected raw bytes parse, got %d", got)
	}
	if got := parseWindowsAdapterRAMBytes(`"25,769,803,776"`); got != 25769803776 {
		t.Fatalf("expected comma-delimited bytes parse, got %d", got)
	}
	if got := parseWindowsAdapterRAMBytes("0"); got != 0 {
		t.Fatalf("expected zero to stay zero, got %d", got)
	}
}

func TestScanGPUsMacFallbackFingerprintIdentity(t *testing.T) {
	origCmd := execCommandContext
	origLookPath := lookPath
	origGOOS := currentGOOS
	defer func() {
		execCommandContext = origCmd
		lookPath = origLookPath
		currentGOOS = origGOOS
	}()

	currentGOOS = "darwin"
	execCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		switch name {
		case "clinfo":
			return exec.Command("/bin/sh", "-c", "exit 1")
		case "system_profiler":
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\n{\"SPDisplaysDataType\":[{\"sppci_model\":\"AMD Radeon Pro 5500M\",\"spdisplays_vendor\":\"AMD\",\"spdisplays_device-id\":\"0x7340\",\"spdisplays_bus\":\"PCIe\"}]}\nEOF")
		default:
			return exec.Command("/bin/sh", "-c", fmt.Sprintf("echo unexpected command %s; exit 1", name))
		}
	}
	lookPath = func(_ string) (string, error) { return "", errors.New("not found") }

	devices, err := ScanGPUs()
	if err != nil {
		t.Fatalf("ScanGPUs() error = %v", err)
	}
	if len(devices) != 1 {
		t.Fatalf("expected one device, got %d", len(devices))
	}
	if devices[0].UUIDSource != "mac_pci_fingerprint" {
		t.Fatalf("expected mac_pci_fingerprint uuid source, got %+v", devices[0])
	}
	if devices[0].GPUUUID == "" {
		t.Fatalf("expected compatibility gpu uuid, got %+v", devices[0])
	}
	if devices[0].VendorID != "" {
		t.Fatalf("expected empty vendor_id when only vendor name is available, got %+v", devices[0])
	}
	if devices[0].DeviceID != "0" {
		t.Fatalf("expected device_id to reflect identity device index for fallback, got %+v", devices[0])
	}
	if devices[0].PCIBusID != "PCIe" {
		t.Fatalf("expected pci_bus_id from system_profiler bus field, got %+v", devices[0])
	}
}

func TestParsePNPIDsExtractsVendorAndDevice(t *testing.T) {
	vendorID, deviceID := parsePNPIDs(`PCI\VEN_1002&DEV_73BF&SUBSYS_0B361002&REV_C1`)
	if vendorID != "1002" || deviceID != "73bf" {
		t.Fatalf("expected parsed ids (1002,73bf), got (%s,%s)", vendorID, deviceID)
	}
}

func TestParseLikelyHexIDRejectsVendorNames(t *testing.T) {
	if got := parseLikelyHexID("AMD", 4); got != "" {
		t.Fatalf("expected empty id for vendor name AMD, got %q", got)
	}
	if got := parseLikelyHexID("NVIDIA", 4); got != "" {
		t.Fatalf("expected empty id for vendor name NVIDIA, got %q", got)
	}
	if got := parseLikelyHexID("0x10DE", 4); got != "10de" {
		t.Fatalf("expected hex id 10de, got %q", got)
	}
}

func TestParseOpenCLMemoryBytesHandlesClinfoFormats(t *testing.T) {
	const expected = int64(17163091968)
	for _, input := range []string{
		"Global memory size: 17163091968",
		"Global memory size 17163091968 (15.99GiB)",
		"CL_DEVICE_GLOBAL_MEM_SIZE 17163091968",
	} {
		if got := parseOpenCLMemoryBytes(input); got != expected {
			t.Fatalf("parseOpenCLMemoryBytes(%q)=%d want %d", input, got, expected)
		}
	}
}
