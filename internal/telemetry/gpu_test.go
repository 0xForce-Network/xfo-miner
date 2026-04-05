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
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\n\"PNPDeviceID\",\"AdapterCompatibility\",\"Name\"\n\"PCI\\VEN_10DE&DEV_2684&SUBSYS_12AA10DE&REV_A1\",\"NVIDIA\",\"NVIDIA RTX 4090\"\nEOF")
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
