package debuglog

import "testing"

func TestClassifyVirtualAdapterDetectsOray(t *testing.T) {
	t.Parallel()
	isVirtual, reason := ClassifyVirtualAdapter("OrayIddDriver Device", "PCI\\VEN_1414")
	if !isVirtual {
		t.Fatalf("expected virtual adapter detection")
	}
	if reason == "" {
		t.Fatalf("expected non-empty virtual adapter reason")
	}
}

func TestClassifyVirtualAdapterDetectsASPEEDAndRootDisplay(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		model string
		busID string
	}{
		{model: "ASPEED Graphics Family(WDDM)", busID: "PCI\\VEN_1A03&DEV_2000"},
		{model: "Remote Display Adapter", busID: "ROOT\\DISPLAY\\0000"},
	} {
		isVirtual, reason := ClassifyVirtualAdapter(tc.model, tc.busID)
		if !isVirtual {
			t.Fatalf("expected virtual adapter detection for %+v", tc)
		}
		if reason == "" {
			t.Fatalf("expected non-empty virtual adapter reason for %+v", tc)
		}
	}
}

func TestCandidateLessPrefersHashcatVisibleOpenCLNonVirtual(t *testing.T) {
	t.Parallel()
	state.hashcatVisibleList = []string{"AMD Radeon Pro VII"}
	a := DeviceSummary{GPUModel: "AMD Radeon Pro VII", UUIDSource: "opencl_uuid_khr", PCIBusID: "0000:03:00.0", VRAMBytes: 16 * 1024 * 1024 * 1024}
	b := DeviceSummary{GPUModel: "OrayIddDriver Device", UUIDSource: "windows_pnp_device_id", PCIBusID: "PCI\\VEN_1414", VRAMBytes: 0, IsVirtual: true}
	if !candidateLess(a, b) {
		t.Fatalf("expected compute GPU to outrank virtual adapter")
	}
}