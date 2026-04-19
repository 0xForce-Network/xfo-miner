package forensic

import (
	"context"
	"errors"
	"testing"
)

func TestNewForensicSandboxRegistersBindingsAndIsClosable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sandbox, err := NewForensicSandbox(ctx, nil)
	if err != nil {
		t.Fatalf("NewForensicSandbox() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Close(ctx)
	})

	bindingSet := map[string]struct{}{}
	for _, name := range sandbox.RegisteredHostBindings() {
		bindingSet[name] = struct{}{}
	}

	expected := []string{
		HostBindingAnalyzeMemoryTemporalAnomalies,
		HostBindingGetHardwareTrustAnchorSignature,
		HostBindingVerifyExecutionLatencyProfile,
		HostBindingReadVRAMEntropySample,
		HostBindingReadChallengeID,
	}

	for _, name := range expected {
		if _, ok := bindingSet[name]; !ok {
			t.Fatalf("missing registered host binding: %s", name)
		}
	}
}

func TestForensicSandboxFailClosedWithoutWASI(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sandbox, err := NewForensicSandbox(ctx, nil)
	if err != nil {
		t.Fatalf("NewForensicSandbox() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Close(ctx)
	})

	_, err = sandbox.runtime.Instantiate(ctx, wasmImportingWASI)
	if err == nil {
		t.Fatal("expected missing WASI import error, got nil")
	}
}

func TestForensicSandboxAllowsWhitelistedHostNamespace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sandbox, err := NewForensicSandbox(ctx, nil)
	if err != nil {
		t.Fatalf("NewForensicSandbox() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Close(ctx)
	})

	mod, err := sandbox.runtime.Instantiate(ctx, wasmImportingForensicBinding)
	if err != nil {
		t.Fatalf("Instantiate(forensic import) error = %v", err)
	}
	_ = mod.Close(ctx)
}

func TestHandleServerProbeRejectsInvalidPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sandbox, err := NewForensicSandbox(ctx, nil)
	if err != nil {
		t.Fatalf("NewForensicSandbox() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Close(ctx)
	})

	result, err := sandbox.HandleServerProbe(ctx, []byte{0x00, 0x61}, "challenge-198-invalid")
	if !errors.Is(err, ErrProbePayloadInvalid) {
		t.Fatalf("expected ErrProbePayloadInvalid, got %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil probe result")
	}
	if result.Status != "FAILED" {
		t.Fatalf("expected FAILED status, got %q", result.Status)
	}
	if result.ChallengeID != "challenge-198-invalid" {
		t.Fatalf("expected challenge id passthrough, got %q", result.ChallengeID)
	}
	if result.ErrorCode != "probe_payload_invalid" {
		t.Fatalf("expected probe_payload_invalid error_code, got %q", result.ErrorCode)
	}
}

func TestHandleServerProbeMapsMissingExport(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sandbox, err := NewForensicSandbox(ctx, nil)
	if err != nil {
		t.Fatalf("NewForensicSandbox() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Close(ctx)
	})

	result, err := sandbox.HandleServerProbe(ctx, wasmImportingForensicBinding, "challenge-198-missing")
	if !errors.Is(err, ErrProbeModuleMissingExport) {
		t.Fatalf("expected ErrProbeModuleMissingExport, got %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil probe result")
	}
	if result.ErrorCode != "probe_module_missing_export" {
		t.Fatalf("expected probe_module_missing_export, got %q", result.ErrorCode)
	}
}

func TestHandleServerProbeHappyPathAndChallengeBridge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sandbox, err := NewForensicSandbox(ctx, nil)
	if err != nil {
		t.Fatalf("NewForensicSandbox() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Close(ctx)
	})

	result, err := sandbox.HandleServerProbe(ctx, wasmProbeMainUsesChallengeBridge, "challenge-198-ok")
	if err != nil {
		t.Fatalf("expected nil error for happy path, got %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil probe result")
	}
	if result.Status != "OK" {
		t.Fatalf("expected OK status, got %q", result.Status)
	}
	if result.Data != "OK" {
		t.Fatalf("expected OK data, got %q", result.Data)
	}
}

func TestHandleServerProbeMapsExecutionTrap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sandbox, err := NewForensicSandbox(ctx, nil)
	if err != nil {
		t.Fatalf("NewForensicSandbox() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Close(ctx)
	})

	result, err := sandbox.HandleServerProbe(ctx, wasmProbeMainTraps, "challenge-198-trap")
	if !errors.Is(err, ErrProbeExecutionTrapped) {
		t.Fatalf("expected ErrProbeExecutionTrapped, got %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil probe result")
	}
	if result.ErrorCode != "probe_execution_trapped" {
		t.Fatalf("expected probe_execution_trapped, got %q", result.ErrorCode)
	}
}

var wasmImportingWASI = []byte{
	0x00, 0x61, 0x73, 0x6d,
	0x01, 0x00, 0x00, 0x00,
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
	0x02, 0x23, 0x01,
	0x16,
	'w', 'a', 's', 'i', '_', 's', 'n', 'a', 'p', 's', 'h', 'o', 't', '_', 'p', 'r', 'e', 'v', 'i', 'e', 'w', '1',
	0x08,
	'f', 'd', '_', 'w', 'r', 'i', 't', 'e',
	0x00, 0x00,
}

var wasmImportingForensicBinding = []byte{
	0x00, 0x61, 0x73, 0x6d,
	0x01, 0x00, 0x00, 0x00,
	0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f,
	0x02, 0x32, 0x01,
	0x0c,
	'x', 'f', 'o', '.', 'f', 'o', 'r', 'e', 'n', 's', 'i', 'c',
	0x21,
	'a', 'n', 'a', 'l', 'y', 'z', 'e', '_', 'm', 'e', 'm', 'o', 'r', 'y', '_', 't', 'e', 'm', 'p', 'o', 'r', 'a', 'l', '_', 'a', 'n', 'o', 'm', 'a', 'l', 'i', 'e', 's',
	0x00, 0x00,
}

var wasmProbeMainUsesChallengeBridge = []byte{
	0x00, 0x61, 0x73, 0x6d,
	0x01, 0x00, 0x00, 0x00,
	0x01, 0x0b, 0x02,
	0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f,
	0x60, 0x00, 0x01, 0x7f,
	0x02, 0x22, 0x01,
	0x0c,
	'x', 'f', 'o', '.', 'f', 'o', 'r', 'e', 'n', 's', 'i', 'c',
	0x11,
	'r', 'e', 'a', 'd', '_', 'c', 'h', 'a', 'l', 'l', 'e', 'n', 'g', 'e', '_', 'i', 'd',
	0x00, 0x00,
	0x03, 0x02, 0x01, 0x01,
	0x05, 0x03, 0x01, 0x00, 0x01,
	0x07, 0x17, 0x02,
	0x06,
	'm', 'e', 'm', 'o', 'r', 'y',
	0x02, 0x00,
	0x0a,
	'p', 'r', 'o', 'b', 'e', '_', 'm', 'a', 'i', 'n',
	0x00, 0x01,
	0x0a, 0x13, 0x01, 0x11,
	0x00,
	0x41, 0x00,
	0x41, 0x40,
	0x10, 0x00,
	0x45,
	0x04, 0x7f,
	0x41, 0x01,
	0x05,
	0x41, 0x00,
	0x0b,
	0x0b,
}

var wasmProbeMainTraps = []byte{
	0x00, 0x61, 0x73, 0x6d,
	0x01, 0x00, 0x00, 0x00,
	0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f,
	0x03, 0x02, 0x01, 0x00,
	0x05, 0x03, 0x01, 0x00, 0x01,
	0x07, 0x17, 0x02,
	0x06,
	'm', 'e', 'm', 'o', 'r', 'y',
	0x02, 0x00,
	0x0a,
	'p', 'r', 'o', 'b', 'e', '_', 'm', 'a', 'i', 'n',
	0x00, 0x00,
	0x0a, 0x05, 0x01, 0x03,
	0x00,
	0x00,
	0x0b,
}
