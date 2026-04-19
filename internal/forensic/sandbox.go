package forensic

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

const (
	HostModuleNamespace = "xfo.forensic"

	HostBindingAnalyzeMemoryTemporalAnomalies  = "analyze_memory_temporal_anomalies"
	HostBindingGetHardwareTrustAnchorSignature = "get_hardware_trust_anchor_signature"
	HostBindingVerifyExecutionLatencyProfile   = "verify_execution_latency_profile"
	HostBindingReadVRAMEntropySample           = "read_vram_entropy_sample"
	HostBindingReadChallengeID                 = "read_challenge_id"
)

var (
	ErrProbePayloadInvalid      = errors.New("forensic probe payload invalid")
	ErrProbeModuleMissingExport = errors.New("forensic probe module missing required export")
	ErrProbeExecutionTrapped    = errors.New("forensic probe execution trapped")
	ErrProbeRuntime             = errors.New("forensic probe runtime error")
)

// ProbeExecutionResult defines the MINER_198 execution/result mapping contract.
type ProbeExecutionResult struct {
	ChallengeID string
	Status      string
	Data        string
	ErrorCode   string
	ErrorDetail string
}

// ForensicSandbox is a visible, fail-closed WASM runtime scaffold.
//
// Safety boundary:
//   - No WASI modules are instantiated.
//   - No filesystem, shell, process, env, or network host imports are registered.
//   - Guest modules can only consume the explicitly registered forensic stub imports.
type ForensicSandbox struct {
	logger  *slog.Logger
	runtime wazero.Runtime

	challengeMu   sync.RWMutex
	challengeID   string
	challengeData []byte
}

func NewForensicSandbox(ctx context.Context, logger *slog.Logger) (*ForensicSandbox, error) {
	if logger == nil {
		logger = slog.Default()
	}

	runtime := wazero.NewRuntime(ctx)
	builder := runtime.NewHostModuleBuilder(HostModuleNamespace)

	s := &ForensicSandbox{
		logger:  logger,
		runtime: runtime,
	}

	builder.NewFunctionBuilder().WithFunc(func() uint32 {
		return 0
	}).Export(HostBindingAnalyzeMemoryTemporalAnomalies)

	builder.NewFunctionBuilder().WithFunc(func() uint64 {
		return 0x58464f
	}).Export(HostBindingGetHardwareTrustAnchorSignature)

	builder.NewFunctionBuilder().WithFunc(func() uint32 {
		return 3
	}).Export(HostBindingVerifyExecutionLatencyProfile)

	builder.NewFunctionBuilder().WithFunc(func() uint32 {
		return 0
	}).Export(HostBindingReadVRAMEntropySample)

	builder.NewFunctionBuilder().WithFunc(func(_ context.Context, mod api.Module, ptr uint32, maxLen uint32) uint32 {
		return s.writeChallengeIDToGuestMemory(mod, ptr, maxLen)
	}).Export(HostBindingReadChallengeID)

	if _, err := builder.Instantiate(ctx); err != nil {
		_ = runtime.Close(ctx)
		return nil, err
	}

	return s, nil
}

func (s *ForensicSandbox) RegisteredHostBindings() []string {
	return []string{
		HostBindingAnalyzeMemoryTemporalAnomalies,
		HostBindingGetHardwareTrustAnchorSignature,
		HostBindingVerifyExecutionLatencyProfile,
		HostBindingReadVRAMEntropySample,
		HostBindingReadChallengeID,
	}
}

func (s *ForensicSandbox) HandleServerProbe(ctx context.Context, payload []byte, challengeID string) (*ProbeExecutionResult, error) {
	challengeID = strings.TrimSpace(challengeID)
	if len(payload) == 0 {
		return &ProbeExecutionResult{
			ChallengeID: challengeID,
			Status:      "FAILED",
			ErrorCode:   "probe_payload_invalid",
			ErrorDetail: "empty payload",
		}, ErrProbePayloadInvalid
	}

	compiled, err := s.runtime.CompileModule(ctx, payload)
	if err != nil {
		return &ProbeExecutionResult{
			ChallengeID: challengeID,
			Status:      "FAILED",
			ErrorCode:   "probe_payload_invalid",
			ErrorDetail: err.Error(),
		}, ErrProbePayloadInvalid
	}
	defer func() {
		_ = compiled.Close(ctx)
	}()

	module, err := s.runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
	if err != nil {
		return &ProbeExecutionResult{
			ChallengeID: challengeID,
			Status:      "FAILED",
			ErrorCode:   "probe_runtime_error",
			ErrorDetail: err.Error(),
		}, ErrProbeRuntime
	}
	defer func() {
		_ = module.Close(ctx)
	}()

	if module.ExportedMemory("memory") == nil {
		return &ProbeExecutionResult{
			ChallengeID: challengeID,
			Status:      "FAILED",
			ErrorCode:   "probe_module_missing_export",
			ErrorDetail: "missing memory export",
		}, ErrProbeModuleMissingExport
	}

	probeMain := module.ExportedFunction("probe_main")
	if probeMain == nil {
		return &ProbeExecutionResult{
			ChallengeID: challengeID,
			Status:      "FAILED",
			ErrorCode:   "probe_module_missing_export",
			ErrorDetail: "missing probe_main export",
		}, ErrProbeModuleMissingExport
	}

	s.setCurrentChallengeID(challengeID)
	defer s.setCurrentChallengeID("")

	results, err := probeMain.Call(ctx)
	if err != nil {
		errCode := "probe_runtime_error"
		errKind := ErrProbeRuntime
		if looksLikeTrap(err) {
			errCode = "probe_execution_trapped"
			errKind = ErrProbeExecutionTrapped
		}

		return &ProbeExecutionResult{
			ChallengeID: challengeID,
			Status:      "FAILED",
			ErrorCode:   errCode,
			ErrorDetail: err.Error(),
		}, errKind
	}

	if len(results) == 0 {
		return &ProbeExecutionResult{
			ChallengeID: challengeID,
			Status:      "FAILED",
			ErrorCode:   "probe_runtime_error",
			ErrorDetail: "probe_main returned no result",
		}, ErrProbeRuntime
	}

	if results[0] != 0 {
		return &ProbeExecutionResult{
			ChallengeID: challengeID,
			Status:      "FAILED",
			ErrorCode:   "probe_runtime_error",
			ErrorDetail: "probe_main returned non-zero exit code",
		}, ErrProbeRuntime
	}

	return &ProbeExecutionResult{
		ChallengeID: challengeID,
		Status:      "OK",
		Data:        "OK",
	}, nil
}

func (s *ForensicSandbox) setCurrentChallengeID(challengeID string) {
	challengeID = strings.TrimSpace(challengeID)

	s.challengeMu.Lock()
	defer s.challengeMu.Unlock()

	s.challengeID = challengeID
	if challengeID == "" {
		s.challengeData = nil
		return
	}
	s.challengeData = []byte(challengeID)
}

func (s *ForensicSandbox) writeChallengeIDToGuestMemory(mod api.Module, ptr uint32, maxLen uint32) uint32 {
	if mod == nil || mod.Memory() == nil {
		return 0
	}

	s.challengeMu.RLock()
	challengeData := append([]byte(nil), s.challengeData...)
	s.challengeMu.RUnlock()

	if len(challengeData) == 0 {
		return 0
	}

	if maxLen == 0 {
		return uint32(len(challengeData))
	}

	toWrite := len(challengeData)
	if toWrite > int(maxLen) {
		toWrite = int(maxLen)
	}

	if ok := mod.Memory().Write(ptr, challengeData[:toWrite]); !ok {
		return 0
	}

	return uint32(toWrite)
}

func looksLikeTrap(err error) bool {
	if err == nil {
		return false
	}
	errText := strings.ToLower(err.Error())
	return strings.Contains(errText, "trap") || strings.Contains(errText, "unreachable")
}

func (s *ForensicSandbox) Close(ctx context.Context) error {
	if s == nil || s.runtime == nil {
		return nil
	}

	return s.runtime.Close(ctx)
}
