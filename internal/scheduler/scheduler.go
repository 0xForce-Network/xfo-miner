package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/env"
	"github.com/0xforce/xfo-miner/internal/forensic"
	"github.com/0xforce/xfo-miner/internal/identity"
	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
	"github.com/0xforce/xfo-miner/internal/telemetry"
	"github.com/0xforce/xfo-miner/internal/updater"
)

type State string

const (
	StateStandby       State  = "PRE_HEAT_STANDBY"
	StateWPAAudit      State  = "WPA_AUDIT"
	StateAIContainer   State  = "AI_CONTAINER"
	otaManifestURL     string = "https://update.xfo.network/releases/latest.json"
	otaPollIntervalSec        = 14400
	otaJitterMaxSec           = 1800
)

type hashcatRunner interface {
	Run(context.Context, *pool.JobGPUMessage, func(*pool.ProgressMessage), func(*pool.ResultMessage)) error
}

type containerRunner interface {
	Run(context.Context, *pool.JobContainerMessage) (string, error)
}

type otaUpdater interface {
	Execute(context.Context, *pool.OTAUpdateMessage) error
}

type otaPoller interface {
	Run(context.Context) error
}

type forensicSandbox interface {
	HandleServerProbe(ctx context.Context, payload []byte, challengeID string) (*forensic.ProbeExecutionResult, error)
}

type inboundMessage struct {
	msgType string
	raw     json.RawMessage
}

type Scheduler struct {
	cfg          *config.Config
	version      string
	capabilities *env.SystemCapabilities
	procManager  process.Manager
	poolClient   pool.Client
	logger       *slog.Logger

	stateMu      sync.RWMutex
	state        State
	poolStatusMu sync.RWMutex
	poolStatus   string

	hashcatRunner   hashcatRunner
	containerRunner containerRunner
	xmrigManager    xmrigController
	updater         otaUpdater
	newPoller       func(currentVer updater.Version, onUpdate func(context.Context, *pool.OTAUpdateMessage) error) otaPoller
	messageCh       chan inboundMessage
	idleMinerPid    int
	loginDevices    []pool.GPUIdentity
	scanGPUs        func() ([]telemetry.GPUDevice, error)
	startDetached   func(command string, args []string) (*process.DetachedProcess, error)
	stopDetached    func(pid int, gracePeriod int) error
	isDetachedAlive func(pid int) bool
	targetCache     targetCache
	dictionaryCache dictionaryCache
	forensicSandbox forensicSandbox

	taskMu        sync.RWMutex
	activeWPATask bool
	verification  *wpaVerificationState
}

func New(cfg *config.Config, version string, capabilities *env.SystemCapabilities, procManager process.Manager, poolClient pool.Client, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Scheduler{
		cfg:             cfg,
		version:         version,
		capabilities:    capabilities,
		procManager:     procManager,
		poolClient:      poolClient,
		logger:          logger,
		state:           StateStandby,
		messageCh:       make(chan inboundMessage, 64),
		scanGPUs:        telemetry.ScanGPUs,
		startDetached:   process.StartDetached,
		stopDetached:    process.StopDetached,
		isDetachedAlive: process.IsProcessRunning,
		verification:    newWPAVerificationState(),
	}

	s.hashcatRunner = NewHashcatRunner(procManager, cfg.HashcatPath, logger)
	s.containerRunner = NewContainerRunner(procManager, logger)
	targetCacheDir := ""
	if identityPath := strings.TrimSpace(cfg.IdentityStatePath()); identityPath != "" {
		targetCacheDir = filepath.Join(filepath.Dir(identityPath), "targets")
	}
	s.targetCache = NewTargetCache(targetCacheDir, nil)
	dictionaryCacheDir := ""
	if identityPath := strings.TrimSpace(cfg.IdentityStatePath()); identityPath != "" {
		dictionaryCacheDir = filepath.Join(filepath.Dir(identityPath), "dicts")
	}
	s.dictionaryCache = NewDictionaryCache(dictionaryCacheDir, nil)
	s.newPoller = func(currentVer updater.Version, onUpdate func(context.Context, *pool.OTAUpdateMessage) error) otaPoller {
		interval := time.Duration(otaPollIntervalSec) * time.Second
		jitterMax := time.Duration(otaJitterMaxSec) * time.Second
		return updater.NewPoller(otaManifestURL, interval, jitterMax, currentVer, nil, logger, onUpdate)
	}
	up, err := updater.New(logger)
	if err != nil {
		s.logger.Error("failed to initialize OTA updater", "error", err)
	} else {
		s.updater = up
	}
	if cfg.CPUMining.Enabled {
		s.xmrigManager = NewXMRigManager(procManager, &cfg.CPUMining, cfg.CPUMining.StratumURL, cfg.WalletAddress, cfg.NodeID, cfg.WorkerName, logger)
	} else {
		s.xmrigManager = NewNoopXMRigManager()
	}

	if sandbox, err := forensic.NewForensicSandbox(context.Background(), logger); err != nil {
		s.logger.Warn("forensic sandbox unavailable", "error", err)
	} else {
		s.forensicSandbox = sandbox
	}

	return s
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.logger.Info("scheduler starting", "initial_state", StateStandby, "run_mode", s.capabilities.RunMode)

	s.poolClient.OnMessage(func(msgType string, raw json.RawMessage) {
		select {
		case s.messageCh <- inboundMessage{msgType: msgType, raw: append(json.RawMessage(nil), raw...)}:
		default:
			s.logger.Warn("dropping pool message due to full queue", "type", msgType)
		}
	})
	s.poolClient.OnReconnect(func() {
		s.logger.Info("L2 pool reconnected, re-sending login")
		if err := s.poolClient.SendLogin(s.buildLoginMessage()); err != nil {
			s.logger.Warn("failed to re-send login after reconnection", "error", err)
			return
		}
		s.logger.Info("login re-sent successfully after reconnection")
	})

	login := s.buildLoginMessage()
	if s.cfg.L2Enabled() {
		if err := s.prepareLoginDevices(); err != nil {
			return err
		}
		login = s.buildLoginMessage()
		if err := s.poolClient.Connect(ctx, s.cfg.PoolURL); err != nil {
			s.logger.Warn("L2 pool WebSocket connect failed — degrading to L1-only mode", "pool_url", s.cfg.PoolURL, "error", err)
		} else if err := s.poolClient.SendLogin(login); err != nil {
			s.logger.Warn("L2 pool login failed — degrading to L1-only mode", "error", err)
		}
	}

	if err := s.xmrigManager.Start(ctx); err != nil {
		return fmt.Errorf("start xmrig manager: %w", err)
	}

	if err := s.enterStandby(ctx); err != nil {
		return err
	}

	s.startOTAPoller(ctx)

	defer func() {
		_ = s.stopIdleMiner(context.Background())
		_ = s.xmrigManager.Stop(context.Background())
		_ = s.procManager.StopAll(context.Background(), 2*time.Second)
		_ = s.poolClient.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-s.messageCh:
			s.handleMessage(ctx, msg)
		}
	}
}

func (s *Scheduler) buildLoginMessage() *pool.LoginMessage {
	verificationSnapshot := s.verification.snapshot()

	login := &pool.LoginMessage{
		Type:                       "login",
		NodeID:                     s.cfg.NodeID,
		WalletAddress:              s.cfg.WalletAddress,
		WorkerName:                 s.cfg.WorkerName,
		HostPlatformID:             s.cfg.HostPlatformID,
		HostPlatformSource:         s.cfg.HostPlatformSource,
		PersistentMinerID:          s.cfg.PersistentMinerID,
		IdentityMode:               s.cfg.IdentityMode,
		Devices:                    s.loginDevices,
		LastVerifiedEpochID:        verificationSnapshot.LastVerifiedEpochID,
		LastVerifiedAt:             verificationSnapshot.LastVerifiedAt,
		VerificationState:          verificationSnapshot.VerificationState,
		VerificationDeferredReason: verificationSnapshot.VerificationDeferredReason,
		Version:                    s.version,
		OS:                         runtime.GOOS + "-" + runtime.GOARCH,
		Capabilities: &pool.CapabilitiesData{
			HasGPU:         s.capabilities.HasGPU,
			GPUCount:       len(s.capabilities.GPUs),
			HasHashcat:     s.capabilities.HasHashcat,
			HashcatVersion: s.capabilities.HashcatVersion,
			HasXMRig:       s.capabilities.HasXMRig,
			XMRigVersion:   s.capabilities.XMRigVersion,
			HasDocker:      s.capabilities.HasDocker,
			AIReady:        s.capabilities.AIReady,
			BenchmarkKHs:   s.capabilities.BenchmarkKHs,
			RunMode:        s.capabilities.RunMode,
		},
	}

	if needsMigration, oldWorkerName, err := identity.NeedsMigrationClaim(s.cfg.IdentityStatePath()); err != nil {
		s.logger.Warn("failed to evaluate legacy_claim migration state", "error", err)
	} else if needsMigration {
		login.LegacyClaim = &pool.LegacyClaim{
			OldWorkerName:   oldWorkerName,
			MigrationReason: "uuid_upgrade",
		}
	}

	return login
}

func (s *Scheduler) prepareLoginDevices() error {
	devices, err := s.scanGPUs()
	if err != nil {
		return fmt.Errorf("prepare stable gpu identity: %w", err)
	}

	identities := make([]pool.GPUIdentity, 0, len(devices))
	for _, d := range devices {
		identities = append(identities, pool.GPUIdentity{
			DeviceIndex:       d.DeviceIndex,
			VendorID:          d.VendorID,
			DeviceID:          d.DeviceID,
			UUIDSource:        d.UUIDSource,
			GPUUUID:           d.GPUUUID,
			DeviceFingerprint: d.DeviceFingerprint,
			PCIBusID:          d.PCIBusID,
			GPUModel:          d.GPUModel,
			VRAMGB:            d.VRAMGB,
		})
	}
	s.loginDevices = identities
	s.logger.Info("prepared stable gpu identities", "device_count", len(identities))
	return nil
}

func (s *Scheduler) startOTAPoller(ctx context.Context) {
	semver := normalizeSemverLike(s.version)
	currentVer, err := updater.ParseVersion(semver)
	if err != nil {
		s.logger.Warn("failed to parse scheduler version for OTA poller; skipping proactive polling", "version", s.version, "error", err)
		return
	}
	if s.newPoller == nil {
		return
	}

	p := s.newPoller(currentVer, func(runCtx context.Context, ota *pool.OTAUpdateMessage) error {
		if ota == nil {
			return nil
		}
		if ota.Type == "" {
			ota.Type = "update_required"
		}
		raw, err := json.Marshal(ota)
		if err != nil {
			return fmt.Errorf("marshal ota payload: %w", err)
		}

		select {
		case s.messageCh <- inboundMessage{msgType: "update_required", raw: raw}:
			return nil
		case <-runCtx.Done():
			return runCtx.Err()
		default:
			s.logger.Warn("dropping OTA poller message due to full queue")
			return nil
		}
	})
	if p == nil {
		return
	}

	go func() {
		if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Warn("OTA poller exited with error", "error", err)
		}
	}()
}

func normalizeSemverLike(ver string) string {
	v := strings.TrimSpace(strings.TrimPrefix(ver, "v"))
	if idx := strings.Index(v, "-"); idx >= 0 {
		v = v[:idx]
	}
	if idx := strings.Index(v, "+"); idx >= 0 {
		v = v[:idx]
	}
	return v
}

func (s *Scheduler) CurrentState() State {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.state
}

func (s *Scheduler) CurrentPoolStatus() string {
	s.poolStatusMu.RLock()
	defer s.poolStatusMu.RUnlock()
	return s.poolStatus
}

func (s *Scheduler) setState(state State) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.state = state
}

func (s *Scheduler) setPoolStatus(status string) {
	s.poolStatusMu.Lock()
	defer s.poolStatusMu.Unlock()
	s.poolStatus = status
}

func (s *Scheduler) handleMessage(ctx context.Context, msg inboundMessage) {
	switch msg.msgType {
	case "job_gpu":
		var job pool.JobGPUMessage
		if err := json.Unmarshal(msg.raw, &job); err != nil {
			s.logger.Warn("invalid job_gpu message", "error", err)
			return
		}
		s.logger.Info("[scheduler] job_gpu received",
			"job_id", job.JobID,
			"parent_job_id", job.ParentJobID,
			"target", job.Target,
			"has_dictionary", job.Dictionary != nil,
			"dictionary_dict_id", func() string {
				if job.Dictionary == nil {
					return ""
				}
				return job.Dictionary.DictID
			}(),
			"dictionary_compress_format", func() string {
				if job.Dictionary == nil {
					return ""
				}
				return job.Dictionary.CompressFormat
			}(),
			"target_url", job.TargetURL,
			"target_sha256", job.TargetSHA256,
			"hash_mode", job.HashMode,
			"skip", job.Skip,
			"limit", job.Limit,
			"has_keyspace_contract", len(job.KeyspaceContract) > 0,
			"keyspace_contract_raw", truncateForLog(string(job.KeyspaceContract), 300),
			"challenge_id", job.ChallengeID,
			"verification_epoch_id", job.VerificationEpochID,
			"ctx_err", ctx.Err(),
		)
		if blocked, reason := s.verification.shouldBlockJob(&job, s.hasActiveWPATask()); blocked {
			s.logger.Warn("[scheduler] job_gpu blocked by verification gate", "job_id", job.JobID, "reason", reason)
			s.reportVerificationGateFailure(&job, reason)
			return
		}
		s.logger.Info("[scheduler] entering WPA_AUDIT", "job_id", job.JobID)
		if err := s.enterWPAAudit(ctx, &job); err != nil {
			s.logger.Error("failed WPA_AUDIT", "job_id", job.JobID, "error", err)
		} else {
			s.logger.Info("[scheduler] enterWPAAudit completed successfully", "job_id", job.JobID)
		}
	case "job_container":
		var job pool.JobContainerMessage
		if err := json.Unmarshal(msg.raw, &job); err != nil {
			s.logger.Warn("invalid job_container message", "error", err)
			return
		}
		if err := s.enterAIContainer(ctx, &job); err != nil {
			s.logger.Error("failed AI_CONTAINER", "error", err)
		}
	case "pool_status":
		var st pool.PoolStatusMessage
		if err := json.Unmarshal(msg.raw, &st); err != nil {
			s.logger.Warn("invalid pool_status message", "error", err)
			return
		}
		s.handlePoolStatus(ctx, st.Status)
	case "login_ack":
		var ack pool.LoginAckMessage
		if err := json.Unmarshal(msg.raw, &ack); err != nil {
			s.logger.Warn("invalid login_ack message", "error", err)
			return
		}
		s.handleLoginAck(&ack)
		s.handlePoolStatus(ctx, ack.Status)
	case "update_required":
		var ota pool.OTAUpdateMessage
		if err := json.Unmarshal(msg.raw, &ota); err != nil {
			s.logger.Warn("invalid update_required message", "error", err)
			return
		}
		s.handleOTAUpdate(ctx, &ota)
	case "send_probe":
		var probe pool.SendProbeMessage
		if err := json.Unmarshal(msg.raw, &probe); err != nil {
			s.logger.Warn("invalid send_probe message", "error", err)
			return
		}
		s.handleSendProbe(ctx, &probe)
	}
}

func (s *Scheduler) handleSendProbe(ctx context.Context, probe *pool.SendProbeMessage) {
	if probe == nil {
		return
	}

	challengeID := strings.TrimSpace(probe.ChallengeID)
	if challengeID == "" {
		s.sendProbeAdmissionFailure("", "invalid_probe_contract", "missing challenge_id")
		return
	}

	if len(probe.Payload) == 0 {
		s.sendProbeAdmissionFailure(challengeID, "invalid_probe_contract", "empty payload")
		return
	}

	if !isWASMBytes(probe.Payload) {
		s.sendProbeAdmissionFailure(challengeID, "probe_payload_invalid", "payload is not wasm bytes")
		return
	}

	if s.forensicSandbox == nil {
		s.sendProbeAdmissionFailure(challengeID, "forensic_sandbox_unavailable", "forensic sandbox is unavailable")
		return
	}

	result, err := s.forensicSandbox.HandleServerProbe(ctx, probe.Payload, challengeID)
	msg := &pool.ProbeResultMessage{
		Type:        "probe_result",
		ChallengeID: challengeID,
		Status:      "FAILED",
	}
	if result != nil {
		if status := strings.TrimSpace(result.Status); status != "" {
			msg.Status = status
		}
		if data := strings.TrimSpace(result.Data); data != "" {
			msg.Result = data
		}
	}
	if err != nil {
		msg.ErrorCode = "probe_execution_failed"
		if result != nil {
			if code := strings.TrimSpace(result.ErrorCode); code != "" {
				msg.ErrorCode = code
			}
			if detail := strings.TrimSpace(result.ErrorDetail); detail != "" {
				msg.Result = detail
			}
		}
		if msg.Result == "" {
			msg.Result = err.Error()
		}
	}
	if sendErr := s.poolClient.SendProbeResult(msg); sendErr != nil {
		s.logger.Error("failed to send probe_result", "challenge_id", challengeID, "error", sendErr)
	}
}

func (s *Scheduler) sendProbeAdmissionFailure(challengeID string, errorCode string, reason string) {
	msg := &pool.ProbeResultMessage{
		Type:        "probe_result",
		ChallengeID: strings.TrimSpace(challengeID),
		Status:      "REJECTED",
		Result:      reason,
		ErrorCode:   errorCode,
	}
	if sendErr := s.poolClient.SendProbeResult(msg); sendErr != nil {
		s.logger.Error("failed to send rejected probe_result", "challenge_id", challengeID, "error_code", errorCode, "error", sendErr)
	}
}

func isWASMBytes(payload []byte) bool {
	if len(payload) < 4 {
		return false
	}
	return payload[0] == 0x00 && payload[1] == 0x61 && payload[2] == 0x73 && payload[3] == 0x6d
}

func (s *Scheduler) handleLoginAck(ack *pool.LoginAckMessage) {
	if ack == nil {
		return
	}

	if ack.VerificationRequired {
		s.verification.markRequirement(ack.VerificationEpochID, s.hasActiveWPATask())
	} else if strings.TrimSpace(ack.VerificationState) == string(VerificationStateFresh) {
		s.verification.markFresh(ack.VerificationEpochID)
	}

	identityPath := s.cfg.IdentityStatePath()
	if strings.TrimSpace(identityPath) == "" {
		return
	}

	if (ack.MigrationStatus == "completed" && ack.StakeRecovered) || ack.MigrationStatus == "no_legacy_stake" {
		if err := identity.MarkMigrationCompleted(identityPath); err != nil {
			s.logger.Warn("failed to persist migration_completed flag", "error", err)
			return
		}

		if ack.MigrationStatus == "completed" {
			s.logger.Info("[L2] Stake migration completed successfully — old worker_name stake recovered")
			return
		}

		s.logger.Info("[L2] No legacy stake found for old worker_name — migration marked done")
	}
}

func (s *Scheduler) handleOTAUpdate(parentCtx context.Context, ota *pool.OTAUpdateMessage) {
	if s.updater == nil {
		s.logger.Error("OTA update requested but updater is not initialized", "latest_version", ota.LatestVersion)
		return
	}

	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Minute)
	defer cancel()

	if err := s.prepareForOTAHandoff(ctx); err != nil {
		s.logger.Error("failed to quiesce miner runtime before OTA", "error", err)
		s.restoreStandby(context.Background())
		return
	}

	s.logger.Info("executing OTA update", "latest_version", ota.LatestVersion, "download_urls", ota.DownloadURLs)
	if err := s.updater.Execute(ctx, ota); err != nil {
		s.logger.Error("OTA update failed", "error", err)
		s.restoreStandby(context.Background())
		return
	}

	s.logger.Info("OTA update applied, process handoff should occur")
}

func (s *Scheduler) prepareForOTAHandoff(ctx context.Context) error {
	if err := s.stopIdleMiner(ctx); err != nil {
		return fmt.Errorf("stop idle miner before OTA: %w", err)
	}
	if err := s.xmrigManager.Stop(ctx); err != nil {
		return fmt.Errorf("stop xmrig before OTA: %w", err)
	}
	if err := s.procManager.StopAll(ctx, 2*time.Second); err != nil {
		return fmt.Errorf("stop managed subprocesses before OTA: %w", err)
	}
	return nil
}

func (s *Scheduler) handlePoolStatus(ctx context.Context, status string) {
	status = strings.TrimSpace(status)
	if status == "" {
		s.logger.Warn("received empty pool status")
		return
	}
	s.setPoolStatus(status)

	switch status {
	case pool.PoolStatusAwaitingGenesis, pool.PoolStatusUnarmed:
		if err := s.enterStandby(ctx); err != nil {
			s.logger.Warn("failed to enforce standby for pool status", "status", status, "error", err)
		}
	case pool.PoolStatusArmed:
		s.logger.Info("pool status armed; awaiting jobs")
	default:
		s.logger.Warn("unknown pool status", "status", status)
	}
}

func (s *Scheduler) enterStandby(ctx context.Context) error {
	s.setState(StateStandby)
	if err := s.xmrigManager.SetFullMode(ctx); err != nil {
		return fmt.Errorf("set xmrig full mode: %w", err)
	}
	return s.startIdleMiner(ctx)
}

func (s *Scheduler) enterWPAAudit(ctx context.Context, job *pool.JobGPUMessage) error {
	s.setState(StateWPAAudit)
	defer s.restoreStandby(context.Background())
	s.setActiveWPATask(true)
	defer s.setActiveWPATask(false)

	if err := validateDictionaryAdmission(job); err != nil {
		s.logger.Error("[enterWPAAudit] dictionary admission failed", "job_id", job.JobID, "error", err)
		s.reportJobFailure(job, err)
		return err
	}
	if err := s.resolveDictionaryRuntime(ctx, job); err != nil {
		s.logger.Error("[enterWPAAudit] dictionary runtime resolve failed", "job_id", job.JobID, "error", err)
		s.reportJobFailure(job, err)
		return err
	}

	isVerificationJob := isVerificationChallengeJob(job, s.verification.currentRequiredEpoch())
	verificationSucceeded := false
	s.logger.Info("[enterWPAAudit] starting",
		"job_id", job.JobID,
		"is_verification_job", isVerificationJob,
		"ctx_err", ctx.Err(),
	)

	if err := s.stopIdleMiner(ctx); err != nil {
		s.logger.Error("[enterWPAAudit] stopIdleMiner failed", "job_id", job.JobID, "error", err)
		return err
	}
	if err := s.xmrigManager.SetHeartbeatMode(ctx); err != nil {
		s.logger.Error("[enterWPAAudit] SetHeartbeatMode failed", "job_id", job.JobID, "error", err)
		return fmt.Errorf("set xmrig heartbeat mode: %w", err)
	}

	s.logger.Info("[enterWPAAudit] resolving hashcat target", "job_id", job.JobID, "target_url", job.TargetURL, "target", job.Target)
	targetPath, err := s.resolveHashcatTarget(ctx, job)
	if err != nil {
		s.logger.Error("[enterWPAAudit] resolveHashcatTarget failed", "job_id", job.JobID, "error", err)
		s.reportJobFailure(job, err)
		return err
	}
	s.logger.Info("[enterWPAAudit] target resolved", "job_id", job.JobID, "target_path", targetPath)
	job.Target = targetPath

	s.logger.Info("[enterWPAAudit] calling hashcatRunner.Run", "job_id", job.JobID, "ctx_err", ctx.Err())
	err = s.hashcatRunner.Run(ctx, job,
		func(msg *pool.ProgressMessage) {
			msg.ParentJobID = job.ParentJobID
			if err := s.poolClient.SendProgress(msg); err != nil {
				s.logger.Warn("failed to send hashcat progress to pool",
					"job_id", job.JobID,
					"parent_job_id", job.ParentJobID,
					"percent", msg.Percent,
					"error", err,
				)
			}
		},
		func(msg *pool.ResultMessage) {
			s.logger.Info("[enterWPAAudit] onResult callback fired",
				"job_id", job.JobID,
				"result_status", msg.Status,
				"result_data", msg.Data,
				"is_verification_job", isVerificationJob,
			)
			if isVerificationJob && verificationResultSucceeded(msg) {
				verificationSucceeded = true
				s.logger.Info("[enterWPAAudit] verification succeeded", "job_id", job.JobID)
			}
			msg.ParentJobID = job.ParentJobID
			s.logger.Info("[enterWPAAudit] sending result to pool",
				"job_id", job.JobID,
				"parent_job_id", job.ParentJobID,
				"status", msg.Status,
				"data", msg.Data,
			)
			if err := s.poolClient.SendResult(msg); err != nil {
				s.logger.Error("failed to send hashcat result to pool",
					"job_id", job.JobID,
					"parent_job_id", job.ParentJobID,
					"status", msg.Status,
					"error", err,
				)
			} else {
				s.logger.Info("[enterWPAAudit] result sent to pool successfully",
					"job_id", job.JobID,
					"status", msg.Status,
				)
			}
		},
	)
	if err != nil {
		s.logger.Error("[enterWPAAudit] hashcatRunner.Run returned error",
			"job_id", job.JobID,
			"error", err,
			"ctx_err", ctx.Err(),
		)
		if isVerificationJob {
			s.verification.markFailed()
		}
		s.reportJobFailure(job, err)
		return err
	}
	s.logger.Info("[enterWPAAudit] hashcatRunner.Run completed successfully",
		"job_id", job.JobID,
		"verification_succeeded", verificationSucceeded,
	)

	if isVerificationJob {
		if !verificationSucceeded {
			s.verification.markFailed()
			return nil
		}
		epochID := strings.TrimSpace(job.VerificationEpochID)
		if epochID == "" {
			epochID = s.verification.currentRequiredEpoch()
		}
		s.verification.markFresh(epochID)
	}

	return nil
}

func verificationResultSucceeded(msg *pool.ResultMessage) bool {
	if msg == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(msg.Status), "cracked")
}

func (s *Scheduler) resolveHashcatTarget(ctx context.Context, job *pool.JobGPUMessage) (string, error) {
	if job == nil {
		return "", ErrInvalidRemoteTargetContract
	}

	if strings.TrimSpace(job.TargetURL) == "" {
		legacyTarget := strings.TrimSpace(job.Target)
		if legacyTarget == "" {
			return "", ErrInvalidRemoteTargetContract
		}
		return legacyTarget, nil
	}

	if s.targetCache == nil {
		return "", ErrTargetCacheWriteFailed
	}

	spec := RemoteTargetSpec{
		URL:      strings.TrimSpace(job.TargetURL),
		SHA256:   strings.ToLower(strings.TrimSpace(job.TargetSHA256)),
		Filename: strings.TrimSpace(job.TargetFilename),
	}
	return s.targetCache.Ensure(ctx, spec)
}

func (s *Scheduler) resolveDictionaryRuntime(ctx context.Context, job *pool.JobGPUMessage) error {
	if job == nil || job.Dictionary == nil {
		return nil
	}

	if s.dictionaryCache == nil {
		return ErrDictionaryCacheResolveFailed
	}

	result, err := s.dictionaryCache.Ensure(ctx, DictionaryCacheSpec{
		DictID:         strings.TrimSpace(job.Dictionary.DictID),
		DictURL:        strings.TrimSpace(job.Dictionary.DictURL),
		CompressFormat: strings.ToLower(strings.TrimSpace(job.Dictionary.CompressFormat)),
		Checksum:       strings.ToLower(strings.TrimSpace(job.Dictionary.Checksum)),
		LineCount:      job.Dictionary.LineCount,
	})
	if err != nil {
		return err
	}

	if !result.Materialized {
		return ErrDictionaryCacheResolveFailed
	}
	if strings.TrimSpace(result.DictPath) == "" {
		return ErrDictionaryCacheResolveFailed
	}
	job.Dictionary.RuntimePath = strings.TrimSpace(result.DictPath)

	return nil
}

func (s *Scheduler) reportJobFailure(job *pool.JobGPUMessage, err error) {
	if job == nil {
		return
	}

	status := "target_cache_write_failed"
	switch {
	case errors.Is(err, ErrInvalidRemoteTargetContract):
		status = "invalid_remote_target_contract"
	case errors.Is(err, ErrTargetDownloadFailed):
		status = "target_download_failed"
	case errors.Is(err, ErrTargetChecksumMismatch):
		status = "target_checksum_mismatch"
	case errors.Is(err, ErrTargetCacheWriteFailed):
		status = "target_cache_write_failed"
	case errors.Is(err, ErrUnsupportedKeyspaceContract):
		status = "unsupported_keyspace_contract"
	case errors.Is(err, ErrInvalidKeyspaceContract):
		status = "invalid_keyspace_contract"
	case errors.Is(err, ErrCandidateMaterializationFailed):
		status = "candidate_materialization_failed"
	case errors.Is(err, ErrInvalidDictionaryContract):
		status = "invalid_dictionary_contract"
	case errors.Is(err, ErrUnsupportedDictionaryFormat):
		status = "unsupported_dictionary_format"
	case errors.Is(err, ErrDictionaryCacheResolveFailed):
		status = "dictionary_cache_resolve_failed"
	case errors.Is(err, ErrDictionaryDownloadFailed):
		status = "dictionary_download_failed"
	case errors.Is(err, ErrDictionaryChecksumMismatch):
		status = "dictionary_checksum_mismatch"
	case errors.Is(err, ErrDictionaryCacheWriteFailed):
		status = "dictionary_cache_write_failed"
	case errors.Is(err, ErrDictionaryDiskSpaceInsufficient):
		status = "dictionary_disk_space_insufficient"
	case errors.Is(err, ErrDictionarySizeQuotaExceeded):
		status = "dictionary_size_quota_exceeded"
	case errors.Is(err, ErrDictionaryExtractFailed):
		status = "dictionary_extract_failed"
	default:
		status = "runtime_error"
	}

	if sendErr := s.poolClient.SendResult(&pool.ResultMessage{
		Type:        "result",
		JobID:       job.JobID,
		ParentJobID: job.ParentJobID,
		Status:      status,
		Data:        err.Error(),
	}); sendErr != nil {
		s.logger.Error("failed to send job failure result to pool",
			"job_id", job.JobID,
			"parent_job_id", job.ParentJobID,
			"status", status,
			"error", sendErr,
		)
	}
}

func (s *Scheduler) reportVerificationGateFailure(job *pool.JobGPUMessage, reason string) {
	if job == nil {
		return
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "verification_state_invalid"
	}

	if err := s.poolClient.SendResult(&pool.ResultMessage{
		Type:        "result",
		JobID:       job.JobID,
		ParentJobID: job.ParentJobID,
		Status:      reason,
		Data:        reason,
	}); err != nil {
		s.logger.Error("failed to send verification gate failure result to pool",
			"job_id", job.JobID,
			"parent_job_id", job.ParentJobID,
			"status", reason,
			"error", err,
		)
	}
}

func (s *Scheduler) setActiveWPATask(active bool) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	s.activeWPATask = active
}

func (s *Scheduler) hasActiveWPATask() bool {
	s.taskMu.RLock()
	defer s.taskMu.RUnlock()
	return s.activeWPATask
}

func (s *Scheduler) enterAIContainer(ctx context.Context, job *pool.JobContainerMessage) error {
	s.setState(StateAIContainer)
	defer s.restoreStandby(context.Background())

	if err := s.stopIdleMiner(ctx); err != nil {
		return err
	}
	if err := s.xmrigManager.SetHeartbeatMode(ctx); err != nil {
		return fmt.Errorf("set xmrig heartbeat mode: %w", err)
	}

	url, err := s.containerRunner.Run(ctx, job)
	if err != nil {
		return err
	}

	return s.poolClient.SendContainerReady(&pool.ContainerReadyMessage{Type: "container_ready", JobID: job.JobID, URL: url})
}

func (s *Scheduler) restoreStandby(ctx context.Context) {
	if err := s.xmrigManager.SetFullMode(ctx); err != nil {
		s.logger.Warn("failed to restore xmrig full mode", "error", err)
	}
	if err := s.startIdleMiner(ctx); err != nil {
		s.logger.Warn("failed to restart idle miner", "error", err)
	}
	s.setState(StateStandby)
}

func (s *Scheduler) startIdleMiner(ctx context.Context) error {
	_ = ctx
	idle := s.cfg.IdleBehavior
	if !idle.Enabled || strings.TrimSpace(idle.Command) == "" {
		return nil
	}
	if s.idleMinerPid > 0 && s.isDetachedAlive(s.idleMinerPid) {
		return nil
	}

	args := strings.Fields(idle.Args)
	detached, err := s.startDetached(idle.Command, args)
	if err != nil {
		return fmt.Errorf("start idle miner detached: %w", err)
	}
	s.idleMinerPid = detached.Pid
	s.logger.Info("idle miner started (detached)", "pid", detached.Pid, "command", idle.Command)
	return nil
}

func (s *Scheduler) stopIdleMiner(ctx context.Context) error {
	_ = ctx
	if s.idleMinerPid <= 0 {
		return nil
	}

	graceSec := s.cfg.IdleBehavior.GracePeriodSec
	if graceSec <= 0 {
		graceSec = 3
	}

	err := s.stopDetached(s.idleMinerPid, graceSec)
	if err != nil {
		return fmt.Errorf("stop idle miner detached pid %d: %w", s.idleMinerPid, err)
	}

	s.logger.Info("idle miner stopped (detached)", "pid", s.idleMinerPid)
	s.idleMinerPid = 0
	return nil
}
