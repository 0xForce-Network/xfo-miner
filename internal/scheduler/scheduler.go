package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/env"
	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
	"github.com/0xforce/xfo-miner/internal/updater"
)

type State string

const (
	StateStandby     State = "PRE_HEAT_STANDBY"
	StateWPAAudit    State = "WPA_AUDIT"
	StateAIContainer State = "AI_CONTAINER"
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
	messageCh       chan inboundMessage
}

func New(cfg *config.Config, version string, capabilities *env.SystemCapabilities, procManager process.Manager, poolClient pool.Client, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Scheduler{
		cfg:          cfg,
		version:      version,
		capabilities: capabilities,
		procManager:  procManager,
		poolClient:   poolClient,
		logger:       logger,
		state:        StateStandby,
		messageCh:    make(chan inboundMessage, 64),
	}

	s.hashcatRunner = NewHashcatRunner(procManager, logger)
	s.containerRunner = NewContainerRunner(procManager, logger)
	up, err := updater.New(logger)
	if err != nil {
		s.logger.Error("failed to initialize OTA updater", "error", err)
	} else {
		s.updater = up
	}
	if cfg.CPUMining.Enabled {
		s.xmrigManager = NewXMRigManager(procManager, &cfg.CPUMining, cfg.PoolURL, cfg.NodeID, cfg.WorkerName, logger)
	} else {
		s.xmrigManager = NewNoopXMRigManager()
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

	if err := s.poolClient.Connect(ctx, s.cfg.PoolURL); err != nil {
		return fmt.Errorf("connect pool: %w", err)
	}

	login := &pool.LoginMessage{
		Type:       "login",
		NodeID:     s.cfg.NodeID,
		WorkerName: s.cfg.WorkerName,
		Version:    s.version,
		OS:         runtime.GOOS + "-" + runtime.GOARCH,
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
	if err := s.poolClient.SendLogin(login); err != nil {
		return fmt.Errorf("send login: %w", err)
	}

	if err := s.xmrigManager.Start(ctx); err != nil {
		return fmt.Errorf("start xmrig manager: %w", err)
	}

	if err := s.enterStandby(ctx); err != nil {
		return err
	}

	defer func() {
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
		if err := s.enterWPAAudit(ctx, &job); err != nil {
			s.logger.Error("failed WPA_AUDIT", "error", err)
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
		s.handlePoolStatus(ctx, ack.Status)
	case "update_required":
		var ota pool.OTAUpdateMessage
		if err := json.Unmarshal(msg.raw, &ota); err != nil {
			s.logger.Warn("invalid update_required message", "error", err)
			return
		}
		s.handleOTAUpdate(ctx, &ota)
	}
}

func (s *Scheduler) handleOTAUpdate(_ context.Context, ota *pool.OTAUpdateMessage) {
	if !s.cfg.AutoUpdate.Enabled {
		s.logger.Warn("OTA update available but auto_update is disabled", "latest_version", ota.LatestVersion)
		return
	}

	if s.updater == nil {
		s.logger.Error("OTA update requested but updater is not initialized", "latest_version", ota.LatestVersion)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := s.stopIdleMiner(ctx); err != nil {
		s.logger.Error("failed to stop idle miner before OTA", "error", err)
	}
	if err := s.xmrigManager.SetHeartbeatMode(ctx); err != nil {
		s.logger.Error("failed to set xmrig heartbeat mode before OTA", "error", err)
	}

	s.logger.Info("executing OTA update", "latest_version", ota.LatestVersion, "download_urls", ota.DownloadURLs)
	if err := s.updater.Execute(ctx, ota); err != nil {
		s.logger.Error("OTA update failed", "error", err)
		s.restoreStandby(context.Background())
		return
	}

	s.logger.Info("OTA update applied, process handoff should occur")
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

	if err := s.stopIdleMiner(ctx); err != nil {
		return err
	}
	if err := s.xmrigManager.SetHeartbeatMode(ctx); err != nil {
		return fmt.Errorf("set xmrig heartbeat mode: %w", err)
	}

	return s.hashcatRunner.Run(ctx, job,
		func(msg *pool.ProgressMessage) {
			_ = s.poolClient.SendProgress(msg)
		},
		func(msg *pool.ResultMessage) {
			_ = s.poolClient.SendResult(msg)
		},
	)
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
	idle := s.cfg.IdleBehavior
	if !idle.Enabled || strings.TrimSpace(idle.Command) == "" {
		return nil
	}
	if s.procManager.IsRunning("idle_miner") {
		return nil
	}
	args := strings.Fields(idle.Args)
	_, err := s.procManager.Start(ctx, "idle_miner", idle.Command, args)
	if err != nil {
		return fmt.Errorf("start idle miner: %w", err)
	}
	return nil
}

func (s *Scheduler) stopIdleMiner(ctx context.Context) error {
	grace := time.Duration(s.cfg.IdleBehavior.GracePeriodSec) * time.Second
	if grace <= 0 {
		grace = 3 * time.Second
	}
	return s.procManager.Stop(ctx, "idle_miner", grace)
}
