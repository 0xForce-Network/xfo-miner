package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/process"
)

const (
	xmrigModeFull      = "full"
	xmrigModeHeartbeat = "heartbeat"
	xmrigProcessName   = "xmrig_l1"
)

type xmrigController interface {
	Start(context.Context) error
	SetFullMode(context.Context) error
	SetHeartbeatMode(context.Context) error
	Stop(context.Context) error
}

type XMRigManager struct {
	procManager   process.Manager
	cfg           *config.CPUMiningConfig
	logger        *slog.Logger
	stratumURL    string
	walletAddress string
	nodeID        string
	workerName    string

	httpPort               int
	httpClient             *http.Client
	apiBaseURL             string
	watchdogInitialBackoff time.Duration
	watchdogMaxBackoff     time.Duration

	mu          sync.Mutex
	currentMode string
	generation  uint64
	stopped     bool
	stopCh      chan struct{}
}

func NewXMRigManager(procManager process.Manager, cfg *config.CPUMiningConfig, stratumURL string, walletAddress string, nodeID string, workerName string, logger *slog.Logger) *XMRigManager {
	if logger == nil {
		logger = slog.Default()
	}
	port := allocateXMRigHTTPPort()
	return &XMRigManager{
		procManager:            procManager,
		cfg:                    cfg,
		logger:                 logger,
		stratumURL:             stratumURL,
		walletAddress:          walletAddress,
		nodeID:                 nodeID,
		workerName:             workerName,
		httpPort:               port,
		httpClient:             &http.Client{Timeout: 5 * time.Second},
		watchdogInitialBackoff: time.Second,
		watchdogMaxBackoff:     30 * time.Second,
		stopCh:                 make(chan struct{}),
	}
}

func NewNoopXMRigManager() xmrigController {
	return &noopXMRigManager{}
}

func (m *XMRigManager) Start(ctx context.Context) error {
	if !m.cfg.Enabled {
		return nil
	}
	return m.restartWithMode(ctx, xmrigModeFull, m.cfg.MaxThreads)
}

func (m *XMRigManager) SetFullMode(ctx context.Context) error {
	if !m.cfg.Enabled {
		return nil
	}
	m.mu.Lock()
	currentMode := m.currentMode
	m.mu.Unlock()
	if currentMode == xmrigModeFull && m.procManager.IsRunning(xmrigProcessName) {
		return nil
	}
	return m.applyMode(ctx, xmrigModeFull, m.cfg.MaxThreads)
}

func (m *XMRigManager) SetHeartbeatMode(ctx context.Context) error {
	if !m.cfg.Enabled {
		return nil
	}
	m.mu.Lock()
	currentMode := m.currentMode
	m.mu.Unlock()
	if currentMode == xmrigModeHeartbeat && m.procManager.IsRunning(xmrigProcessName) {
		return nil
	}
	return m.applyMode(ctx, xmrigModeHeartbeat, m.cfg.BackgroundThreads)
}

func (m *XMRigManager) Stop(ctx context.Context) error {
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.stopCh)
	}
	m.currentMode = ""
	m.mu.Unlock()
	return m.procManager.Stop(ctx, xmrigProcessName, 3*time.Second)
}

func (m *XMRigManager) applyMode(ctx context.Context, mode string, threads int) error {
	if !m.procManager.IsRunning(xmrigProcessName) {
		return m.restartWithMode(ctx, mode, threads)
	}

	const maxAPIAttempts = 3
	const apiRetryDelay = time.Second
	var lastErr error
	for attempt := 1; attempt <= maxAPIAttempts; attempt++ {
		lastErr = m.setThreadsViaHTTPAPI(ctx, threads)
		if lastErr == nil {
			m.mu.Lock()
			m.currentMode = mode
			m.mu.Unlock()
			m.logger.Info("xmrig mode applied via HTTP API", "mode", mode, "threads", threads, "attempt", attempt)
			return nil
		}

		if attempt < maxAPIAttempts {
			m.logger.Warn("xmrig HTTP API update failed, retrying", "mode", mode, "threads", threads, "attempt", attempt, "error", lastErr)
			select {
			case <-time.After(apiRetryDelay):
			case <-ctx.Done():
				return fmt.Errorf("xmrig HTTP API retry canceled: %w", ctx.Err())
			}
		}
	}

	m.logger.Warn("xmrig HTTP API update failed, falling back to restart", "mode", mode, "threads", threads, "error", lastErr)
	return m.restartWithMode(ctx, mode, threads)
}

func (m *XMRigManager) restartWithMode(ctx context.Context, mode string, threads int) error {
	if threads < 1 {
		return fmt.Errorf("invalid xmrig threads: %d", threads)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.restartWithModeLocked(ctx, mode, threads)
}

func (m *XMRigManager) restartWithModeLocked(ctx context.Context, mode string, threads int) error {
	if m.stopped {
		return fmt.Errorf("xmrig manager stopped")
	}

	if m.procManager.IsRunning(xmrigProcessName) {
		if err := m.procManager.Stop(ctx, xmrigProcessName, 3*time.Second); err != nil {
			return fmt.Errorf("stop xmrig for mode switch: %w", err)
		}
	}

	if err := m.cfg.ValidateExtraArgs(); err != nil {
		return fmt.Errorf("validate xmrig extra args: %w", err)
	}

	args := []string{
		"--http-port", strconv.Itoa(m.httpPort),
		"--threads", strconv.Itoa(threads),
		"-o", m.stratumURL,
		"-u", m.walletAddress + "." + m.workerName,
		"-a", "rx/0",
		"-p", "x",
		"--no-color",
		"--user-agent", fmt.Sprintf("XMRig/6.19.3 (xfo-miner) threads:%d", threads),
	}
	args = append(args, m.cfg.ExtraArgs...)
	m.logger.Info("xmrig starting", "path", m.cfg.XMRigPath, "args", args)

	proc, err := m.procManager.Start(ctx, xmrigProcessName, m.cfg.XMRigPath, args)
	if err != nil {
		m.logger.Error(
			"xmrig start FAILED",
			"mode", mode,
			"threads", threads,
			"error", err,
			"generation", m.generation,
		)
		return fmt.Errorf("start xmrig (%s): %w", mode, err)
	}

	m.generation++
	gen := m.generation
	go m.watchProcessExit(proc.Done, gen)

	m.currentMode = mode
	m.logger.Info("xmrig mode applied", "mode", mode, "threads", threads, "generation", gen)
	return nil
}

func (m *XMRigManager) watchProcessExit(done <-chan struct{}, startGen uint64) {
	<-done

	m.mu.Lock()
	stopped := m.stopped
	mode := m.currentMode
	currentGen := m.generation
	m.mu.Unlock()

	if stopped || mode == "" {
		m.logger.Info(
			"xmrig watchdog exiting — manager stopped or mode cleared",
			"stopped", stopped,
			"mode", mode,
			"start_generation", startGen,
			"current_generation", currentGen,
		)
		return
	}

	if startGen != currentGen {
		m.logger.Info(
			"xmrig watchdog ignoring stale process exit",
			"start_generation", startGen,
			"current_generation", currentGen,
		)
		return
	}

	threads := m.cfg.MaxThreads
	if mode == xmrigModeHeartbeat {
		threads = m.cfg.BackgroundThreads
	}

	backoff := m.watchdogInitialBackoff
	if backoff <= 0 {
		backoff = time.Second
	}
	maxBackoff := m.watchdogMaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}
	failures := 0
	for {
		select {
		case <-m.stopCh:
			return
		case <-time.After(backoff):
		}

		restartCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := m.restartWithMode(restartCtx, mode, threads)
		cancel()
		if err == nil {
			m.logger.Info("xmrig watchdog restarted process", "mode", mode, "generation", startGen)
			return
		}

		failures++
		if failures >= 5 {
			m.logger.Error("xmrig watchdog continuous restart failures", "failures", failures, "error", err)
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (m *XMRigManager) setThreadsViaHTTPAPI(ctx context.Context, threads int) error {
	if threads < 1 {
		return fmt.Errorf("invalid threads value: %d", threads)
	}

	baseURL := m.apiBaseURL
	if strings.TrimSpace(baseURL) == "" {
		baseURL = fmt.Sprintf("http://127.0.0.1:%d", m.httpPort)
	}

	body, err := json.Marshal(map[string]any{
		"cpu": map[string]any{
			"max-threads-hint": threads,
		},
	})
	if err != nil {
		return fmt.Errorf("marshal xmrig http payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimRight(baseURL, "/")+"/2/config", strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("create xmrig http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("xmrig http api call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("xmrig http api returned status: %d", resp.StatusCode)
	}

	return nil
}

func allocateXMRigHTTPPort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 18088
	}
	defer listener.Close()
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 18088
	}
	return addr.Port
}

type noopXMRigManager struct{}

func (m *noopXMRigManager) Start(_ context.Context) error            { return nil }
func (m *noopXMRigManager) SetFullMode(_ context.Context) error      { return nil }
func (m *noopXMRigManager) SetHeartbeatMode(_ context.Context) error { return nil }
func (m *noopXMRigManager) Stop(_ context.Context) error             { return nil }
