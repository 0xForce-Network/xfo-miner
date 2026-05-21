package scheduler

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/process"
)

const (
	xmrigModeFull               = "full"
	xmrigModeHeartbeat          = "heartbeat"
	xmrigProcessName            = "xmrig_l1"
	xmrigLogRetention           = 72 * time.Hour
	xmrigActiveLogMaxAge        = 24 * time.Hour
	xmrigRejectMonitorInterval  = 30 * time.Second
	xmrigRejectMonitorCooldown  = 2 * time.Minute
	xmrigRejectRateThreshold    = 0.10
	xmrigRejectMonitorMinShares = 20
)

type xmrigController interface {
	Start(context.Context) error
	SetFullMode(context.Context) error
	SetHeartbeatMode(context.Context) error
	Stop(context.Context) error
	Shutdown(context.Context) error
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
	rejectMonitorInterval  time.Duration
	rejectMonitorCooldown  time.Duration
	rejectMonitorThreshold float64
	rejectMonitorMinShares int

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
		rejectMonitorInterval:  xmrigRejectMonitorInterval,
		rejectMonitorCooldown:  xmrigRejectMonitorCooldown,
		rejectMonitorThreshold: xmrigRejectRateThreshold,
		rejectMonitorMinShares: xmrigRejectMonitorMinShares,
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
	if !m.cfg.Enabled {
		return nil
	}
	m.mu.Lock()
	m.currentMode = ""
	m.generation++
	m.mu.Unlock()
	return m.procManager.Stop(ctx, xmrigProcessName, 3*time.Second)
}

func (m *XMRigManager) Shutdown(ctx context.Context) error {
	if !m.cfg.Enabled {
		return nil
	}
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.stopCh)
	}
	m.currentMode = ""
	m.generation++
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
	if err := prepareXMRigLogPath(m.cfg.XMRigLogPath, time.Now()); err != nil {
		m.logger.Warn("xmrig log preparation warning", "path", m.cfg.XMRigLogPath, "error", err)
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
	m.logger.Info("xmrig starting", "path", m.cfg.XMRigPath, "args", args, "log_path", m.cfg.XMRigLogPath)

	proc, err := m.procManager.StartRaw(ctx, xmrigProcessName, m.cfg.XMRigPath, args)
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
	if err := m.pipeProcessOutputToLog(proc, m.cfg.XMRigLogPath); err != nil {
		_ = m.procManager.Stop(ctx, xmrigProcessName, 3*time.Second)
		return fmt.Errorf("open xmrig log: %w", err)
	}

	m.generation++
	gen := m.generation
	go m.watchProcessExit(proc.Done, gen)
	go m.watchRejectRate(gen)

	m.currentMode = mode
	m.logger.Info("xmrig mode applied", "mode", mode, "threads", threads, "generation", gen)
	return nil
}

func (m *XMRigManager) pipeProcessOutputToLog(proc *process.ManagedProcess, logPath string) error {
	logPath = strings.TrimSpace(logPath)
	if proc == nil || logPath == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("create parent directory %q: %w", filepath.Dir(logPath), err)
	}
	sink, err := newXMRigLogSink(logPath, time.Now)
	if err != nil {
		return err
	}

	m.logger.Info("xmrig output redirected", "log_path", logPath)
	go func() {
		defer sink.close()
		var wg sync.WaitGroup

		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := process.ScanLines(proc.Stdout, func(line string) {
				if err := sink.writeLine("[stdout]", line); err != nil {
					m.logger.Warn("failed writing xmrig stdout log", "error", err)
				}
			}); err != nil && err != io.ErrClosedPipe {
				m.logger.Warn("failed reading xmrig stdout", "error", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := process.ScanLines(proc.Stderr, func(line string) {
				if err := sink.writeLine("[stderr]", line); err != nil {
					m.logger.Warn("failed writing xmrig stderr log", "error", err)
				}
			}); err != nil && err != io.ErrClosedPipe {
				m.logger.Warn("failed reading xmrig stderr", "error", err)
			}
		}()
		wg.Wait()
	}()

	return nil
}

type xmrigLogSink struct {
	path     string
	now      func() time.Time
	mu       sync.Mutex
	file     *os.File
	openedAt time.Time
}

func newXMRigLogSink(logPath string, now func() time.Time) (*xmrigLogSink, error) {
	if now == nil {
		now = time.Now
	}
	if err := prepareXMRigLogPath(logPath, now()); err != nil {
		return nil, err
	}
	s := &xmrigLogSink{path: filepath.Clean(logPath), now: now}
	if err := s.openLocked(now()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *xmrigLogSink) openLocked(now time.Time) error {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open %q: %w", s.path, err)
	}
	s.file = f
	s.openedAt = now
	return nil
}

func (s *xmrigLogSink) writeLine(prefix string, line string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if !s.openedAt.IsZero() && now.Sub(s.openedAt) >= xmrigActiveLogMaxAge {
		if err := s.rotateLocked(now); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(s.file, "%s %s\n", prefix, line)
	return err
}

func (s *xmrigLogSink) rotateLocked(now time.Time) error {
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("close active xmrig log before rotation: %w", err)
		}
		s.file = nil
	}
	if err := rotateActiveXMRigLog(s.path, now); err != nil {
		return err
	}
	if err := cleanupXMRigLogFamily(s.path, now, xmrigLogRetention); err != nil {
		return err
	}
	return s.openLocked(now)
}

func (s *xmrigLogSink) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

func prepareXMRigLogPath(logPath string, now time.Time) error {
	logPath = strings.TrimSpace(logPath)
	if logPath == "" {
		return nil
	}
	cleanPath := filepath.Clean(logPath)
	if err := os.MkdirAll(filepath.Dir(cleanPath), 0o755); err != nil {
		return fmt.Errorf("create parent directory %q: %w", filepath.Dir(cleanPath), err)
	}
	if err := rotateActiveXMRigLogIfStale(cleanPath, now, xmrigActiveLogMaxAge); err != nil {
		return err
	}
	return cleanupXMRigLogFamily(cleanPath, now, xmrigLogRetention)
}

func rotateActiveXMRigLogIfStale(logPath string, now time.Time, maxAge time.Duration) error {
	if maxAge <= 0 {
		return nil
	}
	info, err := os.Stat(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat active xmrig log %q: %w", logPath, err)
	}
	if info.IsDir() || now.Sub(info.ModTime()) < maxAge {
		return nil
	}
	return rotateActiveXMRigLog(logPath, now)
}

func rotateActiveXMRigLog(logPath string, now time.Time) error {
	if _, err := os.Stat(logPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat active xmrig log %q: %w", logPath, err)
	}
	rotatedPath := xmrigRotatedLogPath(logPath, now)
	if err := os.Rename(logPath, rotatedPath); err != nil {
		return fmt.Errorf("rotate active xmrig log %q to %q: %w", logPath, rotatedPath, err)
	}
	return nil
}

func xmrigRotatedLogPath(logPath string, now time.Time) string {
	return filepath.Join(filepath.Dir(logPath), filepath.Base(logPath)+".xfo-"+strconv.FormatInt(now.UTC().UnixNano(), 10)+".log")
}

func cleanupXMRigLogFamily(logPath string, now time.Time, retention time.Duration) error {
	logPath = strings.TrimSpace(logPath)
	if logPath == "" || retention <= 0 {
		return nil
	}

	cleanPath := filepath.Clean(logPath)
	logDir := filepath.Dir(cleanPath)
	base := filepath.Base(cleanPath)
	entries, err := os.ReadDir(logDir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(logDir, 0o755)
		}
		return fmt.Errorf("read log directory %q: %w", logDir, err)
	}

	var cleanupErrs []error
	cutoff := now.Add(-retention)
	for _, entry := range entries {
		if entry.IsDir() || !isXMRigRotatedLogName(entry.Name(), base) {
			continue
		}
		candidate := filepath.Join(logDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("stat log %q: %w", candidate, err))
			continue
		}
		if !info.ModTime().Before(cutoff) {
			continue
		}
		if err := os.Remove(candidate); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("remove old log %q: %w", candidate, err))
		}
	}

	return errors.Join(cleanupErrs...)
}

func isXMRigRotatedLogName(name string, base string) bool {
	prefix := base + ".xfo-"
	suffix := ".log"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	stamp := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if stamp == "" {
		return false
	}
	_, err := strconv.ParseInt(stamp, 10, 64)
	return err == nil
}

type xmrigShareStats struct {
	Accepted int
	Rejected int
}

func (s xmrigShareStats) Total() int {
	return s.Accepted + s.Rejected
}

func (s xmrigShareStats) RejectRate() float64 {
	total := s.Total()
	if total <= 0 {
		return 0
	}
	return float64(s.Rejected) / float64(total)
}

func parseXMRigShareStatsFromLine(line string) (xmrigShareStats, bool) {
	lower := strings.ToLower(line)
	if !strings.Contains(lower, "accepted") && !strings.Contains(lower, "rejected") {
		return xmrigShareStats{}, false
	}

	remaining := line
	for {
		open := strings.Index(remaining, "(")
		if open < 0 {
			return xmrigShareStats{}, false
		}
		close := strings.Index(remaining[open+1:], ")")
		if close < 0 {
			return xmrigShareStats{}, false
		}

		candidate := remaining[open+1 : open+1+close]
		parts := strings.Split(candidate, "/")
		if len(parts) == 2 {
			accepted, acceptedErr := strconv.Atoi(strings.TrimSpace(parts[0]))
			rejected, rejectedErr := strconv.Atoi(strings.TrimSpace(parts[1]))
			if acceptedErr == nil && rejectedErr == nil && accepted >= 0 && rejected >= 0 {
				return xmrigShareStats{Accepted: accepted, Rejected: rejected}, true
			}
		}

		remaining = remaining[open+1+close+1:]
	}
}

func readXMRigShareStats(logPath string) (xmrigShareStats, bool, error) {
	logPath = strings.TrimSpace(logPath)
	if logPath == "" {
		return xmrigShareStats{}, false, nil
	}

	f, err := os.Open(filepath.Clean(logPath))
	if err != nil {
		if os.IsNotExist(err) {
			return xmrigShareStats{}, false, nil
		}
		return xmrigShareStats{}, false, fmt.Errorf("open xmrig log %q: %w", logPath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var latest xmrigShareStats
	found := false
	for scanner.Scan() {
		if stats, ok := parseXMRigShareStatsFromLine(scanner.Text()); ok {
			latest = stats
			found = true
		}
	}
	if err := scanner.Err(); err != nil {
		return xmrigShareStats{}, false, fmt.Errorf("read xmrig log %q: %w", logPath, err)
	}

	return latest, found, nil
}

func (m *XMRigManager) watchRejectRate(startGen uint64) {
	interval := m.rejectMonitorInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	cooldown := m.rejectMonitorCooldown
	if cooldown <= 0 {
		cooldown = 2 * time.Minute
	}
	threshold := m.rejectMonitorThreshold
	if threshold <= 0 {
		threshold = xmrigRejectRateThreshold
	}
	minShares := m.rejectMonitorMinShares
	if minShares <= 0 {
		minShares = xmrigRejectMonitorMinShares
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastRestart time.Time
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
		}

		m.mu.Lock()
		stopped := m.stopped
		mode := m.currentMode
		currentGen := m.generation
		m.mu.Unlock()

		if stopped || mode == "" || currentGen != startGen {
			return
		}
		if !m.procManager.IsRunning(xmrigProcessName) {
			continue
		}

		stats, found, err := readXMRigShareStats(m.cfg.XMRigLogPath)
		if err != nil {
			m.logger.Warn("xmrig reject-rate monitor failed reading log", "path", m.cfg.XMRigLogPath, "error", err, "generation", startGen)
			continue
		}
		if !found || stats.Total() < minShares || stats.RejectRate() <= threshold {
			continue
		}
		if !lastRestart.IsZero() && time.Since(lastRestart) < cooldown {
			continue
		}

		threads := m.cfg.MaxThreads
		if mode == xmrigModeHeartbeat {
			threads = m.cfg.BackgroundThreads
		}
		restartCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = m.restartWithMode(restartCtx, mode, threads)
		cancel()
		if err != nil {
			m.logger.Warn("xmrig reject-rate restart failed", "mode", mode, "accepted", stats.Accepted, "rejected", stats.Rejected, "reject_rate", stats.RejectRate(), "error", err, "generation", startGen)
			continue
		}

		lastRestart = time.Now()
		m.logger.Warn("xmrig restarted after high reject rate", "mode", mode, "accepted", stats.Accepted, "rejected", stats.Rejected, "reject_rate", stats.RejectRate(), "threshold", threshold, "generation", startGen)
		return
	}
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
func (m *noopXMRigManager) Shutdown(_ context.Context) error         { return nil }
