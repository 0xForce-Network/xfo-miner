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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
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
	xmrigPortSwitchInterval     = 30 * time.Second
	xmrigPortSwitchWindow       = 3 * time.Minute
	xmrigPortSwitchCooldown     = 5 * time.Minute
	xmrigPortSwitchProbeTimeout = 3 * time.Second
)

var xmrigShareDiffRegex = regexp.MustCompile(`(?i)\bdiff\s+([0-9]+)\b`)
var xmrigLogTimestampRegex = regexp.MustCompile(`\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\]`)

type xmrigPortAutoSwitchPolicy struct {
	Ports             []xmrigStratumPortProfile
	CheckInterval     time.Duration
	DecisionWindow    time.Duration
	Cooldown          time.Duration
	ProbeTimeout      time.Duration
	MinAcceptedDelta  int
	HighDiffSamples   int
	LowDiffSamples    int
	HighDiffRatio     float64
	LowDiffRatio      float64
	FailureMinSamples int
	FailureRejectRate float64
	ProbeBeforeSwitch bool
}

type xmrigStratumPortProfile struct {
	URL     string
	MinDiff int
	MaxDiff int
}

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
	portSwitchProbe        func(ctx context.Context, stratumURL string, timeout time.Duration) error
	now                    func() time.Time
	watchdogSleep          func(time.Duration)

	mu                  sync.Mutex
	currentMode         string
	currentURL          string
	generation          uint64
	generationStartedAt map[uint64]time.Time
	stopped             bool
	stopCh              chan struct{}
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
		portSwitchProbe:        probeStratumPort,
		now:                    time.Now,
		watchdogSleep:          time.Sleep,
		generationStartedAt:    make(map[uint64]time.Time),
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
	stratumURL := m.currentStratumURLLocked()

	if err := m.cfg.ValidateExtraArgs(); err != nil {
		return fmt.Errorf("validate xmrig extra args: %w", err)
	}
	if err := prepareXMRigLogPath(m.cfg.XMRigLogPath, time.Now()); err != nil {
		m.logger.Warn("xmrig log preparation warning", "path", m.cfg.XMRigLogPath, "error", err)
	}

	args := []string{
		"--http-port", strconv.Itoa(m.httpPort),
		"--threads", strconv.Itoa(threads),
		"-o", stratumURL,
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
	m.generationStartedAt[gen] = m.now()
	go m.watchProcessExit(proc, gen)
	go m.watchRejectRate(gen)
	go m.watchPortAutoSwitch(gen)

	m.currentMode = mode
	m.currentURL = stratumURL
	m.logger.Info("xmrig mode applied", "mode", mode, "threads", threads, "stratum_url", stratumURL, "generation", gen, "http_port", m.httpPort, "extra_args", m.cfg.ExtraArgs)
	return nil
}

func (m *XMRigManager) currentStratumURLLocked() string {
	if strings.TrimSpace(m.currentURL) != "" {
		return strings.TrimSpace(m.currentURL)
	}
	if port, ok := m.currentPortProfileLocked(); ok {
		return port.URL
	}
	return strings.TrimSpace(m.stratumURL)
}

func (m *XMRigManager) currentPortProfileLocked() (xmrigStratumPortProfile, bool) {
	policy := fixedPortAutoSwitchPolicy(m.stratumURL)
	ports := normalizedPortProfiles(policy, m.stratumURL)
	if len(ports) == 0 {
		return xmrigStratumPortProfile{}, false
	}
	current := strings.TrimSpace(m.currentURL)
	if current == "" {
		current = strings.TrimSpace(m.stratumURL)
	}
	idx := findPortProfileIndex(ports, current)
	if idx < 0 {
		return ports[0], true
	}
	return ports[idx], true
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

type xmrigShareObservation struct {
	Timestamp time.Time
	Accepted  int
	Rejected  int
	Diff      int
	Kind      string
}

type xmrigLogRunBoundary struct {
	Timestamp time.Time
}

type xmrigLogEvent struct {
	Observation *xmrigShareObservation
	RunBoundary *xmrigLogRunBoundary
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
	obs, ok := parseXMRigShareObservationFromLine(line, time.Now())
	if !ok {
		return xmrigShareStats{}, false
	}
	return xmrigShareStats{Accepted: obs.Accepted, Rejected: obs.Rejected}, true
}

func parseXMRigShareObservationFromLine(line string, fallbackTime time.Time) (xmrigShareObservation, bool) {
	lower := strings.ToLower(line)
	if !strings.Contains(lower, "accepted") && !strings.Contains(lower, "rejected") {
		return xmrigShareObservation{}, false
	}
	timestamp := parseXMRigLogTimestamp(line, fallbackTime)
	kind := "accepted"
	if strings.Contains(lower, "rejected") {
		kind = "rejected"
	}
	diff := 0
	if matches := xmrigShareDiffRegex.FindStringSubmatch(line); len(matches) == 2 {
		if parsed, err := strconv.Atoi(matches[1]); err == nil && parsed >= 0 {
			diff = parsed
		}
	}

	remaining := line
	for {
		open := strings.Index(remaining, "(")
		if open < 0 {
			return xmrigShareObservation{}, false
		}
		close := strings.Index(remaining[open+1:], ")")
		if close < 0 {
			return xmrigShareObservation{}, false
		}

		candidate := remaining[open+1 : open+1+close]
		parts := strings.Split(candidate, "/")
		if len(parts) == 2 {
			accepted, acceptedErr := strconv.Atoi(strings.TrimSpace(parts[0]))
			rejected, rejectedErr := strconv.Atoi(strings.TrimSpace(parts[1]))
			if acceptedErr == nil && rejectedErr == nil && accepted >= 0 && rejected >= 0 {
				return xmrigShareObservation{Timestamp: timestamp, Accepted: accepted, Rejected: rejected, Diff: diff, Kind: kind}, true
			}
		}

		remaining = remaining[open+1+close+1:]
	}
}

func parseXMRigLogTimestamp(line string, fallbackTime time.Time) time.Time {
	matches := xmrigLogTimestampRegex.FindStringSubmatch(line)
	if len(matches) != 2 {
		return fallbackTime
	}
	for _, layout := range []string{"2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, matches[1], time.UTC); err == nil {
			return parsed
		}
	}
	return fallbackTime
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

func readXMRigShareObservations(logPath string, since time.Time) ([]xmrigShareObservation, error) {
	events, err := readXMRigLogEvents(logPath, since)
	if err != nil {
		return nil, err
	}
	observations := make([]xmrigShareObservation, 0, len(events))
	for _, event := range events {
		if event.Observation != nil {
			observations = append(observations, *event.Observation)
		}
	}
	return observations, nil
}

func readXMRigLogEvents(logPath string, since time.Time) ([]xmrigLogEvent, error) {
	logPath = strings.TrimSpace(logPath)
	if logPath == "" {
		return nil, nil
	}

	f, err := os.Open(filepath.Clean(logPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open xmrig log %q: %w", logPath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	events := []xmrigLogEvent{}
	now := time.Now()
	for scanner.Scan() {
		line := scanner.Text()
		if obs, ok := parseXMRigShareObservationFromLine(line, now); ok {
			if !since.IsZero() && obs.Timestamp.Before(since) {
				continue
			}
			events = append(events, xmrigLogEvent{Observation: &obs})
			continue
		}
		lower := strings.ToLower(line)
		if !strings.Contains(lower, "use pool") && !strings.Contains(lower, "pool #1") {
			continue
		}
		timestamp := parseXMRigLogTimestamp(line, now)
		if !since.IsZero() && timestamp.Before(since) {
			continue
		}
		events = append(events, xmrigLogEvent{RunBoundary: &xmrigLogRunBoundary{Timestamp: timestamp}})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read xmrig log %q: %w", logPath, err)
	}
	return events, nil
}

func (m *XMRigManager) watchPortAutoSwitch(startGen uint64) {
	policy := fixedPortAutoSwitchPolicy(m.stratumURL)
	if len(policy.Ports) < 2 {
		return
	}
	interval := policy.CheckInterval
	if interval <= 0 {
		interval = xmrigPortSwitchInterval
	}
	cooldown := policy.Cooldown
	decisionWindow := policy.DecisionWindow
	if decisionWindow <= 0 {
		decisionWindow = xmrigPortSwitchWindow
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastSwitch time.Time
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
		currentURL := m.currentStratumURLLocked()
		m.mu.Unlock()
		if stopped || mode == "" || currentGen != startGen {
			return
		}
		if !lastSwitch.IsZero() && cooldown > 0 && time.Since(lastSwitch) < cooldown {
			continue
		}

		events, err := readXMRigLogEvents(m.cfg.XMRigLogPath, time.Now().Add(-decisionWindow))
		if err != nil {
			m.logger.Warn("xmrig port auto-switch failed reading log", "path", m.cfg.XMRigLogPath, "error", err, "generation", startGen)
			continue
		}
		observations := latestXMRigRunShareObservations(events)
		nextURL, reason, ok := selectAutoSwitchPort(policy, m.stratumURL, currentURL, observations)
		if !ok {
			continue
		}
		m.logger.Info(
			"xmrig port auto-switch decision",
			"from", currentURL,
			"to", nextURL,
			"reason", reason,
			"generation", startGen,
			"observations", len(observations),
			"summary", summarizeXMRigShareObservations(observations),
		)
		if policy.ProbeBeforeSwitch {
			probeTimeout := policy.ProbeTimeout
			if probeTimeout <= 0 {
				probeTimeout = xmrigPortSwitchProbeTimeout
			}
			probeCtx, cancel := context.WithTimeout(context.Background(), probeTimeout)
			err := m.portSwitchProbe(probeCtx, nextURL, probeTimeout)
			cancel()
			if err != nil {
				m.logger.Warn("xmrig port auto-switch probe failed", "from", currentURL, "to", nextURL, "reason", reason, "error", err)
				continue
			}
			m.logger.Info("xmrig port auto-switch probe succeeded", "from", currentURL, "to", nextURL, "reason", reason, "timeout", probeTimeout)
		}

		threads := m.cfg.MaxThreads
		if mode == xmrigModeHeartbeat {
			threads = m.cfg.BackgroundThreads
		}
		restartCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = m.switchStratumURL(restartCtx, mode, threads, nextURL, reason)
		cancel()
		if err != nil {
			m.logger.Warn("xmrig port auto-switch restart failed", "from", currentURL, "to", nextURL, "reason", reason, "error", err)
			continue
		}
		lastSwitch = time.Now()
		return
	}
}

func (m *XMRigManager) switchStratumURL(ctx context.Context, mode string, threads int, nextURL string, reason string) error {
	nextURL = strings.TrimSpace(nextURL)
	if nextURL == "" {
		return errors.New("empty stratum URL")
	}
	m.mu.Lock()
	currentURL := m.currentStratumURLLocked()
	m.mu.Unlock()
	if currentURL == nextURL {
		return nil
	}
	if err := m.restartWithModeForURL(ctx, mode, threads, nextURL); err != nil {
		return err
	}
	m.logger.Info("xmrig port auto-switch applied", "stratum_url", nextURL, "reason", reason)
	return nil
}

func (m *XMRigManager) restartWithModeForURL(ctx context.Context, mode string, threads int, nextURL string) error {
	if threads < 1 {
		return fmt.Errorf("invalid xmrig threads: %d", threads)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentURL = nextURL
	return m.restartWithModeLocked(ctx, mode, threads)
}

func fixedPortAutoSwitchPolicy(fallbackURL string) xmrigPortAutoSwitchPolicy {
	return xmrigPortAutoSwitchPolicy{
		Ports:             fixedStratumPortProfiles(fallbackURL),
		CheckInterval:     xmrigPortSwitchInterval,
		DecisionWindow:    xmrigPortSwitchWindow,
		Cooldown:          xmrigPortSwitchCooldown,
		ProbeTimeout:      xmrigPortSwitchProbeTimeout,
		MinAcceptedDelta:  20,
		HighDiffSamples:   3,
		LowDiffSamples:    5,
		HighDiffRatio:     0.98,
		LowDiffRatio:      1.20,
		FailureMinSamples: 10,
		FailureRejectRate: 0.50,
		ProbeBeforeSwitch: true,
	}
}

func fixedStratumPortProfiles(fallbackURL string) []xmrigStratumPortProfile {
	baseURL := strings.TrimSpace(fallbackURL)
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" {
		return []xmrigStratumPortProfile{{URL: baseURL, MinDiff: 100, MaxDiff: 50000}}
	}
	host := parsed.Hostname()
	if host == "" {
		return []xmrigStratumPortProfile{{URL: baseURL, MinDiff: 100, MaxDiff: 50000}}
	}
	scheme := parsed.Scheme
	if scheme == "" {
		scheme = "stratum+tcp"
	}
	portURL := func(port string) string {
		candidate := *parsed
		candidate.Scheme = scheme
		candidate.User = nil
		candidate.Host = net.JoinHostPort(host, port)
		candidate.Path = ""
		candidate.RawQuery = ""
		candidate.Fragment = ""
		return candidate.String()
	}
	return []xmrigStratumPortProfile{
		{URL: portURL("3333"), MinDiff: 100, MaxDiff: 50000},
		{URL: portURL("5555"), MinDiff: 100, MaxDiff: 4000000000},
		{URL: portURL("7777"), MinDiff: 100, MaxDiff: 4000000000},
	}
}

func normalizedPortProfiles(policy xmrigPortAutoSwitchPolicy, fallbackURL string) []xmrigStratumPortProfile {
	ports := make([]xmrigStratumPortProfile, 0, len(policy.Ports)+1)
	seen := map[string]struct{}{}
	add := func(port xmrigStratumPortProfile) {
		port.URL = strings.TrimSpace(port.URL)
		if port.URL == "" {
			return
		}
		if _, ok := seen[port.URL]; ok {
			return
		}
		seen[port.URL] = struct{}{}
		ports = append(ports, port)
	}
	for _, port := range policy.Ports {
		add(port)
	}
	add(xmrigStratumPortProfile{URL: fallbackURL})
	return ports
}

func findPortProfileIndex(ports []xmrigStratumPortProfile, stratumURL string) int {
	stratumURL = strings.TrimSpace(stratumURL)
	for idx, port := range ports {
		if strings.TrimSpace(port.URL) == stratumURL {
			return idx
		}
	}
	return -1
}

func selectAutoSwitchPort(policy xmrigPortAutoSwitchPolicy, fallbackURL string, currentURL string, observations []xmrigShareObservation) (string, string, bool) {
	ports := normalizedPortProfiles(policy, fallbackURL)
	if len(ports) < 2 {
		return "", "", false
	}
	currentIdx := findPortProfileIndex(ports, currentURL)
	if currentIdx < 0 {
		currentIdx = findPortProfileIndex(ports, fallbackURL)
	}
	if currentIdx < 0 {
		currentIdx = 0
	}
	current := ports[currentIdx]
	if len(observations) == 0 {
		return "", "", false
	}
	observations = latestMonotonicShareObservations(observations)
	if len(observations) == 0 {
		return "", "", false
	}

	first := observations[0]
	last := observations[len(observations)-1]
	acceptedDelta := last.Accepted - first.Accepted
	rejectedDelta := last.Rejected - first.Rejected
	totalDelta := acceptedDelta + rejectedDelta
	if acceptedDelta < policy.MinAcceptedDelta && totalDelta < policy.FailureMinSamples {
		return "", "", false
	}

	if policy.FailureMinSamples > 0 && policy.FailureRejectRate > 0 && totalDelta >= policy.FailureMinSamples && totalDelta > 0 && float64(rejectedDelta)/float64(totalDelta) >= policy.FailureRejectRate {
		for idx := currentIdx + 1; idx < len(ports); idx++ {
			return ports[idx].URL, "high_reject_rate_try_higher_port", true
		}
		for idx := currentIdx - 1; idx >= 0; idx-- {
			return ports[idx].URL, "high_reject_rate_try_lower_port", true
		}
		return "", "", false
	}

	acceptedAtHigh := 0
	acceptedAtLow := 0
	for _, obs := range observations {
		if obs.Kind != "accepted" || obs.Diff <= 0 {
			continue
		}
		if current.MaxDiff > 0 && float64(obs.Diff) >= float64(current.MaxDiff)*policy.HighDiffRatio {
			acceptedAtHigh++
		}
		if current.MinDiff > 0 && float64(obs.Diff) <= float64(current.MinDiff)*policy.LowDiffRatio {
			acceptedAtLow++
		}
	}

	if acceptedAtHigh >= policy.HighDiffSamples && currentIdx+1 < len(ports) {
		return ports[currentIdx+1].URL, "diff_at_high_cap", true
	}
	if acceptedAtLow >= policy.LowDiffSamples && currentIdx > 0 {
		return ports[currentIdx-1].URL, "diff_at_low_floor", true
	}
	return "", "", false
}

func latestMonotonicShareObservations(observations []xmrigShareObservation) []xmrigShareObservation {
	if len(observations) <= 1 {
		return observations
	}
	start := 0
	for idx := 1; idx < len(observations); idx++ {
		prev := observations[idx-1]
		current := observations[idx]
		if current.Accepted < prev.Accepted || current.Rejected < prev.Rejected {
			start = idx
		}
	}
	return observations[start:]
}

func latestXMRigRunShareObservations(events []xmrigLogEvent) []xmrigShareObservation {
	if len(events) == 0 {
		return nil
	}
	start := 0
	for i, event := range events {
		if event.RunBoundary != nil {
			start = i + 1
		}
	}
	observations := make([]xmrigShareObservation, 0, len(events)-start)
	for _, event := range events[start:] {
		if event.Observation != nil {
			observations = append(observations, *event.Observation)
		}
	}
	return observations
}

func summarizeXMRigShareObservations(observations []xmrigShareObservation) map[string]any {
	observations = latestMonotonicShareObservations(observations)
	if len(observations) == 0 {
		return map[string]any{"count": 0}
	}
	first := observations[0]
	last := observations[len(observations)-1]
	acceptedAtHigh := 0
	rejected := 0
	for _, obs := range observations {
		if obs.Kind == "accepted" && obs.Diff >= 50000 {
			acceptedAtHigh++
		}
		if obs.Kind == "rejected" {
			rejected++
		}
	}
	return map[string]any{
		"count":            len(observations),
		"first_ts":         first.Timestamp.Format(time.RFC3339Nano),
		"last_ts":          last.Timestamp.Format(time.RFC3339Nano),
		"first_accepted":   first.Accepted,
		"last_accepted":    last.Accepted,
		"accepted_delta":   last.Accepted - first.Accepted,
		"first_rejected":   first.Rejected,
		"last_rejected":    last.Rejected,
		"rejected_delta":   last.Rejected - first.Rejected,
		"accepted_at_high": acceptedAtHigh,
		"rejected_samples": rejected,
		"last_diff":        last.Diff,
		"last_kind":        last.Kind,
	}
}

func probeStratumPort(ctx context.Context, stratumURL string, timeout time.Duration) error {
	parsed, err := url.Parse(strings.TrimSpace(stratumURL))
	if err != nil {
		return err
	}
	if parsed.Host == "" {
		return errors.New("stratum URL missing host")
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", parsed.Host)
	if err != nil {
		return err
	}
	return conn.Close()
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

func (m *XMRigManager) watchProcessExit(proc *process.ManagedProcess, startGen uint64) {
	if proc == nil {
		return
	}
	<-proc.Done

	m.mu.Lock()
	stopped := m.stopped
	mode := m.currentMode
	currentGen := m.generation
	startedAt := m.generationStartedAt[startGen]
	if len(m.generationStartedAt) > 32 {
		for gen := range m.generationStartedAt {
			if gen+32 < currentGen {
				delete(m.generationStartedAt, gen)
			}
		}
	}
	now := m.now
	sleep := m.watchdogSleep
	m.mu.Unlock()

	exitErr := proc.Wait()
	m.logger.Warn(
		"xmrig process exited",
		"name", proc.Name,
		"command", proc.Command,
		"args", proc.Args,
		"exit_error", exitErr,
		"start_generation", startGen,
		"current_generation", currentGen,
		"mode", mode,
		"stopped", stopped,
	)

	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = time.Sleep
	}
	if !startedAt.IsZero() {
		uptime := now().Sub(startedAt)
		if uptime < 5*time.Second {
			delay := 5*time.Second - uptime
			m.logger.Warn(
				"xmrig exited shortly after start; delaying watchdog restart",
				"uptime", uptime,
				"delay", delay,
				"start_generation", startGen,
				"current_generation", currentGen,
			)
			sleep(delay)
		}
	}

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
