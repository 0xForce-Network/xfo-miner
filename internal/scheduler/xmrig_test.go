package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
)

const testWalletAddress = "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3"

func waitForPoolHandler(t *testing.T, pcl *mockPoolClient) {
	t.Helper()
	waitFor(t, func() bool {
		pcl.mu.Lock()
		defer pcl.mu.Unlock()
		return pcl.handler != nil
	})
}

type mockXMRigManager struct {
	mu             sync.Mutex
	startCalls     int
	fullCalls      int
	heartbeatCalls int
	stopCalls      int
	shutdownCalls  int
}

func (m *mockXMRigManager) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalls++
	return nil
}

func (m *mockXMRigManager) SetFullMode(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fullCalls++
	return nil
}

func (m *mockXMRigManager) SetHeartbeatMode(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCalls++
	return nil
}

func (m *mockXMRigManager) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalls++
	return nil
}

func (m *mockXMRigManager) Shutdown(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shutdownCalls++
	return nil
}

func (m *mockXMRigManager) snapshot() (start, full, heartbeat, stop int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startCalls, m.fullCalls, m.heartbeatCalls, m.stopCalls
}

func TestXMRigStartsOnSchedulerRun(t *testing.T) {
	t.Parallel()

	s, _, _, _ := newTestScheduler()
	mgr := &mockXMRigManager{}
	s.xmrigManager = mgr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	waitFor(t, func() bool {
		start, _, _, _ := mgr.snapshot()
		return start >= 1
	})
}

func TestXMRigSwitchesToHeartbeatOnGPUJob(t *testing.T) {
	t.Parallel()

	s, _, pcl, _ := newTestScheduler()
	mgr := &mockXMRigManager{}
	s.xmrigManager = mgr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()
	waitForPoolHandler(t, pcl)

	pcl.emit(pool.JobGPUMessage{Type: "job_gpu", JobID: "job-xmrig-1", HashMode: 22000, Target: "hash", Skip: 0, Limit: 1})

	waitFor(t, func() bool {
		_, _, heartbeat, _ := mgr.snapshot()
		return heartbeat >= 1
	})
}

func TestXMRigRestoresFullModeAfterJob(t *testing.T) {
	t.Parallel()

	s, _, pcl, _ := newTestScheduler()
	mgr := &mockXMRigManager{}
	s.xmrigManager = mgr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()
	waitForPoolHandler(t, pcl)

	pcl.emit(pool.JobGPUMessage{Type: "job_gpu", JobID: "job-xmrig-2", HashMode: 22000, Target: "hash", Skip: 0, Limit: 1})

	waitFor(t, func() bool {
		_, full, heartbeat, _ := mgr.snapshot()
		return heartbeat >= 1 && full >= 2
	})
}

func TestXMRigNeverFullyStoppedDuringTransitions(t *testing.T) {
	t.Parallel()

	s, _, pcl, _ := newTestScheduler()
	mgr := &mockXMRigManager{}
	s.xmrigManager = mgr

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()
	waitForPoolHandler(t, pcl)

	pcl.emit(pool.JobGPUMessage{Type: "job_gpu", JobID: "job-xmrig-3", HashMode: 22000, Target: "hash", Skip: 0, Limit: 1})
	pcl.emit(pool.JobContainerMessage{Type: "job_container", JobID: "job-xmrig-4", Image: "img", TargetPort: 8080})

	waitFor(t, func() bool {
		_, full, heartbeat, stop := mgr.snapshot()
		return full >= 3 && heartbeat >= 2 && stop == 0
	})

	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestXMRigManagerUsesHTTPAPIForThreadSwitch(t *testing.T) {
	t.Parallel()

	proc := newMockProcessManager()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var (
		putCalls int
		callsMu  sync.Mutex
	)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/2/config" {
			callsMu.Lock()
			putCalls++
			callsMu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer api.Close()

	mgr := NewXMRigManager(proc, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)
	mgr.apiBaseURL = api.URL
	mgr.httpClient = api.Client()

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := mgr.SetHeartbeatMode(context.Background()); err != nil {
		t.Fatalf("SetHeartbeatMode() error = %v", err)
	}

	callsMu.Lock()
	defer callsMu.Unlock()
	if putCalls < 1 {
		t.Fatalf("expected HTTP API PUT /2/config to be called")
	}
}

func TestXMRigManagerRetriesHTTPAPIBeforeSuccess(t *testing.T) {
	t.Parallel()

	proc := newMockProcessManager()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var (
		putCalls int
		callsMu  sync.Mutex
	)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/2/config" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		callsMu.Lock()
		defer callsMu.Unlock()
		putCalls++
		if putCalls < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()

	mgr := NewXMRigManager(proc, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)
	mgr.apiBaseURL = api.URL
	mgr.httpClient = api.Client()

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := mgr.SetHeartbeatMode(context.Background()); err != nil {
		t.Fatalf("SetHeartbeatMode() error = %v", err)
	}

	callsMu.Lock()
	defer callsMu.Unlock()
	if putCalls != 3 {
		t.Fatalf("expected 3 HTTP API attempts, got %d", putCalls)
	}
	if got := proc.startCount(xmrigProcessName); got != 1 {
		t.Fatalf("expected no restart when retry succeeds, starts=%d", got)
	}
	if got := proc.stopCount(xmrigProcessName); got != 0 {
		t.Fatalf("expected no stop when retry succeeds, stops=%d", got)
	}
}

func TestXMRigManagerFallsBackToRestartAfterHTTPAPIRetriesExhausted(t *testing.T) {
	t.Parallel()

	proc := newMockProcessManager()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var (
		putCalls int
		callsMu  sync.Mutex
	)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/2/config" {
			callsMu.Lock()
			putCalls++
			callsMu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer api.Close()

	mgr := NewXMRigManager(proc, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)
	mgr.apiBaseURL = api.URL
	mgr.httpClient = api.Client()

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := mgr.SetHeartbeatMode(context.Background()); err != nil {
		t.Fatalf("SetHeartbeatMode() error = %v", err)
	}

	callsMu.Lock()
	defer callsMu.Unlock()
	if putCalls != 3 {
		t.Fatalf("expected 3 HTTP API attempts before fallback, got %d", putCalls)
	}
	if got := proc.startCount(xmrigProcessName); got != 2 {
		t.Fatalf("expected fallback restart to start process again, starts=%d", got)
	}
	if got := proc.stopCount(xmrigProcessName); got != 1 {
		t.Fatalf("expected fallback restart to stop process once, stops=%d", got)
	}
}

type flakyProcessManager struct {
	mu       sync.Mutex
	running  bool
	failures int
	starts   int
}

func (m *flakyProcessManager) Start(_ context.Context, _ string, _ string, _ []string) (*process.ManagedProcess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.starts++
	if m.failures > 0 {
		m.failures--
		return nil, errors.New("transient start failure")
	}
	m.running = true
	return &process.ManagedProcess{Name: "xmrig_l1", Done: make(chan struct{})}, nil
}

func (m *flakyProcessManager) StartRaw(ctx context.Context, name string, command string, args []string) (*process.ManagedProcess, error) {
	return m.Start(ctx, name, command, args)
}

func (m *flakyProcessManager) Stop(_ context.Context, _ string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	return nil
}

func (m *flakyProcessManager) StopAll(_ context.Context, _ time.Duration) error { return nil }
func (m *flakyProcessManager) Get(_ string) (*process.ManagedProcess, bool)     { return nil, false }
func (m *flakyProcessManager) IsRunning(_ string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

func TestXMRigManagerWatchdogRetriesRestart(t *testing.T) {
	t.Parallel()

	pm := &flakyProcessManager{failures: 1}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)
	mgr.currentMode = xmrigModeFull
	mgr.generation = 1
	mgr.watchdogInitialBackoff = 10 * time.Millisecond
	mgr.watchdogMaxBackoff = 20 * time.Millisecond

	proc := &process.ManagedProcess{Name: "xmrig_l1", Done: make(chan struct{})}
	go mgr.watchProcessExit(proc, 1)
	close(proc.Done)

	waitFor(t, func() bool {
		pm.mu.Lock()
		defer pm.mu.Unlock()
		return pm.starts >= 2
	})
}

func TestXMRigWatchdogIgnoresStaleExit(t *testing.T) {
	t.Parallel()

	pm := &flakyProcessManager{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)
	mgr.currentMode = xmrigModeHeartbeat
	mgr.generation = 2
	mgr.watchdogInitialBackoff = 10 * time.Millisecond
	mgr.watchdogMaxBackoff = 20 * time.Millisecond

	proc := &process.ManagedProcess{Name: "xmrig_l1", Done: make(chan struct{})}
	go mgr.watchProcessExit(proc, 1)
	close(proc.Done)

	time.Sleep(40 * time.Millisecond)

	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.starts != 0 {
		t.Fatalf("expected stale watchdog exit to be ignored, starts=%d", pm.starts)
	}
}

func TestXMRigWatchdogRestartsCurrentGenOnly(t *testing.T) {
	t.Parallel()

	pm := &flakyProcessManager{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)
	mgr.currentMode = xmrigModeHeartbeat
	mgr.generation = 3
	mgr.watchdogInitialBackoff = 10 * time.Millisecond
	mgr.watchdogMaxBackoff = 20 * time.Millisecond

	proc := &process.ManagedProcess{Name: "xmrig_l1", Done: make(chan struct{})}
	go mgr.watchProcessExit(proc, 3)
	close(proc.Done)

	waitFor(t, func() bool {
		pm.mu.Lock()
		defer pm.mu.Unlock()
		return pm.starts >= 1
	})
}

func TestXMRigWatchdogDelaysRapidExitRestart(t *testing.T) {
	t.Parallel()

	pm := &flakyProcessManager{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)
	now := time.Unix(100, 0)
	sleepCalls := make(chan time.Duration, 1)
	mgr.currentMode = xmrigModeFull
	mgr.generation = 1
	mgr.generationStartedAt[1] = now
	mgr.now = func() time.Time { return now.Add(time.Second) }
	mgr.watchdogSleep = func(d time.Duration) { sleepCalls <- d }
	mgr.watchdogInitialBackoff = time.Millisecond
	mgr.watchdogMaxBackoff = time.Millisecond

	proc := &process.ManagedProcess{Name: "xmrig_l1", Done: make(chan struct{})}
	go mgr.watchProcessExit(proc, 1)
	close(proc.Done)

	select {
	case got := <-sleepCalls:
		if got != 4*time.Second {
			t.Fatalf("sleep delay = %s, want 4s", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected watchdog rapid-exit delay")
	}
}

type captureProcessManager struct {
	mu       sync.Mutex
	running  bool
	lastArgs []string
}

func (m *captureProcessManager) Start(_ context.Context, _ string, _ string, args []string) (*process.ManagedProcess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = true
	m.lastArgs = append([]string(nil), args...)
	return &process.ManagedProcess{Name: "xmrig_l1", Done: make(chan struct{})}, nil
}

func (m *captureProcessManager) StartRaw(ctx context.Context, name string, command string, args []string) (*process.ManagedProcess, error) {
	return m.Start(ctx, name, command, args)
}

func (m *captureProcessManager) Stop(_ context.Context, _ string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	return nil
}

func (m *captureProcessManager) StopAll(_ context.Context, _ time.Duration) error { return nil }
func (m *captureProcessManager) Get(_ string) (*process.ManagedProcess, bool)     { return nil, false }
func (m *captureProcessManager) IsRunning(_ string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

func TestXMRigManagerInjectsPoolAndWorkerArgs(t *testing.T) {
	t.Parallel()

	pm := &captureProcessManager{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example.com:3333", testWalletAddress, "XFo2A88ABC", "Rig-4090-Alpha", logger)

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	pm.mu.Lock()
	args := append([]string(nil), pm.lastArgs...)
	pm.mu.Unlock()

	requirePair := func(key, wantVal string) {
		t.Helper()
		for i := 0; i+1 < len(args); i++ {
			if args[i] == key && args[i+1] == wantVal {
				return
			}
		}
		t.Fatalf("missing arg pair %s %s in args=%v", key, wantVal, args)
	}

	requirePair("-o", "stratum+tcp://pool.example.com:3333")
	requirePair("-u", testWalletAddress+".Rig-4090-Alpha")
}

func TestXMRigManagerAppendsExtraArgs(t *testing.T) {
	t.Parallel()

	pm := &captureProcessManager{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
		ExtraArgs:         []string{"--proxy=127.0.0.1:1080", "--keepalive"},
	}, "stratum+tcp://pool.example.com:3333", testWalletAddress, "XFo2A88ABC", "Rig-4090-Alpha", logger)

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	pm.mu.Lock()
	args := append([]string(nil), pm.lastArgs...)
	pm.mu.Unlock()

	requireToken := func(want string) {
		t.Helper()
		for _, arg := range args {
			if arg == want {
				return
			}
		}
		t.Fatalf("missing arg token %s in args=%v", want, args)
	}

	requireToken("--proxy=127.0.0.1:1080")
	requireToken("--keepalive")
}

func TestXMRigManagerRejectsReservedExtraArgs(t *testing.T) {
	t.Parallel()

	pm := &captureProcessManager{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
		ExtraArgs:         []string{"--http-port=17777"},
	}, "stratum+tcp://pool.example.com:3333", testWalletAddress, "XFo2A88ABC", "Rig-4090-Alpha", logger)

	if err := mgr.Start(context.Background()); err == nil {
		t.Fatalf("expected Start() to reject reserved extra args")
	}
}

func TestParseXMRigShareStatsFromRejectedLine(t *testing.T) {
	t.Parallel()

	stats, ok := parseXMRigShareStatsFromLine(`[stdout] [2026-05-20 10:11:39.886]  cpu      rejected (566/3780) diff 5000 "Unknown miner" (1 ms)`)
	if !ok {
		t.Fatalf("expected xmrig rejected line to parse")
	}
	if stats.Accepted != 566 || stats.Rejected != 3780 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if rate := stats.RejectRate(); rate <= 0.10 {
		t.Fatalf("expected reject rate above 10%%, got %f", rate)
	}
}

func TestParseXMRigShareStatsFromAcceptedLineWithZeroRejected(t *testing.T) {
	t.Parallel()

	stats, ok := parseXMRigShareStatsFromLine(`[stdout] [2026-05-21 01:30:00.000]  cpu      accepted (3/0) diff 5000 (1 ms)`)
	if !ok {
		t.Fatalf("expected xmrig accepted line to parse")
	}
	if stats.Accepted != 3 || stats.Rejected != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if rate := stats.RejectRate(); rate != 0 {
		t.Fatalf("expected zero reject rate for accepted (3/0), got %f", rate)
	}
}

func TestParseXMRigShareObservationIncludesDifficulty(t *testing.T) {
	t.Parallel()

	obs, ok := parseXMRigShareObservationFromLine(`[stdout] [2026-05-21 01:30:00.000]  cpu      accepted (3/0) diff 50000 (1 ms)`, time.Now())
	if !ok {
		t.Fatalf("expected xmrig share observation to parse")
	}
	if obs.Accepted != 3 || obs.Rejected != 0 || obs.Diff != 50000 || obs.Kind != "accepted" {
		t.Fatalf("unexpected observation: %+v", obs)
	}
	wantTimestamp := time.Date(2026, 5, 21, 1, 30, 0, 0, time.UTC)
	if !obs.Timestamp.Equal(wantTimestamp) {
		t.Fatalf("unexpected timestamp: got %s want %s", obs.Timestamp, wantTimestamp)
	}
}

func TestSelectAutoSwitchPortUpgradesAtHighDiffCap(t *testing.T) {
	t.Parallel()

	policy := xmrigPortAutoSwitchPolicy{
		MinAcceptedDelta: 2,
		HighDiffSamples:  2,
		HighDiffRatio:    0.98,
		Ports: []xmrigStratumPortProfile{
			{URL: "stratum+tcp://pool.example.com:3333", MinDiff: 100, MaxDiff: 50000},
			{URL: "stratum+tcp://pool.example.com:5555", MinDiff: 100, MaxDiff: 4000000000},
		},
	}
	observations := []xmrigShareObservation{
		{Accepted: 1, Rejected: 0, Diff: 50000, Kind: "accepted"},
		{Accepted: 2, Rejected: 0, Diff: 50000, Kind: "accepted"},
		{Accepted: 3, Rejected: 0, Diff: 50000, Kind: "accepted"},
	}

	next, reason, ok := selectAutoSwitchPort(policy, "stratum+tcp://pool.example.com:3333", "stratum+tcp://pool.example.com:3333", observations)
	if !ok || next != "stratum+tcp://pool.example.com:5555" || reason != "diff_at_high_cap" {
		t.Fatalf("unexpected switch decision: ok=%v next=%q reason=%q", ok, next, reason)
	}
}

func TestSelectAutoSwitchPortDowngradesAtLowDiffFloor(t *testing.T) {
	t.Parallel()

	policy := xmrigPortAutoSwitchPolicy{
		MinAcceptedDelta: 3,
		LowDiffSamples:   3,
		LowDiffRatio:     1.20,
		Ports: []xmrigStratumPortProfile{
			{URL: "stratum+tcp://pool.example.com:3333", MinDiff: 100, MaxDiff: 50000},
			{URL: "stratum+tcp://pool.example.com:5555", MinDiff: 10000000, MaxDiff: 4000000000},
		},
	}
	observations := []xmrigShareObservation{
		{Accepted: 1, Rejected: 0, Diff: 10000000, Kind: "accepted"},
		{Accepted: 2, Rejected: 0, Diff: 10000000, Kind: "accepted"},
		{Accepted: 3, Rejected: 0, Diff: 10000000, Kind: "accepted"},
		{Accepted: 4, Rejected: 0, Diff: 10000000, Kind: "accepted"},
	}

	next, reason, ok := selectAutoSwitchPort(policy, "stratum+tcp://pool.example.com:3333", "stratum+tcp://pool.example.com:5555", observations)
	if !ok || next != "stratum+tcp://pool.example.com:3333" || reason != "diff_at_low_floor" {
		t.Fatalf("unexpected switch decision: ok=%v next=%q reason=%q", ok, next, reason)
	}
}

func TestSelectAutoSwitchPortEscalatesOnHighRejectRate(t *testing.T) {
	t.Parallel()

	policy := xmrigPortAutoSwitchPolicy{
		FailureMinSamples: 10,
		FailureRejectRate: 0.5,
		Ports: []xmrigStratumPortProfile{
			{URL: "stratum+tcp://pool.example.com:3333", MinDiff: 100, MaxDiff: 50000},
			{URL: "stratum+tcp://pool.example.com:5555", MinDiff: 100, MaxDiff: 4000000000},
		},
	}
	observations := []xmrigShareObservation{
		{Accepted: 10, Rejected: 0, Diff: 50000, Kind: "accepted"},
		{Accepted: 12, Rejected: 20, Diff: 50000, Kind: "rejected"},
	}

	next, reason, ok := selectAutoSwitchPort(policy, "stratum+tcp://pool.example.com:3333", "stratum+tcp://pool.example.com:3333", observations)
	if !ok || next != "stratum+tcp://pool.example.com:5555" || reason != "high_reject_rate_try_higher_port" {
		t.Fatalf("unexpected switch decision: ok=%v next=%q reason=%q", ok, next, reason)
	}
}

func TestSelectAutoSwitchPortIgnoresPreviousXMRigRunAfterCounterReset(t *testing.T) {
	t.Parallel()

	policy := xmrigPortAutoSwitchPolicy{
		MinAcceptedDelta: 20,
		HighDiffSamples:  3,
		HighDiffRatio:    0.98,
		Ports: []xmrigStratumPortProfile{
			{URL: "stratum+tcp://pool.example.com:3333", MinDiff: 100, MaxDiff: 50000},
			{URL: "stratum+tcp://pool.example.com:5555", MinDiff: 100, MaxDiff: 4000000000},
		},
	}
	observations := []xmrigShareObservation{
		{Accepted: 507508, Rejected: 937, Diff: 50000, Kind: "accepted"},
		{Accepted: 1, Rejected: 0, Diff: 50000, Kind: "accepted"},
		{Accepted: 20, Rejected: 0, Diff: 50000, Kind: "accepted"},
		{Accepted: 21, Rejected: 0, Diff: 50000, Kind: "accepted"},
		{Accepted: 22, Rejected: 0, Diff: 50000, Kind: "accepted"},
	}

	next, reason, ok := selectAutoSwitchPort(policy, "stratum+tcp://pool.example.com:3333", "stratum+tcp://pool.example.com:3333", observations)
	if !ok || next != "stratum+tcp://pool.example.com:5555" || reason != "diff_at_high_cap" {
		t.Fatalf("unexpected switch decision after counter reset: ok=%v next=%q reason=%q", ok, next, reason)
	}
}

func TestReadXMRigShareObservationsFiltersByLogTimestamp(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "xmrig.log")
	content := strings.Join([]string{
		`[stdout] [2026-05-31 15:20:00.000]  cpu      accepted (1/0) diff 50000 (239 ms)`,
		`[stdout] [2026-05-31 15:29:00.000]  cpu      accepted (2/0) diff 50000 (239 ms)`,
	}, "\n") + "\n"
	if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write xmrig log: %v", err)
	}

	since := time.Date(2026, 5, 31, 15, 25, 0, 0, time.UTC)
	observations, err := readXMRigShareObservations(logPath, since)
	if err != nil {
		t.Fatalf("readXMRigShareObservations() error = %v", err)
	}
	if len(observations) != 1 || observations[0].Accepted != 2 {
		t.Fatalf("unexpected filtered observations: %+v", observations)
	}
}

func TestLatestXMRigRunShareObservationsIgnoresPreviousRun(t *testing.T) {
	t.Parallel()

	events := []xmrigLogEvent{
		{RunBoundary: &xmrigLogRunBoundary{Timestamp: time.Date(2026, 5, 31, 16, 0, 0, 0, time.UTC)}},
		{Observation: &xmrigShareObservation{Accepted: 10, Rejected: 0, Diff: 50000, Kind: "accepted"}},
		{Observation: &xmrigShareObservation{Accepted: 20, Rejected: 0, Diff: 50000, Kind: "accepted"}},
		{RunBoundary: &xmrigLogRunBoundary{Timestamp: time.Date(2026, 5, 31, 16, 1, 0, 0, time.UTC)}},
	}

	observations := latestXMRigRunShareObservations(events)
	if len(observations) != 0 {
		t.Fatalf("expected latest run to have no shares yet, got %+v", observations)
	}
}

func TestReadXMRigShareStatsUsesLatestShareSummary(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "xmrig.log")
	content := strings.Join([]string{
		`[stdout] [2026-05-20 10:11:38.100]  cpu      accepted (100/5) diff 5000 (1 ms)`,
		`[stdout] [2026-05-20 10:11:39.886]  cpu      rejected (566/3780) diff 5000 "Unknown miner" (1 ms)`,
	}, "\n") + "\n"
	if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write xmrig log: %v", err)
	}

	stats, found, err := readXMRigShareStats(logPath)
	if err != nil {
		t.Fatalf("readXMRigShareStats() error = %v", err)
	}
	if !found {
		t.Fatalf("expected share stats found")
	}
	if stats.Accepted != 566 || stats.Rejected != 3780 {
		t.Fatalf("unexpected latest stats: %+v", stats)
	}
}

func TestXMRigRejectRateMonitorRestartsAboveThreshold(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "xmrig.log")
	if err := os.WriteFile(logPath, []byte(`[stdout] [2026-05-20 10:11:39.886]  cpu      rejected (566/3780) diff 5000 "Unknown miner" (1 ms)`+"\n"), 0o600); err != nil {
		t.Fatalf("write xmrig log: %v", err)
	}

	pm := newMockProcessManager()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		XMRigLogPath:      logPath,
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example.com:3333", testWalletAddress, "XFo2A88ABC", "Rig-4090-Alpha", logger)
	mgr.rejectMonitorInterval = 10 * time.Millisecond
	mgr.rejectMonitorCooldown = time.Hour
	mgr.rejectMonitorThreshold = 0.10
	mgr.rejectMonitorMinShares = 20

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	waitFor(t, func() bool {
		return pm.startCount(xmrigProcessName) >= 2 && pm.stopCount(xmrigProcessName) >= 1
	})
}

func TestXMRigRejectRateMonitorIgnoresTenPercentOrLower(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "xmrig.log")
	if err := os.WriteFile(logPath, []byte(`[stdout] [2026-05-20 10:11:39.886]  cpu      rejected (90/10) diff 5000 (1 ms)`+"\n"), 0o600); err != nil {
		t.Fatalf("write xmrig log: %v", err)
	}

	pm := newMockProcessManager()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		XMRigLogPath:      logPath,
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example.com:3333", testWalletAddress, "XFo2A88ABC", "Rig-4090-Alpha", logger)
	mgr.rejectMonitorInterval = 10 * time.Millisecond
	mgr.rejectMonitorCooldown = time.Hour
	mgr.rejectMonitorThreshold = 0.10
	mgr.rejectMonitorMinShares = 20

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(40 * time.Millisecond)
	if got := pm.startCount(xmrigProcessName); got != 1 {
		t.Fatalf("expected no restart at or below 10%% reject rate, starts=%d", got)
	}
}

func TestXMRigRejectRateMonitorIgnoresStartupAcceptedZeroRejected(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "xmrig.log")
	if err := os.WriteFile(logPath, []byte(`[stdout] [2026-05-21 01:30:00.000]  cpu      accepted (3/0) diff 5000 (1 ms)`+"\n"), 0o600); err != nil {
		t.Fatalf("write xmrig log: %v", err)
	}

	pm := newMockProcessManager()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(pm, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		XMRigLogPath:      logPath,
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example.com:3333", testWalletAddress, "XFo2A88ABC", "Rig-4090-Alpha", logger)
	mgr.rejectMonitorInterval = 10 * time.Millisecond
	mgr.rejectMonitorCooldown = time.Hour
	mgr.rejectMonitorThreshold = 0.10
	mgr.rejectMonitorMinShares = 1

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(40 * time.Millisecond)
	if got := pm.startCount(xmrigProcessName); got != 1 {
		t.Fatalf("expected no restart for accepted startup summary with zero rejects, starts=%d", got)
	}
}

func TestXMRigLogCleanupRemovesOnlyOldLogFamilyMembers(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "xmrig.log")
	now := time.Now()
	files := map[string]time.Time{
		"xmrig.log":                         now,
		"xmrig.log.xfo-100000000000000.log": now.Add(-96 * time.Hour),
		"xmrig.log.xfo-200000000000000.log": now.Add(-24 * time.Hour),
		"xmrig.config":                      now.Add(-96 * time.Hour),
		"xmrig.notes":                       now.Add(-96 * time.Hour),
		"xmrig-20260101.log":                now.Add(-96 * time.Hour),
		"xmrig.1":                           now.Add(-96 * time.Hour),
		"other-20260101.log":                now.Add(-96 * time.Hour),
	}
	for name, modTime := range files {
		path := filepath.Join(logDir, name)
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatalf("chtimes %s: %v", name, err)
		}
	}

	if err := cleanupXMRigLogFamily(logPath, now, 72*time.Hour); err != nil {
		t.Fatalf("cleanupXMRigLogFamily() error = %v", err)
	}

	for _, removed := range []string{"xmrig.log.xfo-100000000000000.log"} {
		if _, err := os.Stat(filepath.Join(logDir, removed)); !os.IsNotExist(err) {
			t.Fatalf("expected %s removed, stat err=%v", removed, err)
		}
	}
	for _, kept := range []string{"xmrig.log", "xmrig.log.xfo-200000000000000.log", "xmrig.config", "xmrig.notes", "xmrig-20260101.log", "xmrig.1", "other-20260101.log"} {
		if _, err := os.Stat(filepath.Join(logDir, kept)); err != nil {
			t.Fatalf("expected %s kept: %v", kept, err)
		}
	}
}

func TestXMRigPrepareLogPathRotatesStaleActiveLog(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "xmrig.log")
	oldTime := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	now := oldTime.Add(49 * time.Hour)
	if err := os.WriteFile(logPath, []byte("old active content"), 0o600); err != nil {
		t.Fatalf("write active log: %v", err)
	}
	if err := os.Chtimes(logPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes active log: %v", err)
	}

	if err := prepareXMRigLogPath(logPath, now); err != nil {
		t.Fatalf("prepareXMRigLogPath() error = %v", err)
	}

	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("expected stale active log moved away, stat err=%v", err)
	}
	rotatedPath := xmrigRotatedLogPath(logPath, now)
	content, err := os.ReadFile(rotatedPath)
	if err != nil {
		t.Fatalf("read rotated active log: %v", err)
	}
	if string(content) != "old active content" {
		t.Fatalf("unexpected rotated content: %q", string(content))
	}
}

func TestXMRigLogSinkRollsActiveLogDuringLongRun(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "xmrig.log")
	now := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)
	sink, err := newXMRigLogSink(logPath, func() time.Time { return now })
	if err != nil {
		t.Fatalf("newXMRigLogSink() error = %v", err)
	}
	defer sink.close()

	if err := sink.writeLine("[stdout]", "day-one"); err != nil {
		t.Fatalf("write first line: %v", err)
	}
	now = now.Add(xmrigActiveLogMaxAge + time.Second)
	if err := sink.writeLine("[stdout]", "day-two"); err != nil {
		t.Fatalf("write second line after age rollover: %v", err)
	}

	rotatedPath := xmrigRotatedLogPath(logPath, now)
	rotated, err := os.ReadFile(rotatedPath)
	if err != nil {
		t.Fatalf("read rotated log: %v", err)
	}
	if !strings.Contains(string(rotated), "day-one") || strings.Contains(string(rotated), "day-two") {
		t.Fatalf("unexpected rotated log content: %q", string(rotated))
	}
	active, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read active log: %v", err)
	}
	if !strings.Contains(string(active), "day-two") || strings.Contains(string(active), "day-one") {
		t.Fatalf("unexpected active log content: %q", string(active))
	}
}

func TestXMRigManagerRedirectsOutputToConfiguredLogFile(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "nested", "xmrig.log")
	fakeXMRigPath := filepath.Join(t.TempDir(), "fake-xmrig.sh")
	if err := os.WriteFile(fakeXMRigPath, []byte("#!/bin/sh\necho xmrig-stdout\necho xmrig-stderr >&2\n"), 0o700); err != nil {
		t.Fatalf("write fake xmrig: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(process.NewRealManager(logger), &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         fakeXMRigPath,
		XMRigLogPath:      logPath,
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example.com:3333", testWalletAddress, "XFo2A88ABC", "Rig-4090-Alpha", logger)
	mgr.watchdogInitialBackoff = time.Hour
	mgr.watchdogMaxBackoff = time.Hour

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	waitFor(t, func() bool {
		content, err := os.ReadFile(logPath)
		if err != nil {
			return false
		}
		text := string(content)
		return strings.Contains(text, "[stdout] xmrig-stdout") && strings.Contains(text, "[stderr] xmrig-stderr")
	})
}

func TestXMRigManagerStopAllowsSubsequentRestart(t *testing.T) {
	t.Parallel()

	proc := newMockProcessManager()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(proc, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := mgr.SetFullMode(context.Background()); err != nil {
		t.Fatalf("SetFullMode() after Stop error = %v", err)
	}

	if got := proc.startCount(xmrigProcessName); got != 2 {
		t.Fatalf("expected xmrig to be started twice, got %d", got)
	}
	if got := proc.stopCount(xmrigProcessName); got != 1 {
		t.Fatalf("expected xmrig to be stopped once, got %d", got)
	}
}

func TestXMRigManagerShutdownPreventsRestart(t *testing.T) {
	t.Parallel()

	proc := newMockProcessManager()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewXMRigManager(proc, &config.CPUMiningConfig{
		Enabled:           true,
		XMRigPath:         "xmrig",
		MaxThreads:        4,
		BackgroundThreads: 1,
	}, "stratum+tcp://pool.example:3333", testWalletAddress, "node-1", "worker-1", logger)

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := mgr.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := mgr.SetFullMode(context.Background()); err == nil {
		t.Fatalf("expected SetFullMode() after Shutdown to fail")
	}
}
