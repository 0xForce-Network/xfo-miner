package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	mgr.watchdogInitialBackoff = 10 * time.Millisecond
	mgr.watchdogMaxBackoff = 20 * time.Millisecond

	done := make(chan struct{})
	go mgr.watchProcessExit(done)
	close(done)

	waitFor(t, func() bool {
		pm.mu.Lock()
		defer pm.mu.Unlock()
		return pm.starts >= 2
	})
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
