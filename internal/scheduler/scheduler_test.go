package scheduler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/env"
	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
)

type mockProcessManager struct {
	mu      sync.Mutex
	running map[string]bool
	starts  map[string]int
	stops   map[string]int
}

type mockDetachedController struct {
	mu        sync.Mutex
	running   bool
	currentID int
	starts    int
	stops     int
}

func newMockDetachedController() *mockDetachedController {
	return &mockDetachedController{}
}

func (m *mockDetachedController) start(_ string, _ []string) (*process.DetachedProcess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.starts++
	m.currentID++
	m.running = true
	return &process.DetachedProcess{Pid: m.currentID}, nil
}

func (m *mockDetachedController) stop(_ int, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stops++
	m.running = false
	return nil
}

func (m *mockDetachedController) alive(pid int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running && pid == m.currentID
}

func (m *mockDetachedController) startCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.starts
}

func (m *mockDetachedController) stopCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stops
}

func newMockProcessManager() *mockProcessManager {
	return &mockProcessManager{running: map[string]bool{}, starts: map[string]int{}, stops: map[string]int{}}
}

func (m *mockProcessManager) Start(_ context.Context, name string, _ string, _ []string) (*process.ManagedProcess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running[name] = true
	m.starts[name]++
	return &process.ManagedProcess{Name: name, Done: make(chan struct{})}, nil
}

func (m *mockProcessManager) Stop(_ context.Context, name string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running[name] = false
	m.stops[name]++
	return nil
}

func (m *mockProcessManager) StopAll(_ context.Context, _ time.Duration) error { return nil }
func (m *mockProcessManager) Get(_ string) (*process.ManagedProcess, bool)     { return nil, false }
func (m *mockProcessManager) IsRunning(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running[name]
}

func (m *mockProcessManager) startCount(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.starts[name]
}
func (m *mockProcessManager) stopCount(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stops[name]
}

type mockPoolClient struct {
	mu        sync.Mutex
	handler   func(string, json.RawMessage)
	connected bool
	lastLogin *pool.LoginMessage
	connectErr error
	loginErr   error
	connects   int
	logins     int
}

func (m *mockPoolClient) Connect(_ context.Context, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connects++
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected = true
	return nil
}
func (m *mockPoolClient) Close() error                              { return nil }
func (m *mockPoolClient) SendLogin(login *pool.LoginMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logins++
	if m.loginErr != nil {
		return m.loginErr
	}
	if login == nil {
		m.lastLogin = nil
		return nil
	}
	copy := *login
	m.lastLogin = &copy
	return nil
}
func (m *mockPoolClient) SendHeartbeat() error                                   { return nil }
func (m *mockPoolClient) SendProgress(_ *pool.ProgressMessage) error             { return nil }
func (m *mockPoolClient) SendResult(_ *pool.ResultMessage) error                 { return nil }
func (m *mockPoolClient) SendContainerReady(_ *pool.ContainerReadyMessage) error { return nil }
func (m *mockPoolClient) SendTelemetryL1(_ *pool.TelemetryL1Message) error       { return nil }
func (m *mockPoolClient) SendTelemetryL2(_ *pool.TelemetryL2Message) error       { return nil }
func (m *mockPoolClient) OnMessage(h func(msgType string, raw json.RawMessage)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = h
}
func (m *mockPoolClient) emit(v any) {
	b, _ := json.Marshal(v)
	var envelope struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(b, &envelope)
	m.mu.Lock()
	h := m.handler
	m.mu.Unlock()
	if h != nil {
		h(envelope.Type, b)
	}
}

func (m *mockPoolClient) getLastLogin() *pool.LoginMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastLogin == nil {
		return nil
	}
	copy := *m.lastLogin
	return &copy
}

func (m *mockPoolClient) connectCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connects
}

func (m *mockPoolClient) loginCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logins
}

type mockHashcatRunner struct{}

func (m *mockHashcatRunner) Run(_ context.Context, job *pool.JobGPUMessage, onProgress func(*pool.ProgressMessage), onResult func(*pool.ResultMessage)) error {
	if onProgress != nil {
		onProgress(&pool.ProgressMessage{Type: "progress", JobID: job.JobID, Current: 1, Total: 2, Percent: 50})
	}
	if onResult != nil {
		onResult(&pool.ResultMessage{Type: "result", JobID: job.JobID, Status: "exhausted"})
	}
	return nil
}

type mockContainerRunner struct{}

func (m *mockContainerRunner) Run(_ context.Context, _ *pool.JobContainerMessage) (string, error) {
	return "https://abc.trycloudflare.com", nil
}

type mockOTAUpdater struct {
	mu     sync.Mutex
	called int
}

func (m *mockOTAUpdater) Execute(_ context.Context, _ *pool.OTAUpdateMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.called++
	return nil
}

func (m *mockOTAUpdater) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.called
}

func newTestScheduler() (*Scheduler, *mockProcessManager, *mockPoolClient, *mockDetachedController) {
	proc := newMockProcessManager()
	pcl := &mockPoolClient{}
	detached := newMockDetachedController()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s := New(&config.Config{
		NodeID:     "node-1",
		WorkerName: "worker-1",
		PoolURL:    "wss://pool.example/ws",
		AutoUpdate: config.AutoUpdateConfig{Enabled: true},
		CPUMining: config.CPUMiningConfig{
			Enabled:           false,
			XMRigPath:         "./bin/xmrig",
			MaxThreads:        2,
			BackgroundThreads: 1,
		},
		IdleBehavior: config.IdleBehavior{
			Enabled:        true,
			GracePeriodSec: 1,
			Command:        "idle-miner",
			Args:           "--x",
		},
	}, "0.1.0-test", &env.SystemCapabilities{RunMode: env.RunModeCPUOnly}, proc, pcl, logger)

	s.hashcatRunner = &mockHashcatRunner{}
	s.containerRunner = &mockContainerRunner{}
	s.updater = &mockOTAUpdater{}
	s.startDetached = detached.start
	s.stopDetached = detached.stop
	s.isDetachedAlive = detached.alive
	return s, proc, pcl, detached
}

func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met before timeout")
}

func TestSchedulerStartsInStandby(t *testing.T) {
	s, _, _, detached := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	waitFor(t, func() bool { return detached.startCount() >= 1 })
	if s.CurrentState() != StateStandby {
		t.Fatalf("expected standby, got %s", s.CurrentState())
	}
}

func TestSchedulerTransitionToWPAAudit(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return detached.startCount() >= 1 })

	pcl.emit(pool.JobGPUMessage{Type: "job_gpu", JobID: "job1", HashMode: 22000, Target: "hash", Skip: 0, Limit: 1})
	waitFor(t, func() bool { return detached.stopCount() >= 1 })
}

func TestSchedulerTransitionToAIContainer(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return detached.startCount() >= 1 })

	pcl.emit(pool.JobContainerMessage{Type: "job_container", JobID: "job2", Image: "img", TargetPort: 8080})
	waitFor(t, func() bool { return detached.stopCount() >= 1 })
}

func TestSchedulerReturnsToStandbyAfterJob(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return detached.startCount() >= 1 })

	pcl.emit(pool.JobGPUMessage{Type: "job_gpu", JobID: "job3", HashMode: 22000, Target: "hash", Skip: 0, Limit: 1})
	waitFor(t, func() bool { return detached.startCount() >= 2 })

	if s.CurrentState() != StateStandby {
		t.Fatalf("expected standby after job, got %s", s.CurrentState())
	}
}

func TestSchedulerHandlesPoolStatus(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return detached.startCount() >= 1 })

	pcl.emit(pool.PoolStatusMessage{Type: "pool_status", Status: pool.PoolStatusAwaitingGenesis})
	waitFor(t, func() bool { return s.CurrentPoolStatus() == pool.PoolStatusAwaitingGenesis })
	if s.CurrentState() != StateStandby {
		t.Fatalf("expected standby on awaiting_genesis, got %s", s.CurrentState())
	}

	pcl.emit(pool.LoginAckMessage{Type: "login_ack", Status: pool.PoolStatusArmed})
	waitFor(t, func() bool { return s.CurrentPoolStatus() == pool.PoolStatusArmed })

	pcl.emit(pool.PoolStatusMessage{Type: "pool_status", Status: pool.PoolStatusUnarmed})
	waitFor(t, func() bool { return s.CurrentPoolStatus() == pool.PoolStatusUnarmed })
	if s.CurrentState() != StateStandby {
		t.Fatalf("expected standby on unarmed, got %s", s.CurrentState())
	}
}

func TestSchedulerLoginCarriesVersionAndOS(t *testing.T) {
	s, _, pcl, _ := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	waitFor(t, func() bool { return pcl.getLastLogin() != nil })
	login := pcl.getLastLogin()
	if login == nil {
		t.Fatalf("expected login payload")
	}
	if login.Version != "0.1.0-test" {
		t.Fatalf("unexpected version: got %q", login.Version)
	}
	if login.OS == "" {
		t.Fatalf("expected non-empty os")
	}
}

func TestSchedulerHandlesOTAUpdateRequired(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()
	mockUpdater, ok := s.updater.(*mockOTAUpdater)
	if !ok {
		t.Fatalf("expected mock ota updater")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return detached.startCount() >= 1 })

	pcl.emit(pool.OTAUpdateMessage{
		Type:          "update_required",
		LatestVersion: "0.2.0",
		DownloadURLs:  []string{"https://example.com/xfo-miner"},
		Checksum:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})

	waitFor(t, func() bool { return mockUpdater.callCount() >= 1 })
}

func TestSchedulerRunConnectFailDoesNotExitFatal(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()
	pcl.connectErr = context.DeadlineExceeded

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitFor(t, func() bool { return detached.startCount() >= 1 })
	if got := pcl.connectCount(); got != 1 {
		t.Fatalf("expected connect count 1, got %d", got)
	}
	if got := pcl.loginCount(); got != 0 {
		t.Fatalf("expected login count 0 when connect fails, got %d", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned error on connect failure degrade path: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not exit after context cancel")
	}
}

func TestSchedulerRunLoginFailDoesNotExitFatal(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()
	pcl.loginErr = context.Canceled

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitFor(t, func() bool { return detached.startCount() >= 1 })
	if got := pcl.connectCount(); got != 1 {
		t.Fatalf("expected connect count 1, got %d", got)
	}
	if got := pcl.loginCount(); got != 1 {
		t.Fatalf("expected login count 1, got %d", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned error on login failure degrade path: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not exit after context cancel")
	}
}
