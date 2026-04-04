package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/env"
	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
	"github.com/0xforce/xfo-miner/internal/telemetry"
	"github.com/0xforce/xfo-miner/internal/updater"
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
	mu         sync.Mutex
	handler    func(string, json.RawMessage)
	reconnect  func()
	connected  bool
	lastLogin  *pool.LoginMessage
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
func (m *mockPoolClient) Close() error { return nil }
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
func (m *mockPoolClient) OnReconnect(h func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnect = h
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

func (m *mockPoolClient) triggerReconnect() {
	m.mu.Lock()
	h := m.reconnect
	m.mu.Unlock()
	if h != nil {
		h()
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

type mockXMRigController struct {
	mu             sync.Mutex
	heartbeatCalls int
	fullCalls      int
	startCalls     int
	stopCalls      int
}

func (m *mockXMRigController) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalls++
	return nil
}

func (m *mockXMRigController) SetFullMode(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fullCalls++
	return nil
}

func (m *mockXMRigController) SetHeartbeatMode(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCalls++
	return nil
}

func (m *mockXMRigController) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalls++
	return nil
}

func (m *mockXMRigController) heartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeatCalls
}

func (m *mockXMRigController) fullCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fullCalls
}

type contextAwareOTAUpdater struct {
	mu             sync.Mutex
	seenContextErr error
	called         int
}

func (m *contextAwareOTAUpdater) Execute(ctx context.Context, _ *pool.OTAUpdateMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.called++
	m.seenContextErr = ctx.Err()
	if m.seenContextErr != nil {
		return m.seenContextErr
	}
	return nil
}

func (m *contextAwareOTAUpdater) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.called
}

func (m *contextAwareOTAUpdater) contextErr() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.seenContextErr
}

type mockOTAPoller struct {
	mu     sync.Mutex
	runs   int
	runCh  chan struct{}
	errRun error
}

func (m *mockOTAPoller) Run(ctx context.Context) error {
	m.mu.Lock()
	m.runs++
	runCh := m.runCh
	err := m.errRun
	m.mu.Unlock()

	if runCh != nil {
		select {
		case runCh <- struct{}{}:
		default:
		}
	}
	if err != nil {
		return err
	}
	<-ctx.Done()
	return nil
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
		NodeID:            "node-1",
		WorkerName:        "worker-1",
		PoolURL:           "wss://pool.example/ws",
		HostPlatformID:    "host-1",
		PersistentMinerID: "miner-1",
		IdentityMode:      "stable",
		AutoUpdate:        config.AutoUpdateConfig{Enabled: true},
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
	s.newPoller = func(_ updater.Version, _ func(context.Context, *pool.OTAUpdateMessage) error) otaPoller {
		return &mockOTAPoller{}
	}
	s.scanGPUs = func() ([]telemetry.GPUDevice, error) {
		return []telemetry.GPUDevice{{
			DeviceID:          "0",
			DeviceIndex:       0,
			VendorID:          "10de",
			UUIDSource:        "opencl_uuid_khr",
			GPUUUID:           "abc123",
			DeviceFingerprint: "fp01",
			GPUModel:          "RTX",
			PCIBusID:          "0000:01:00.0",
		}}, nil
	}
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
	if login.HostPlatformID != "host-1" || login.PersistentMinerID != "miner-1" || login.IdentityMode != "stable" {
		t.Fatalf("expected stable host identity in login, got %+v", login)
	}
	if len(login.Devices) != 1 || login.Devices[0].GPUUUID == "" {
		t.Fatalf("expected stable gpu devices in login, got %+v", login.Devices)
	}
}

func TestSchedulerLoginCarriesLegacyClaimWhenMigrationPending(t *testing.T) {
	s, _, pcl, _ := newTestScheduler()

	tempDir := t.TempDir()
	identityPath := filepath.Join(tempDir, "identity_state.json")
	raw := []byte(`{"persistent_miner_id":"miner-1","old_worker_name":"worker-legacy","migration_completed":false}`)
	if err := os.WriteFile(identityPath, raw, 0o600); err != nil {
		t.Fatalf("write identity state: %v", err)
	}
	s.cfg.SetIdentityStatePath(identityPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	waitFor(t, func() bool { return pcl.getLastLogin() != nil })
	login := pcl.getLastLogin()
	if login == nil || login.LegacyClaim == nil {
		t.Fatalf("expected login legacy_claim payload")
	}
	if login.LegacyClaim.OldWorkerName != "worker-legacy" {
		t.Fatalf("unexpected legacy old worker name: %q", login.LegacyClaim.OldWorkerName)
	}
	if login.LegacyClaim.MigrationReason != "uuid_upgrade" {
		t.Fatalf("unexpected migration reason: %q", login.LegacyClaim.MigrationReason)
	}
}

func TestSchedulerLoginAckMarksMigrationCompleted(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()

	tempDir := t.TempDir()
	identityPath := filepath.Join(tempDir, "identity_state.json")
	raw := []byte(`{"persistent_miner_id":"miner-1","old_worker_name":"worker-legacy","migration_completed":false}`)
	if err := os.WriteFile(identityPath, raw, 0o600); err != nil {
		t.Fatalf("write identity state: %v", err)
	}
	s.cfg.SetIdentityStatePath(identityPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return detached.startCount() >= 1 })

	pcl.emit(pool.LoginAckMessage{Type: "login_ack", Status: pool.PoolStatusArmed, MigrationStatus: "no_legacy_stake"})

	waitFor(t, func() bool {
		payload, err := os.ReadFile(identityPath)
		if err != nil {
			return false
		}
		var state map[string]any
		if err := json.Unmarshal(payload, &state); err != nil {
			return false
		}
		flag, ok := state["migration_completed"].(bool)
		return ok && flag
	})

	login := s.buildLoginMessage()
	if login.LegacyClaim != nil {
		t.Fatalf("expected legacy_claim to stop after migration completed")
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

func TestSchedulerHandlesOTAUpdateRequiredWhenAutoUpdateDisabled(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()
	s.cfg.AutoUpdate.Enabled = false
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

func TestSchedulerResendsLoginOnReconnect(t *testing.T) {
	s, _, pcl, detached := newTestScheduler()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitFor(t, func() bool { return detached.startCount() >= 1 })
	waitFor(t, func() bool { return pcl.loginCount() >= 1 })

	pcl.triggerReconnect()
	waitFor(t, func() bool { return pcl.loginCount() >= 2 })

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not exit after context cancel")
	}
}

func TestSchedulerStartsPollerWhenAutoUpdateEnabled(t *testing.T) {
	s, _, _, detached := newTestScheduler()

	runCh := make(chan struct{}, 1)
	created := 0
	s.newPoller = func(_ updater.Version, _ func(context.Context, *pool.OTAUpdateMessage) error) otaPoller {
		created++
		return &mockOTAPoller{runCh: runCh}
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitFor(t, func() bool { return detached.startCount() >= 1 })
	waitFor(t, func() bool {
		select {
		case <-runCh:
			return true
		default:
			return false
		}
	})
	if created != 1 {
		t.Fatalf("expected poller to be created once, got %d", created)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not exit after context cancel")
	}
}

func TestSchedulerSkipsPollerWhenAutoUpdateDisabled(t *testing.T) {
	s, _, _, detached := newTestScheduler()
	s.cfg.AutoUpdate.Enabled = false

	created := 0
	s.newPoller = func(_ updater.Version, _ func(context.Context, *pool.OTAUpdateMessage) error) otaPoller {
		created++
		return &mockOTAPoller{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitFor(t, func() bool { return detached.startCount() >= 1 })
	time.Sleep(50 * time.Millisecond)
	if created != 0 {
		t.Fatalf("expected poller not to be created, got %d", created)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not exit after context cancel")
	}
}

func TestSchedulerRestoreStandbyAfterOTAFailure(t *testing.T) {
	s, _, _, detached := newTestScheduler()

	xm := &mockXMRigController{}
	s.xmrigManager = xm
	s.updater = otaUpdaterFunc(func(_ context.Context, _ *pool.OTAUpdateMessage) error {
		return context.DeadlineExceeded
	})

	if err := s.enterStandby(context.Background()); err != nil {
		t.Fatalf("enterStandby() error = %v", err)
	}

	s.handleOTAUpdate(context.Background(), &pool.OTAUpdateMessage{
		Type:          "update_required",
		LatestVersion: "0.2.0",
		DownloadURLs:  []string{"https://example.com/xfo-miner"},
		Checksum:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})

	if s.CurrentState() != StateStandby {
		t.Fatalf("expected standby after OTA failure, got %s", s.CurrentState())
	}
	if xm.heartbeatCount() < 1 {
		t.Fatalf("expected xmrig heartbeat mode before OTA")
	}
	if xm.fullCount() < 2 {
		t.Fatalf("expected xmrig full mode restored after OTA failure, got %d", xm.fullCount())
	}
	if detached.startCount() < 2 {
		t.Fatalf("expected idle miner restarted after OTA failure")
	}
}

func TestSchedulerOTAUsesParentContextCancellation(t *testing.T) {
	s, _, _, _ := newTestScheduler()

	ctxUpdater := &contextAwareOTAUpdater{}
	s.updater = ctxUpdater
	s.xmrigManager = &mockXMRigController{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.handleOTAUpdate(ctx, &pool.OTAUpdateMessage{
		Type:          "update_required",
		LatestVersion: "0.2.0",
		DownloadURLs:  []string{"https://example.com/xfo-miner"},
		Checksum:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})

	if ctxUpdater.callCount() != 1 {
		t.Fatalf("expected updater to be invoked once, got %d", ctxUpdater.callCount())
	}
	if !errors.Is(ctxUpdater.contextErr(), context.Canceled) {
		t.Fatalf("expected updater context canceled, got %v", ctxUpdater.contextErr())
	}
}

type otaUpdaterFunc func(context.Context, *pool.OTAUpdateMessage) error

func (f otaUpdaterFunc) Execute(ctx context.Context, ota *pool.OTAUpdateMessage) error {
	return f(ctx, ota)
}
