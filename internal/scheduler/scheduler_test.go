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
}

func (m *mockPoolClient) Connect(_ context.Context, _ string) error              { m.connected = true; return nil }
func (m *mockPoolClient) Close() error                                           { return nil }
func (m *mockPoolClient) SendLogin(_ *pool.LoginMessage) error                   { return nil }
func (m *mockPoolClient) SendHeartbeat() error                                   { return nil }
func (m *mockPoolClient) SendProgress(_ *pool.ProgressMessage) error             { return nil }
func (m *mockPoolClient) SendResult(_ *pool.ResultMessage) error                 { return nil }
func (m *mockPoolClient) SendContainerReady(_ *pool.ContainerReadyMessage) error { return nil }
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

func newTestScheduler() (*Scheduler, *mockProcessManager, *mockPoolClient) {
	proc := newMockProcessManager()
	pcl := &mockPoolClient{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s := New(&config.Config{
		NodeID:     "node-1",
		WorkerName: "worker-1",
		PoolURL:    "wss://pool.example/ws",
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
	}, &env.SystemCapabilities{RunMode: env.RunModeCPUOnly}, proc, pcl, logger)

	s.hashcatRunner = &mockHashcatRunner{}
	s.containerRunner = &mockContainerRunner{}
	return s, proc, pcl
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
	s, proc, _ := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	waitFor(t, func() bool { return proc.startCount("idle_miner") >= 1 })
	if s.CurrentState() != StateStandby {
		t.Fatalf("expected standby, got %s", s.CurrentState())
	}
}

func TestSchedulerTransitionToWPAAudit(t *testing.T) {
	s, proc, pcl := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return proc.startCount("idle_miner") >= 1 })

	pcl.emit(pool.JobGPUMessage{Type: "job_gpu", JobID: "job1", HashMode: 22000, Target: "hash", Skip: 0, Limit: 1})
	waitFor(t, func() bool { return proc.stopCount("idle_miner") >= 1 })
}

func TestSchedulerTransitionToAIContainer(t *testing.T) {
	s, proc, pcl := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return proc.startCount("idle_miner") >= 1 })

	pcl.emit(pool.JobContainerMessage{Type: "job_container", JobID: "job2", Image: "img", TargetPort: 8080})
	waitFor(t, func() bool { return proc.stopCount("idle_miner") >= 1 })
}

func TestSchedulerReturnsToStandbyAfterJob(t *testing.T) {
	s, proc, pcl := newTestScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return proc.startCount("idle_miner") >= 1 })

	pcl.emit(pool.JobGPUMessage{Type: "job_gpu", JobID: "job3", HashMode: 22000, Target: "hash", Skip: 0, Limit: 1})
	waitFor(t, func() bool { return proc.startCount("idle_miner") >= 2 })

	if s.CurrentState() != StateStandby {
		t.Fatalf("expected standby after job, got %s", s.CurrentState())
	}
}
