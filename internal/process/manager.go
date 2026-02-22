package process

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"time"
)

// Manager defines process lifecycle controls for managed subprocesses.
type Manager interface {
	Start(ctx context.Context, name string, command string, args []string) (*ManagedProcess, error)
	Stop(ctx context.Context, name string, gracePeriod time.Duration) error
	StopAll(ctx context.Context, gracePeriod time.Duration) error
	Get(name string) (*ManagedProcess, bool)
	IsRunning(name string) bool
}

type RealManager struct {
	mu               sync.RWMutex
	procs            map[string]*ManagedProcess
	logger           *slog.Logger
	defaultGraceTime time.Duration
}

func NewRealManager(logger *slog.Logger) *RealManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &RealManager{
		procs:            make(map[string]*ManagedProcess),
		logger:           logger,
		defaultGraceTime: 3 * time.Second,
	}
}

func (m *RealManager) Start(ctx context.Context, name string, command string, args []string) (*ManagedProcess, error) {
	m.mu.Lock()
	if _, exists := m.procs[name]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("process %q already managed", name)
	}

	proc := NewManagedProcess(name, command, args)
	m.procs[name] = proc
	m.mu.Unlock()

	if err := proc.Start(ctx); err != nil {
		m.mu.Lock()
		delete(m.procs, name)
		m.mu.Unlock()
		return nil, err
	}

	m.logger.Info("process started", "name", name, "command", command)
	go func(procName string, done <-chan struct{}) {
		<-done
		m.remove(procName)
	}(name, proc.Done)

	return proc, nil
}

func (m *RealManager) Stop(ctx context.Context, name string, gracePeriod time.Duration) error {
	proc, ok := m.Get(name)
	if !ok {
		return nil
	}

	if gracePeriod <= 0 {
		gracePeriod = m.defaultGraceTime
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		if !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("send SIGTERM to %q: %w", name, err)
		}
	}

	select {
	case <-proc.Done:
		m.logger.Info("process stopped gracefully", "name", name)
		return nil
	case <-time.After(gracePeriod):
		m.logger.Warn("grace period exceeded; escalating to SIGKILL", "name", name, "grace_period", gracePeriod)
	case <-ctx.Done():
		return fmt.Errorf("stop process %q canceled before grace timeout: %w", name, ctx.Err())
	}

	if err := proc.Signal(syscall.SIGKILL); err != nil {
		if !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("send SIGKILL to %q: %w", name, err)
		}
	}

	select {
	case <-proc.Done:
		m.logger.Info("process stopped forcefully", "name", name)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait after SIGKILL for %q canceled: %w", name, ctx.Err())
	}
}

func (m *RealManager) StopAll(ctx context.Context, gracePeriod time.Duration) error {
	m.mu.RLock()
	names := make([]string, 0, len(m.procs))
	for name := range m.procs {
		names = append(names, name)
	}
	m.mu.RUnlock()

	errList := make([]error, 0)
	for _, name := range names {
		if err := m.Stop(ctx, name, gracePeriod); err != nil {
			errList = append(errList, err)
		}
	}

	return errors.Join(errList...)
}

func (m *RealManager) Get(name string) (*ManagedProcess, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	proc, ok := m.procs[name]
	return proc, ok
}

func (m *RealManager) IsRunning(name string) bool {
	proc, ok := m.Get(name)
	if !ok {
		return false
	}
	return proc.IsRunning()
}

func (m *RealManager) remove(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.procs, name)
}

type NoopManager struct{}

func NewNoopManager() *NoopManager {
	return &NoopManager{}
}

func (m *NoopManager) Start(_ context.Context, name string, command string, args []string) (*ManagedProcess, error) {
	_ = name
	_ = command
	_ = args
	return nil, nil
}

func (m *NoopManager) Stop(_ context.Context, _ string, _ time.Duration) error {
	return nil
}

func (m *NoopManager) StopAll(_ context.Context, _ time.Duration) error {
	return nil
}

func (m *NoopManager) Get(_ string) (*ManagedProcess, bool) {
	return nil, false
}

func (m *NoopManager) IsRunning(_ string) bool {
	return false
}
