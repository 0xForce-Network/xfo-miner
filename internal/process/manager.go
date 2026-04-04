package process

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

type ManagerOption func(*RealManager)

func WithLogDir(dir string) ManagerOption {
	return func(m *RealManager) {
		m.logDir = dir
	}
}

type RealManager struct {
	mu               sync.RWMutex
	procs            map[string]*ManagedProcess
	logger           *slog.Logger
	defaultGraceTime time.Duration
	logDir           string
}

func NewRealManager(logger *slog.Logger, opts ...ManagerOption) *RealManager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &RealManager{
		procs:            make(map[string]*ManagedProcess),
		logger:           logger,
		defaultGraceTime: 3 * time.Second,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
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
	if m.logDir != "" {
		go m.pipeToFile(proc, name)
	} else {
		go m.pipeToLogger(proc, name)
	}

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
		m.remove(name)
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
		m.remove(name)
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

func (m *RealManager) pipeToFile(proc *ManagedProcess, name string) {
	if err := os.MkdirAll(m.logDir, 0o755); err != nil {
		m.logger.Error("failed to create subprocess log directory", "dir", m.logDir, "error", err)
		m.pipeToLogger(proc, name)
		return
	}

	logPath := filepath.Join(m.logDir, name+".log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		m.logger.Error("failed to open subprocess log file", "name", name, "path", logPath, "error", err)
		m.pipeToLogger(proc, name)
		return
	}
	defer f.Close()

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	writeLine := func(prefix, line string) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = fmt.Fprintf(f, "%s %s\n", prefix, line)
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := ScanLines(proc.Stdout, func(line string) {
			writeLine("[stdout]", line)
		}); err != nil {
			m.logger.Warn("failed reading subprocess stdout", "name", name, "error", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := ScanLines(proc.Stderr, func(line string) {
			writeLine("[stderr]", line)
		}); err != nil {
			m.logger.Warn("failed reading subprocess stderr", "name", name, "error", err)
		}
	}()
	wg.Wait()
}

func (m *RealManager) pipeToLogger(proc *ManagedProcess, name string) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := ScanLines(proc.Stdout, func(line string) {
			m.logger.Info("subprocess output", "name", name, "stream", "stdout", "line", line)
		}); err != nil {
			m.logger.Warn("failed reading subprocess stdout", "name", name, "error", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := ScanLines(proc.Stderr, func(line string) {
			m.logger.Warn("subprocess output", "name", name, "stream", "stderr", "line", line)
		}); err != nil {
			m.logger.Warn("failed reading subprocess stderr", "name", name, "error", err)
		}
	}()
	wg.Wait()
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
