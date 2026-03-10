package process

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestManager() *RealManager {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewRealManager(logger)
}

func TestStartAndStop(t *testing.T) {
	t.Parallel()

	mgr := newTestManager()
	ctx := context.Background()

	if _, err := mgr.Start(ctx, "sleepy", "sleep", []string{"30"}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if !mgr.IsRunning("sleepy") {
		t.Fatalf("process should be running")
	}

	if err := mgr.Stop(ctx, "sleepy", 200*time.Millisecond); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if mgr.IsRunning("sleepy") {
		t.Fatalf("process should be stopped")
	}
}

func TestForceKill(t *testing.T) {
	t.Parallel()

	mgr := newTestManager()
	ctx := context.Background()

	cmd := "trap '' TERM; while true; do sleep 1; done"
	if _, err := mgr.Start(ctx, "term-ignore", "sh", []string{"-c", cmd}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := mgr.Stop(ctx, "term-ignore", 100*time.Millisecond); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if mgr.IsRunning("term-ignore") {
		t.Fatalf("process should be force killed")
	}
}

func TestStopNonExistent(t *testing.T) {
	t.Parallel()

	mgr := newTestManager()
	if err := mgr.Stop(context.Background(), "missing", 100*time.Millisecond); err != nil {
		t.Fatalf("Stop() for non-existent process should not fail: %v", err)
	}
}

func TestStdoutCapture(t *testing.T) {
	t.Parallel()

	mgr := newTestManager()
	proc, err := mgr.Start(context.Background(), "echo", "sh", []string{"-c", "echo hello"})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	lines := make([]string, 0)
	mu := sync.Mutex{}
	if err := ScanLines(proc.Stdout, func(line string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
	}); err != nil {
		t.Fatalf("ScanLines() error = %v", err)
	}

	if err := proc.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lines) == 0 || lines[0] != "hello" {
		t.Fatalf("expected stdout line 'hello', got %#v", lines)
	}
}

func TestStopAll(t *testing.T) {
	t.Parallel()

	mgr := newTestManager()
	ctx := context.Background()

	if _, err := mgr.Start(ctx, "p1", "sleep", []string{"30"}); err != nil {
		t.Fatalf("Start(p1) error = %v", err)
	}
	if _, err := mgr.Start(ctx, "p2", "sleep", []string{"30"}); err != nil {
		t.Fatalf("Start(p2) error = %v", err)
	}

	if err := mgr.StopAll(ctx, 200*time.Millisecond); err != nil {
		t.Fatalf("StopAll() error = %v", err)
	}
	if mgr.IsRunning("p1") || mgr.IsRunning("p2") {
		t.Fatalf("all managed processes should be stopped")
	}
}

func TestRealManagerCapturesSubprocessOutput(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewRealManager(logger, WithLogDir(logDir))

	proc, err := mgr.Start(context.Background(), "echoer", "sh", []string{"-c", "echo hello"})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := proc.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	logPath := filepath.Join(logDir, "echoer.log")
	var content []byte
	for i := 0; i < 20; i++ {
		content, err = os.ReadFile(logPath)
		if err == nil && strings.Contains(string(content), "[stdout] hello") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	if !strings.Contains(string(content), "[stdout] hello") {
		t.Fatalf("expected log to contain stdout line, got %q", string(content))
	}
}
