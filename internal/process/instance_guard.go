package process

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var errInstanceAlreadyRunning = errors.New("miner instance already running")

type InstanceGuard struct {
	path string
	file *os.File
}

func NewInstanceGuard(anchorPath string) (*InstanceGuard, error) {
	anchor := strings.TrimSpace(anchorPath)
	if anchor == "" {
		return nil, errors.New("instance guard anchor path is empty")
	}
	lockPath := filepath.Clean(anchor) + ".instance.lock"
	return &InstanceGuard{path: lockPath}, nil
}

func (g *InstanceGuard) Acquire(timeout time.Duration) error {
	if g == nil {
		return errors.New("instance guard is nil")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		err := g.acquireOnce()
		if err == nil {
			return nil
		}
		if !errors.Is(err, errInstanceAlreadyRunning) {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (g *InstanceGuard) acquireOnce() error {
	if err := os.MkdirAll(filepath.Dir(g.path), 0o755); err != nil {
		return fmt.Errorf("prepare instance guard dir: %w", err)
	}

	f, err := os.OpenFile(g.path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("open instance guard lock file: %w", err)
	}

	if err := lockFile(f); err != nil {
		owner := strings.TrimSpace(readCurrentLockOwner(f))
		_ = f.Close()
		if owner == "" {
			return fmt.Errorf("%w: %s", errInstanceAlreadyRunning, g.path)
		}
		return fmt.Errorf("%w: owner_pid=%s lock=%s", errInstanceAlreadyRunning, owner, g.path)
	}

	if err := f.Truncate(0); err != nil {
		_ = unlockFile(f)
		_ = f.Close()
		return fmt.Errorf("truncate instance guard owner pid file: %w", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		_ = unlockFile(f)
		_ = f.Close()
		return fmt.Errorf("seek instance guard owner pid file: %w", err)
	}

	pid := strconv.Itoa(os.Getpid())
	if _, err := f.WriteAt([]byte(pid+"\n"), 0); err != nil {
		_ = unlockFile(f)
		_ = f.Close()
		return fmt.Errorf("write instance guard owner pid: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = unlockFile(f)
		_ = f.Close()
		return fmt.Errorf("sync instance guard lock file: %w", err)
	}

	g.file = f
	return nil
}

func (g *InstanceGuard) Release() error {
	if g == nil || g.file == nil {
		return nil
	}
	f := g.file
	g.file = nil
	if err := unlockFile(f); err != nil {
		_ = f.Close()
		return fmt.Errorf("unlock instance guard file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close instance guard file: %w", err)
	}
	return nil
}

func readCurrentLockOwner(f *os.File) string {
	if f == nil {
		return ""
	}
	if _, err := f.Seek(0, 0); err != nil {
		return ""
	}
	raw, err := os.ReadFile(f.Name())
	if err != nil {
		return ""
	}
	return string(raw)
}
