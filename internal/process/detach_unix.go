//go:build !windows

package process

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type DetachedProcess struct {
	Pid int
}

func StartDetached(command string, args []string) (*DetachedProcess, error) {
	if strings.TrimSpace(command) == "" {
		return nil, errors.New("empty command")
	}

	setsidPath, err := exec.LookPath("setsid")
	if err != nil {
		return startDetachedFallback(command, args)
	}

	pidFile := fmt.Sprintf("/tmp/xfo-miner-detached-%d.pid", time.Now().UnixNano())
	script := "echo $$ > \"$1\"; shift; exec \"$@\" </dev/null >/dev/null 2>&1"

	cmdArgs := []string{"--fork", "sh", "-c", script, "sh", pidFile, command}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command(setsidPath, cmdArgs...)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start setsid wrapper: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("wait setsid wrapper: %w", err)
	}

	pid, err := waitAndReadPID(pidFile, 2*time.Second)
	_ = os.Remove(pidFile)
	if err != nil {
		return nil, err
	}

	return &DetachedProcess{Pid: pid}, nil
}

func startDetachedFallback(command string, args []string) (*DetachedProcess, error) {
	devNull, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open /dev/null: %w", err)
	}
	defer devNull.Close()

	cmd := exec.Command(command, args...)
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start detached fallback: %w", err)
	}

	pid := cmd.Process.Pid
	if err := cmd.Process.Release(); err != nil {
		return nil, fmt.Errorf("release detached fallback process: %w", err)
	}

	return &DetachedProcess{Pid: pid}, nil
}

func waitAndReadPID(pidFile string, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(pidFile)
		if err == nil {
			pidStr := strings.TrimSpace(string(raw))
			pid, parseErr := strconv.Atoi(pidStr)
			if parseErr != nil || pid <= 0 {
				return 0, fmt.Errorf("parse detached pid %q: %w", pidStr, parseErr)
			}
			return pid, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("read detached pid file: %w", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	return 0, fmt.Errorf("timeout waiting detached pid file %s", pidFile)
}

func StopDetached(pid int, gracePeriod int) error {
	if pid <= 0 {
		return nil
	}

	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return fmt.Errorf("sigterm detached pid %d: %w", pid, err)
	}

	if gracePeriod <= 0 {
		gracePeriod = 3
	}

	deadline := time.Now().Add(time.Duration(gracePeriod) * time.Second)
	for time.Now().Before(deadline) {
		if !IsProcessRunning(pid) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("sigkill detached pid %d: %w", pid, err)
	}

	return nil
}

func IsProcessRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil
}
