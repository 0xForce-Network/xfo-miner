//go:build !windows

package process

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const detachedZombieState = "Z"

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

	if gracePeriod <= 0 {
		gracePeriod = 3
	}
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return fmt.Errorf("get detached process group for pid %d: %w", pid, err)
	}
	if pgid <= 0 {
		return fmt.Errorf("invalid detached process group %d for pid %d", pgid, pid)
	}

	if err := killDetachedGroupOrPid(pid, pgid, syscall.SIGTERM); err != nil {
		return err
	}

	deadline := time.Now().Add(time.Duration(gracePeriod) * time.Second)
	if waitForDetachedExit(pid, pgid, deadline) {
		return nil
	}

	if err := killDetachedGroupOrPid(pid, pgid, syscall.SIGKILL); err != nil {
		return err
	}
	if waitForDetachedExit(pid, pgid, time.Now().Add(2*time.Second)) {
		return nil
	}

	return fmt.Errorf("detached process group %d for pid %d still alive after SIGKILL", pgid, pid)
}

func killDetachedGroupOrPid(pid int, pgid int, signal syscall.Signal) error {
	if pgid > 0 {
		if err := syscall.Kill(-pgid, signal); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return nil
			}
			return fmt.Errorf("signal detached process group %d with %s: %w", pgid, signal.String(), err)
		}
		return nil
	}

	if err := syscall.Kill(pid, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return fmt.Errorf("signal detached pid %d with %s: %w", pid, signal.String(), err)
	}
	return nil
}

func waitForDetachedExit(pid int, pgid int, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		if !isProcessGroupRunning(pgid) && !IsProcessRunning(pid) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return !isProcessGroupRunning(pgid) && !IsProcessRunning(pid)
}

func isProcessGroupRunning(pgid int) bool {
	if pgid <= 0 {
		return false
	}
	if runtime.GOOS == "linux" && isLinuxProcessGroupOnlyZombies(pgid) {
		return false
	}
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func isLinuxProcessGroupOnlyZombies(pgid int) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}

	found := false
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		statPGID, state, ok := readLinuxProcStat(pid)
		if !ok || statPGID != pgid {
			continue
		}
		found = true
		if state != detachedZombieState {
			return false
		}
	}
	return found
}

func readLinuxProcStat(pid int) (int, string, bool) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, "", false
	}
	line := string(raw)
	closeIdx := strings.LastIndex(line, ")")
	if closeIdx < 0 || closeIdx+2 >= len(line) {
		return 0, "", false
	}
	fields := strings.Fields(line[closeIdx+2:])
	if len(fields) < 3 {
		return 0, "", false
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return 0, "", false
	}
	return pgid, fields[0], true
}

func IsProcessRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil
}
