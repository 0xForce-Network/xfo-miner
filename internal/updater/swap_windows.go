//go:build windows

package updater

import (
	"context"
	"errors"
	"fmt"
	"os"
)

func (u *Updater) swapBinary(ctx context.Context, downloadedFile string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	oldPath := oldBinaryPath(u.executablePath)
	if err := os.Remove(oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cleanup previous old binary: %w", err)
	}

	if err := os.Rename(u.executablePath, oldPath); err != nil {
		return fmt.Errorf("rename current binary to old tmp: %w", err)
	}

	if err := os.Rename(downloadedFile, u.executablePath); err != nil {
		_ = os.Rename(oldPath, u.executablePath)
		return fmt.Errorf("place updated binary: %w", err)
	}

	procAttr := &os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Env:   u.environ,
	}
	proc, err := os.StartProcess(u.executablePath, u.args, procAttr)
	if err != nil {
		_ = os.Rename(u.executablePath, downloadedFile)
		_ = os.Rename(oldPath, u.executablePath)
		return fmt.Errorf("start updated process: %w", err)
	}
	if err := u.completeWindowsHandoff(osHandoffProcess{proc: proc}); err != nil {
		return fmt.Errorf("complete windows ota handoff: %w", err)
	}
	return nil
}

func CleanupOldBinary() error {
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}

	oldPath := oldBinaryPath(execPath)
	if err := os.Remove(oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove old tmp binary: %w", err)
	}
	return nil
}