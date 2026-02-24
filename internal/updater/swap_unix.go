//go:build !windows

package updater

import (
	"context"
	"fmt"
	"os"
	"syscall"
)

func (u *Updater) swapBinary(_ context.Context, downloadedFile string) error {
	if err := os.Rename(downloadedFile, u.executablePath); err != nil {
		return fmt.Errorf("replace executable: %w", err)
	}

	if err := syscall.Exec(u.executablePath, u.args, u.environ); err != nil {
		return fmt.Errorf("exec updated binary: %w", err)
	}
	return nil
}

func CleanupOldBinary() error {
	return nil
}