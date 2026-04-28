//go:build windows

package process

import (
	"errors"
	"os"
	"syscall"
)

func lockFile(f *os.File) error {
	if f == nil {
		return errors.New("nil lock file")
	}
	var ol syscall.Overlapped
	err := syscall.LockFileEx(syscall.Handle(f.Fd()), syscall.LOCKFILE_EXCLUSIVE_LOCK|syscall.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ol)
	if err != nil {
		if errors.Is(err, syscall.ERROR_LOCK_VIOLATION) {
			return errInstanceAlreadyRunning
		}
		return err
	}
	return nil
}

func unlockFile(f *os.File) error {
	if f == nil {
		return nil
	}
	var ol syscall.Overlapped
	return syscall.UnlockFileEx(syscall.Handle(f.Fd()), 0, 1, 0, &ol)
}
