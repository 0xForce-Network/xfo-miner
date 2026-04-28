//go:build !windows

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
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
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
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
