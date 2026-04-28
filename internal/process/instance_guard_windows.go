//go:build windows

package process

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
)

var (
	errorLockViolation = syscall.Errno(33)
	kernel32DLL        = syscall.NewLazyDLL("kernel32.dll")
	lockFileExProc     = kernel32DLL.NewProc("LockFileEx")
	unlockFileExProc   = kernel32DLL.NewProc("UnlockFileEx")
)

func lockFile(f *os.File) error {
	if f == nil {
		return errors.New("nil lock file")
	}
	var ol syscall.Overlapped
	r1, _, err := lockFileExProc.Call(
		f.Fd(),
		uintptr(lockfileExclusiveLock|lockfileFailImmediately),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		if errors.Is(err, errorLockViolation) {
			return errInstanceAlreadyRunning
		}
		if err == syscall.Errno(0) {
			return syscall.EINVAL
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
	r1, _, err := unlockFileExProc.Call(
		f.Fd(),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		if err == syscall.Errno(0) {
			return syscall.EINVAL
		}
		return err
	}
	return nil
}
