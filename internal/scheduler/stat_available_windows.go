//go:build windows

package scheduler

import (
	"path/filepath"
	"syscall"
	"unsafe"
)

var (
	kernel32DLL              = syscall.NewLazyDLL("kernel32.dll")
	procGetDiskFreeSpaceExW  = kernel32DLL.NewProc("GetDiskFreeSpaceExW")
)

func statAvailableBytes(path string) (uint64, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return 0, err
	}
	pathPtr, err := syscall.UTF16PtrFromString(absPath)
	if err != nil {
		return 0, err
	}

	var freeBytes uint64
	r1, _, callErr := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytes)),
		0,
		0,
	)
	if r1 == 0 {
		if callErr != syscall.Errno(0) {
			return 0, callErr
		}
		return 0, syscall.EINVAL
	}

	return freeBytes, nil
}
