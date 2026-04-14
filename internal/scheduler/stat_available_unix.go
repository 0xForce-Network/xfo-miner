//go:build !windows

package scheduler

import "syscall"

func statAvailableBytes(path string) (uint64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return 0, err
	}
	return fs.Bavail * uint64(fs.Bsize), nil
}
