//go:build !windows

package env

import "os"

func CheckRoot() bool {
	return os.Getuid() == 0
}
