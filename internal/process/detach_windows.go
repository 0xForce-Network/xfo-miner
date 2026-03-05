//go:build windows

package process

import "fmt"

type DetachedProcess struct {
	Pid int
}

func StartDetached(command string, args []string) (*DetachedProcess, error) {
	_ = command
	_ = args
	return nil, fmt.Errorf("detached process not supported on Windows")
}

func StopDetached(pid int, gracePeriod int) error {
	_ = pid
	_ = gracePeriod
	return fmt.Errorf("detached process not supported on Windows")
}

func IsProcessRunning(pid int) bool {
	_ = pid
	return false
}
