//go:build windows

package env

// CheckRoot returns true on Windows builds as a compatibility fallback.
// Admin privilege probing can be implemented in a future hardening pass.
func CheckRoot() bool {
	return true
}
