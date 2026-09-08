//go:build !darwin

package cmd

import (
	"errors"
	"time"
)

// The test that uses procStart skips off darwin; this keeps the package
// building there.
func procStart(int) (time.Time, error) {
	return time.Time{}, errors.New("process start time is not readable on this platform")
}

// Nothing to wait for where the argv cannot be read at all.
func procArgvCarries(int, string) bool { return true }
