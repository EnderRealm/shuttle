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
