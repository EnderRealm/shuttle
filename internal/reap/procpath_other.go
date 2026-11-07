//go:build !darwin || !cgo

package reap

import "errors"

// execPath cannot be read without libproc: no process reads as an exempt
// service, so a user whose services launchd respawns is never established
// empty and its segment stays unsettled.
func execPath(int) (string, error) {
	return "", errors.New("exec paths are not readable on this build")
}
