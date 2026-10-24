//go:build !darwin || !cgo

package reap

import "errors"

// errNoSeatbelt is a grant that cannot be read: without sandbox_check no
// process can be shown free of a segment's grant, so containment fails
// closed and the reservation is held.
var errNoSeatbelt = errors.New("seatbelt grants are not readable on this build")

func seatbeltSandboxed(int) (bool, error) { return false, errNoSeatbelt }

func seatbeltWriteDenied(int, string) (bool, error) { return false, errNoSeatbelt }
