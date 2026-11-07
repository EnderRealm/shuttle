//go:build release && !cgo

package reap

// Without cgo this package compiles seatbelt_other.go, whose checks return
// errNoSeatbelt: such a binary builds and runs cleanly, containment fails
// closed, and every reservation stays held. The release build sets the release
// tag so that this undefined name stops that binary at compile time.
var _ = releaseBuildRequiresCgo
