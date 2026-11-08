//go:build release && !cgo

package reap

// Without cgo this package compiles procpath_other.go, whose execPath reads no
// exec path: such a binary builds and runs cleanly, but no process reads as an
// exempt launchd service, so an agent user whose services launchd respawns is
// never established empty and its segment and reservation stay held. The
// release build sets the release tag so that this undefined name stops that
// binary at compile time.
var _ = releaseBuildRequiresCgo
