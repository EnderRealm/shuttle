//go:build !darwin

package reap

// describePID has no kernel view to give on a platform whose process identity
// this package cannot read.
func describePID(int) string { return "kernel view unavailable on this platform" }
