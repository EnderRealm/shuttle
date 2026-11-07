// Package updater installs new shuttle releases on a host that cannot simply
// restart. Its shape is loom's internal/updater (docs/auto-update-pattern.md
// in loom): a launchd daemon polls the latest GitHub Release, verifies the
// platform tarball against checksums.txt, installs over the running binary
// and re-bootstraps the other jobs, itself last.
//
// Loom's auto-deploy is a restart, and a restart here kills what the daemon
// supervises, leaves a forward-only ledger migrated and reaches desktops that
// decode v2 strictly. So everything that can refuse a release does so before
// dispatch is disturbed, and the install happens only inside a pause on
// weft-worker, once the ledger shows nothing a restart would kill and a
// barrier in the same transaction holds new work until the update ends.
// Anything that fails after the pause falls back to the running build and
// lifts it.
// docs/releasing.md has the release side.
package updater
