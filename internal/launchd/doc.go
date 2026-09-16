// Package launchd installs and removes the scheduler and its crash-recovery
// watchdog. Its lifecycle is copied from loom/internal/launchd: write, plutil
// lint, bootout, wait, bootstrap with retry, and kickstart.
package launchd
