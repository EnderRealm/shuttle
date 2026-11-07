// Package launchd installs and removes the scheduler, its crash-recovery
// watchdog and the updater. Its lifecycle is copied from loom/internal/launchd: write, plutil
// lint, bootout, wait, bootstrap with retry, and kickstart.
package launchd
