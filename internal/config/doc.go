// Package config owns Home() and config.json: the state root under
// $SHUTTLE_HOME (default ~/.shuttle), the agents directory, timezone,
// delivery and Keychain item names, absolute binary paths, notification
// cooldowns and daily limits, plus the tunables persisted to
// ~/.shuttle/daemon-env for the launchd plist. Secrets never live here.
package config
