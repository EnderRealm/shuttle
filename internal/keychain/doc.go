// Package keychain reads generic password items at use time with
// `security find-generic-password -s <item> -w`, so no secret is written
// to config.json, a plist or a run directory.
package keychain
