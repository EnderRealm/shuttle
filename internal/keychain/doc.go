// Package keychain reads generic password items at use time with
// `security find-generic-password -s <item> -w`, or with -g where a value's
// bytes have to be told from their hex, so no secret is written to
// config.json, a plist or a run directory.
package keychain
