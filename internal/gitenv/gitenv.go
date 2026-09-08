// Package gitenv supplies the environment every git the harness execs is run
// with.
package gitenv

import (
	"os"
	"strings"
)

// Env is os.Environ() with git's three override variables stripped, weft's
// CommandWorker rule: they outrank the -C every call passes, so a GIT_DIR left
// in the daemon's environment would point every checkout's log at one
// repository (§8.5) and every reports-repo commit at another (§9.2).
func Env() []string {
	env := os.Environ()
	out := make([]string, 0, len(env))
	for _, kv := range env {
		switch {
		case strings.HasPrefix(kv, "GIT_DIR="),
			strings.HasPrefix(kv, "GIT_WORK_TREE="),
			strings.HasPrefix(kv, "GIT_INDEX_FILE="):
		default:
			out = append(out, kv)
		}
	}
	return out
}
