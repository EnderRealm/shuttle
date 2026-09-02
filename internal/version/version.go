// Package version holds the shuttle binary's version string. Release
// builds set Current via ldflags (-X shuttle/internal/version.Current);
// dev builds leave it "dev" and derive a VCS-stamped string from the
// embedded build info.
package version

import (
	"fmt"
	"runtime/debug"
)

// Current is the released semver (e.g. "v1.1.1"), set by ldflags on
// release builds. Empty/unset stays "dev".
var Current = "dev"

// String returns the human-facing version: the release semver when set,
// otherwise a VCS-stamped dev string (dev (<sha7>[, dirty])).
func String() string {
	if Current != "dev" {
		return Current
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return Current
	}
	var revision, dirty string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = ", dirty"
			}
		}
	}
	if revision == "" {
		return Current
	}
	if len(revision) > 7 {
		revision = revision[:7]
	}
	return fmt.Sprintf("dev (%s%s)", revision, dirty)
}
