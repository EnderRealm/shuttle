package reap

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// A no-cgo binary builds and runs, and an agent user's kill then never
// establishes the user empty on the host, so the release tag has to turn that
// build into a failure.
func TestReleaseBuildRefusesNoCgo(t *testing.T) {
	build := func(cgo string) ([]byte, error) {
		cmd := exec.Command("go", "build", "-tags", "release", "-o", os.DevNull, ".")
		cmd.Env = append(os.Environ(), "CGO_ENABLED="+cgo)
		return cmd.CombinedOutput()
	}
	out, err := build("0")
	if err == nil {
		t.Fatal("a release build without cgo succeeded")
	}
	if !strings.Contains(string(out), "releaseBuildRequiresCgo") {
		t.Fatalf("it failed for another reason: %s", out)
	}
	if out, err := build("1"); err != nil {
		t.Fatalf("a release build with cgo failed: %v\n%s", err, out)
	}
}
