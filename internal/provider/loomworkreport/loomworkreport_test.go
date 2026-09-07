package loomworkreport

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/provider"
)

// fakeLoom writes a stand-in for the binary: the provider's contract with loom
// is the argv and one JSON object on stdout, which a script can hold.
func fakeLoom(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "loom")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func testWindow(t *testing.T) provider.Window {
	t.Helper()
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	w, err := provider.Resolve(provider.WindowPreviousDay, time.Date(2026, 9, 2, 4, 30, 0, 0, loc), loc)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func run(t *testing.T, bin string) (json.RawMessage, error) {
	t.Helper()
	env := provider.Env{LoomHome: t.TempDir(), LoomBin: bin, Logger: log.New(os.Stderr, "", 0)}
	return Run(context.Background(), env, nil, testWindow(t))
}

func TestReportIsStoredVerbatim(t *testing.T) {
	bin := fakeLoom(t, `echo '{"since":"a","until":"b","runs":[{"id":1}],"totals":{"runs":1}}'`)
	data, err := run(t, bin)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := `{"since":"a","until":"b","runs":[{"id":1}],"totals":{"runs":1}}`
	if string(data) != want {
		t.Fatalf("data = %s, want %s", data, want)
	}
}

// A [] runs array is a valid empty day (§8.3).
func TestEmptyWindowSucceeds(t *testing.T) {
	bin := fakeLoom(t, `echo '{"runs":[],"totals":{}}'`)
	data, err := run(t, bin)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(string(data), `"runs":[]`) {
		t.Fatalf("data = %s, want the empty report", data)
	}
}

// The window is passed as the half-open RFC3339 pair loom's --since/--until
// take, and the database is named explicitly rather than left to LOOM_HOME.
func TestArgvCarriesTheWindowAndTheDatabase(t *testing.T) {
	bin := fakeLoom(t, `printf '{"argv":"%s"}\n' "$*"`)
	data, err := run(t, bin)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got struct {
		Argv string `json:"argv"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	w := testWindow(t)
	want := "work-report --since " + w.From.Format(time.RFC3339) + " --until " + w.To.Format(time.RFC3339) + " --db "
	if !strings.HasPrefix(got.Argv, want) || !strings.HasSuffix(got.Argv, "summaries.db") {
		t.Fatalf("argv = %q, want %q plus the database path", got.Argv, want)
	}
}

func TestMissingBinaryFailsTheRun(t *testing.T) {
	_, err := run(t, filepath.Join(t.TempDir(), "loom"))
	if err == nil || !strings.Contains(err.Error(), "work-report") {
		t.Fatalf("err = %v, want one naming the binary", err)
	}
}

func TestNonZeroExitCarriesTheStderrTail(t *testing.T) {
	bin := fakeLoom(t, "echo 'no such database' >&2; exit 1")
	_, err := run(t, bin)
	if err == nil || !strings.Contains(err.Error(), "no such database") {
		t.Fatalf("err = %v, want one carrying the stderr tail", err)
	}
}

func TestOutputThatIsNotOneObjectFails(t *testing.T) {
	for _, c := range []struct{ name, body string }{
		{"not JSON", "echo 'building index...'"},
		{"an array", `echo '[]'`},
		{"two values", `echo '{} {}'`},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := run(t, fakeLoom(t, c.body)); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}
