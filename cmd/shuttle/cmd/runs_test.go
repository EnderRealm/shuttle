package cmd

import (
	"database/sql"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"shuttle/internal/ledger"
)

func TestEscapeControl(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"ordinary text", "died: the runtime exited", "died: the runtime exited"},
		{"utf-8 survives", "provider: 429 — slow down", "provider: 429 — slow down"},
		{"escape sequence", "auth: \x1b[2J\x1b]0;pwned\a", `auth: \x1b[2J\x1b]0;pwned\a`},
		{"newline", "line one\nline two", `line one\nline two`},
		{"tab", "col\tcol", `col\tcol`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeControl(tt.value); got != tt.want {
				t.Fatalf("escapeControl(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestNullOrEscapesTheValue(t *testing.T) {
	if got := nullOr(sql.NullString{}); got != "-" {
		t.Fatalf("unset column = %q, want a dash", got)
	}
	// delivery_error is an email provider's response verbatim; the escaping
	// belongs here, at the render boundary, not on the way into the ledger.
	got := nullOr(sql.NullString{String: "401 \x1b[31munauthorized", Valid: true})
	if got != `401 \x1b[31munauthorized` {
		t.Fatalf("nullOr = %q, want the escape sequence escaped", got)
	}
}

// execStdout runs one command through cobra and returns what it printed. runs
// and show write with fmt.Printf, so the process's stdout is swapped for a pipe.
func execStdout(t *testing.T, args ...string) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	rootCmd.SetArgs(args)
	runErr := rootCmd.Execute()
	os.Stdout = stdout
	rootCmd.SetArgs(nil)
	w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out), runErr
}

// The ledger commands carry no terminal writes, so a typo in config.json must
// not stand between an operator and a recorded failure or its incident.
func TestLedgerCommandsIgnoreAMalformedConfig(t *testing.T) {
	shuttleEnv(t)
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	run, ok, err := store.Claim("daily", time.Now().Add(-time.Hour), ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Fail(run.ID, ledger.ClassDied, "exit 1", ledger.Result{}); err != nil {
		t.Fatal(err)
	}
	incidents, err := store.OpenIncidents()
	if err != nil || len(incidents) != 1 {
		t.Fatalf("open incidents = %v, %v", incidents, err)
	}
	writeConfig(t, `{"notify": {"enabled": true,}`)

	out, err := execStdout(t, "runs")
	if err != nil || !strings.Contains(out, run.ID+"  "+ledger.StatusFailed+"  ") {
		t.Fatalf("runs = %q, %v", out, err)
	}
	out, err = execStdout(t, "show", run.ID)
	if err != nil || !strings.Contains(out, run.ID) || !strings.Contains(out, ledger.ClassDied) {
		t.Fatalf("show = %q, %v", out, err)
	}
	if _, err := execStdout(t, "ack", strconv.FormatInt(incidents[0].ID, 10)); err != nil {
		t.Fatalf("ack: %v", err)
	}
	incidents, err = store.OpenIncidents()
	if err != nil || len(incidents) != 0 {
		t.Fatalf("open incidents after ack = %v, %v", incidents, err)
	}
}

// Ids and scheduled_for values differ in width across agents and zones; every
// column must still start at one offset on every row, separated by spaces.
func TestRunsAlignsColumns(t *testing.T) {
	shuttleEnv(t)
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	pacific := time.FixedZone("", -8*60*60)
	if _, ok, err := store.Skip("daily", time.Date(2026, 11, 8, 4, 30, 0, 0, pacific), ledger.TriggerCron, ledger.SkipMissed); err != nil || !ok {
		t.Fatalf("skip daily: %v", err)
	}
	if _, ok, err := store.Skip("weft-worker", time.Date(2026, 11, 7, 0, 13, 17, 0, time.UTC), ledger.TriggerCron, ledger.SkipMissed); err != nil || !ok {
		t.Fatalf("skip weft-worker: %v", err)
	}
	run, ok, err := store.Claim("nightly", time.Date(2026, 11, 6, 4, 30, 0, 0, pacific), ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Fail(run.ID, ledger.ClassDied, "exit 1", ledger.Result{}); err != nil {
		t.Fatal(err)
	}

	out, err := execStdout(t, "runs")
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	if strings.Contains(out, "\t") {
		t.Fatalf("runs output carries a tab:\n%s", out)
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("runs printed %d rows, want 3:\n%s", len(lines), out)
	}
	var want []int
	for _, line := range lines {
		if strings.HasSuffix(line, " ") {
			t.Fatalf("row %q ends in padding", line)
		}
		var starts []int
		for i := range line {
			if line[i] != ' ' && (i == 0 || line[i-1] == ' ') {
				starts = append(starts, i)
			}
		}
		if len(starts) != 5 {
			t.Fatalf("row %q has %d columns, want 5", line, len(starts))
		}
		if want == nil {
			want = starts
			continue
		}
		if !slices.Equal(starts, want) {
			t.Fatalf("column offsets %v, want %v:\n%s", starts, want, out)
		}
	}
}
