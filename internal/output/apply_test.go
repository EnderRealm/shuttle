package output

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"shuttle/internal/ledger"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// newReports returns an initialised reports repo and a logger whose buffer the
// caller can read back. The global and system git configs are taken away so the
// commit is made under the identity Apply passes with -c rather than the
// developer's, which is the launchd job's situation (§9.2).
func newReports(t *testing.T) (string, *log.Logger, *bytes.Buffer) {
	t.Helper()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	dir := filepath.Join(t.TempDir(), "reports")
	if err := InitReports(dir); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	return dir, log.New(&logs, "", 0), &logs
}

func TestInitReports(t *testing.T) {
	dir, _, _ := newReports(t)
	if got := git(t, dir, "rev-parse", "--is-inside-work-tree"); got != "true" {
		t.Errorf("rev-parse: got %q, want a work tree", got)
	}
	if got := git(t, dir, "remote"); got != "" {
		t.Errorf("remote: got %q, want none", got)
	}
	// Idempotent: install runs against a live tree.
	if err := InitReports(dir); err != nil {
		t.Fatal(err)
	}
}

func TestApplyCommitsOnce(t *testing.T) {
	dir, logger, logs := newReports(t)
	target := Target{ReportsDir: dir, Agent: "daily", Label: "2026-09-01", RunID: "daily-20260902-043000-8f2a"}

	applied, err := Apply(target, &Outputs{Subject: "shuttle daily", Report: []byte(validReport)}, logger)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "daily", "2026-09-01.md")
	if applied.ReportPath != want {
		t.Errorf("report path: got %s, want %s", applied.ReportPath, want)
	}
	body, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != validReport {
		t.Error("report: not the outputs' bytes")
	}
	if applied.DeliveryStatus != ledger.DeliveryPending {
		t.Errorf("delivery_status: got %s, want %s", applied.DeliveryStatus, ledger.DeliveryPending)
	}
	if applied.Subject != "shuttle daily" {
		t.Errorf("subject: got %q", applied.Subject)
	}
	if applied.Commit == "" || applied.CommitError != "" {
		t.Fatalf("commit: got %q, error %q", applied.Commit, applied.CommitError)
	}
	if got := git(t, dir, "rev-list", "--count", "HEAD"); got != "1" {
		t.Errorf("commits: got %s, want exactly 1", got)
	}
	if got := git(t, dir, "log", "-1", "--format=%s"); got != "daily 2026-09-01 (daily-20260902-043000-8f2a)" {
		t.Errorf("subject: got %q", got)
	}
	if got := git(t, dir, "log", "-1", "--format=%an <%ae>"); got != commitName+" <"+commitEmail+">" {
		t.Errorf("author: got %q, want the identity the commit supplies", got)
	}
	if logs.Len() != 0 {
		t.Errorf("logged on a clean apply: %s", logs)
	}
}

// A re-run for the same slot replaces that slot's report; it never files a
// sibling (§9.2).
func TestApplyOverwritesTheSlot(t *testing.T) {
	dir, logger, _ := newReports(t)
	first := Target{ReportsDir: dir, Agent: "daily", Label: "2026-09-01", RunID: "daily-20260902-043000-8f2a"}
	if _, err := Apply(first, &Outputs{Subject: "one", Report: []byte(validReport)}, logger); err != nil {
		t.Fatal(err)
	}
	second := first
	second.RunID = "daily-20260902-231500-1c9d"
	revised := validReport + "\nRevised after the catch-up run.\n"
	if _, err := Apply(second, &Outputs{Subject: "two", Report: []byte(revised)}, logger); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(filepath.Join(dir, "daily"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "2026-09-01.md" {
		t.Fatalf("agent directory: got %v, want one 2026-09-01.md", entries)
	}
	body, err := os.ReadFile(filepath.Join(dir, "daily", "2026-09-01.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != revised {
		t.Error("report: the re-run did not replace the first attempt")
	}
	if got := git(t, dir, "rev-list", "--count", "HEAD"); got != "2" {
		t.Errorf("commits: got %s, want one per applied run", got)
	}
}

func TestApplyUsesTheDeclaredFileDirectory(t *testing.T) {
	dir, logger, _ := newReports(t)
	target := Target{
		ReportsDir: dir, Agent: "observer", Dir: "assessments",
		Label: "2026-09-01", RunID: "observer-20260902-043000-8f2a",
	}
	applied, err := Apply(target, &Outputs{Subject: "one", Report: []byte(validReport)}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if applied.ReportPath != filepath.Join(dir, "assessments", "2026-09-01.md") {
		t.Fatalf("report path = %s", applied.ReportPath)
	}
	if got := git(t, dir, "log", "-1", "--format=%s"); got != "observer 2026-09-01 (observer-20260902-043000-8f2a)" {
		t.Fatalf("commit subject = %q, want the agent identity", got)
	}
}

// An identical re-run stages nothing: a recorded no-op, not an error.
func TestApplyIdenticalRerunIsANoOp(t *testing.T) {
	dir, logger, logs := newReports(t)
	target := Target{ReportsDir: dir, Agent: "daily", Label: "2026-09-01", RunID: "daily-20260902-043000-8f2a"}
	out := &Outputs{Subject: "one", Report: []byte(validReport)}
	if _, err := Apply(target, out, logger); err != nil {
		t.Fatal(err)
	}
	target.RunID = "daily-20260902-231500-1c9d"
	applied, err := Apply(target, out, logger)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Commit != "" || applied.CommitSkipped == "" || applied.CommitError != "" {
		t.Fatalf("got %+v, want a recorded no-op", applied)
	}
	if got := git(t, dir, "rev-list", "--count", "HEAD"); got != "1" {
		t.Errorf("commits: got %s, want 1", got)
	}
	if logs.Len() != 0 {
		t.Errorf("a no-op logged: %s", logs)
	}
}

// A commit that cannot be made is logged and recorded, and the run continues:
// the report is on disk either way (§9.2).
func TestApplyCommitFailureDoesNotFail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "reports")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	target := Target{ReportsDir: dir, Agent: "daily", Label: "2026-09-01", RunID: "daily-20260902-043000-8f2a"}

	applied, err := Apply(target, &Outputs{Subject: "one", Report: []byte(validReport)}, log.New(&logs, "", 0))
	if err != nil {
		t.Fatalf("apply failed on a repo-less reports directory: %v", err)
	}
	if applied.CommitError == "" {
		t.Error("commit_error: empty, want the git failure recorded")
	}
	if logs.Len() == 0 {
		t.Error("the commit failure was not logged")
	}
	if _, err := os.Stat(applied.ReportPath); err != nil {
		t.Errorf("the report was not written: %v", err)
	}
}

// silent applies the file and skips the email (§9.2).
func TestApplySilentSkipsDelivery(t *testing.T) {
	dir, logger, _ := newReports(t)
	target := Target{ReportsDir: dir, Agent: "daily", Label: "2026-09-01", RunID: "daily-20260902-043000-8f2a"}

	applied, err := Apply(target, &Outputs{Silent: true, Report: []byte("# Daily\n\nNothing happened.\n")}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if applied.DeliveryStatus != ledger.DeliverySkipped {
		t.Errorf("delivery_status: got %s, want %s", applied.DeliveryStatus, ledger.DeliverySkipped)
	}
	if applied.Subject != "" {
		t.Errorf("subject: got %q, want none", applied.Subject)
	}
	if _, err := os.Stat(applied.ReportPath); err != nil {
		t.Errorf("a silent run must still file the report: %v", err)
	}
}

// git's override variables outrank the -C every call passes, so a GIT_DIR left
// in the daemon's environment must not decide which repository install
// initialises and the run commits to (§9.2).
func TestApplyIgnoresAGitDirInTheEnvironment(t *testing.T) {
	other := t.TempDir()
	git(t, other, "init", "--initial-branch=main")
	t.Setenv("GIT_DIR", filepath.Join(other, ".git"))
	dir, logger, logs := newReports(t)
	target := Target{ReportsDir: dir, Agent: "daily", Label: "2026-09-01", RunID: "daily-20260902-043000-8f2a"}

	applied, err := Apply(target, &Outputs{Subject: "one", Report: []byte(validReport)}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Commit == "" || applied.CommitError != "" {
		t.Fatalf("commit: got %q, error %q; logs: %s", applied.Commit, applied.CommitError, logs)
	}
	// The assertions run git themselves; t.Setenv restores the original value.
	os.Unsetenv("GIT_DIR")
	if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
		t.Fatalf("the reports repo was not initialised: %v", err)
	}
	if got := git(t, dir, "log", "-1", "--format=%s"); got != "daily 2026-09-01 (daily-20260902-043000-8f2a)" {
		t.Errorf("reports repo subject: got %q", got)
	}
	if got := git(t, other, "rev-list", "--all", "--count"); got != "0" {
		t.Errorf("the repository named by GIT_DIR got %s commits, want none", got)
	}
}

// One commit per applied run carries that run's report and nothing else:
// content left staged by an operator or an interrupted attempt is neither read
// as this run's change nor swept into its commit (§9.2).
func TestApplyCommitsOnlyTheReport(t *testing.T) {
	dir, logger, _ := newReports(t)
	stray := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(stray, []byte("an operator's staged edit\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", "--", stray)
	target := Target{ReportsDir: dir, Agent: "daily", Label: "2026-09-01", RunID: "daily-20260902-043000-8f2a"}
	out := &Outputs{Subject: "one", Report: []byte(validReport)}

	applied, err := Apply(target, out, logger)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Commit == "" || applied.CommitError != "" {
		t.Fatalf("commit: got %q, error %q", applied.Commit, applied.CommitError)
	}
	if got := git(t, dir, "show", "--name-only", "--format=", "HEAD"); got != "daily/2026-09-01.md" {
		t.Errorf("the commit holds %q, want only the run's report", got)
	}
	if got := git(t, dir, "status", "--porcelain"); got != "A  notes.txt" {
		t.Errorf("status: got %q, want the unrelated file left staged", got)
	}
	// And the same staged file does not make an identical re-run look changed.
	target.RunID = "daily-20260902-231500-1c9d"
	rerun, err := Apply(target, out, logger)
	if err != nil {
		t.Fatal(err)
	}
	if rerun.Commit != "" || rerun.CommitSkipped == "" {
		t.Errorf("got %+v, want a recorded no-op", rerun)
	}
}

func TestWriteApplied(t *testing.T) {
	runDir := t.TempDir()
	applied := &Applied{ReportPath: "/reports/daily/2026-09-01.md", Commit: "abc123", DeliveryStatus: ledger.DeliveryPending}
	if err := WriteApplied(runDir, applied); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(runDir, appliedFile))
	if err != nil {
		t.Fatal(err)
	}
	var got Applied
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got != *applied {
		t.Errorf("got %+v, want %+v", got, *applied)
	}
}
