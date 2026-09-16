package output

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"shuttle/internal/gitenv"
	"shuttle/internal/ledger"
)

const (
	// reportExt is what prior-reports reads back out of the repo (§8.6), so the
	// layout written here is <reports>/<agent>/<label>.md.
	reportExt = ".md"

	// appliedFile is §7.2's record of what §9 and §10 did.
	appliedFile = "applied.json"

	// commitName and commitEmail are passed with -c on the commit rather than
	// relying on a global identity: a launchd job inherits no login-shell
	// configuration, and an unset identity would fail every commit (§9.2).
	commitName  = "shuttle"
	commitEmail = "shuttle@localhost"

	// gitTimeout bounds one call against the reports repo, which is local.
	gitTimeout = 30 * time.Second
)

// Target is the slot an apply files under and the run the commit names (§9.2).
type Target struct {
	ReportsDir string
	Agent      string // identity recorded in the commit subject
	Dir        string // manifest outputs.file.dir; defaults to Agent
	Label      string
	RunID      string
}

// Applied is applied.json (§7.2): what §9 did with the run's outputs and what
// §10 is left to do.
type Applied struct {
	ReportPath     string `json:"report_path"`
	Commit         string `json:"commit,omitempty"`
	CommitSkipped  string `json:"commit_skipped,omitempty"`
	CommitError    string `json:"commit_error,omitempty"`
	Subject        string `json:"subject,omitempty"`
	DeliveryStatus string `json:"delivery_status"`
	DeliveryError  string `json:"delivery_error,omitempty"`
}

// InitReports creates §9.3's repo: a git repository with no remote, since
// adding one is the operator's choice. Idempotent, so `shuttle install` on a
// live tree changes nothing.
func InitReports(dir string) error {
	// 0755 rather than the state root's 0700: the root above this directory is
	// what closes the archive to other local users, and git writes the repo's
	// own objects at the process umask regardless, so tightening this one
	// directory would be cosmetic.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), gitTimeout)
	defer cancel()
	if out, err := runGit(ctx, dir, "init", "--initial-branch=main"); err != nil {
		return fmt.Errorf("git init %s: %w: %s", dir, err, out)
	}
	return nil
}

// Apply performs §9.2's file kind and settles the delivery status the sender
// picks up. The report is written before the commit is attempted, so a repo
// that refuses a commit still leaves the archive correct.
func Apply(t Target, out *Outputs, logger *log.Logger) (*Applied, error) {
	dirName := t.Dir
	if dirName == "" {
		dirName = t.Agent
	}
	dir := filepath.Join(t.ReportsDir, dirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, t.Label+reportExt)
	// A re-run for the same slot replaces that slot's report; it never files a
	// sibling (§9.2).
	if err := os.WriteFile(path, out.Report, 0o644); err != nil {
		return nil, err
	}
	applied := &Applied{ReportPath: path, DeliveryStatus: ledger.DeliveryPending}
	if out.Silent {
		// The archive keeps quiet days; the email is skipped (§9.2).
		applied.DeliveryStatus = ledger.DeliverySkipped
	} else {
		applied.Subject = out.Subject
	}
	// A commit failure is logged and does not fail the run (§9.2): the report
	// is on disk either way and the repo is the operator's to repair.
	if err := commit(t, path, applied); err != nil {
		applied.CommitError = err.Error()
		logger.Printf("reports repo: %v", err)
	}
	return applied, nil
}

// WriteApplied writes applied.json into the run directory (§7.2).
func WriteApplied(runDir string, applied *Applied) error {
	data, err := json.MarshalIndent(applied, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(runDir, appliedFile), append(data, '\n'), 0o644)
}

// commit makes exactly one commit per applied run, the subject naming the agent,
// the slot label and the run id (§9.2).
func commit(t Target, path string, applied *Applied) error {
	ctx, cancel := context.WithTimeout(context.Background(), gitTimeout)
	defer cancel()
	if out, err := runGit(ctx, t.ReportsDir, "add", "--", path); err != nil {
		return fmt.Errorf("git add: %w: %s", err, out)
	}
	// Scoped to the report: content an operator or an interrupted earlier
	// attempt left staged elsewhere is neither read as this run's change nor
	// swept into its commit (§9.2).
	staged, err := runGit(ctx, t.ReportsDir, "diff", "--cached", "--name-only", "--", path)
	if err != nil {
		return fmt.Errorf("git diff --cached: %w: %s", err, staged)
	}
	// A re-run whose report is byte-identical stages nothing: there is no commit
	// to make, and that is a recorded no-op rather than a failure.
	if staged == "" {
		applied.CommitSkipped = "no change since the last applied run"
		return nil
	}
	subject := fmt.Sprintf("%s %s (%s)", t.Agent, t.Label, t.RunID)
	if out, err := runGit(ctx, t.ReportsDir,
		"-c", "user.name="+commitName, "-c", "user.email="+commitEmail,
		"commit", "-m", subject, "--", path); err != nil {
		return fmt.Errorf("git commit: %w: %s", err, out)
	}
	head, err := runGit(ctx, t.ReportsDir, "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("git rev-parse: %w: %s", err, head)
	}
	applied.Commit = head
	return nil
}

func runGit(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.Env = gitenv.Env()
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}
