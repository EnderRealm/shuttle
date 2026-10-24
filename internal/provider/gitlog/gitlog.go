// Package gitlog reports the window's commits across every registered project
// checkout plus the reports repo (§8.5). Ticket namespaces with no repository
// — Root among them — are never handed to git.
package gitlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"shuttle/internal/gitenv"
	"shuttle/internal/provider"
	"shuttle/internal/provider/tkconfig"
)

const (
	// gitTimeout bounds one repository. A checkout on a stalled network mount
	// must not hold the run open.
	gitTimeout = 30 * time.Second

	// reportsName is what §9.3's repo is called in the document.
	reportsName = "reports"

	// recordSep starts each commit and fieldSep separates its fields: a subject
	// carries neither, and both survive --shortstat's extra lines.
	recordSep = "\x00"
	fieldSep  = "\x1f"

	logFormat = "--format=%x00%H%x1f%aI%x1f%s%x1f%D"

	stderrTail = 300
)

// shortstatRE matches git's --shortstat summary line; the two counts are absent
// when a commit only added or only removed files.
var shortstatRE = regexp.MustCompile(`(\d+) files? changed(?:, (\d+) insertions?\(\+\))?(?:, (\d+) deletions?\(-\))?`)

type document struct {
	Repos   []repo `json:"repos"`
	Skipped []skip `json:"skipped"`
}

type repo struct {
	Name    string   `json:"name"`
	Path    string   `json:"path"`
	Commits []commit `json:"commits"`
}

type commit struct {
	Hash         string   `json:"hash"`
	Date         string   `json:"date"`
	Subject      string   `json:"subject"`
	Branches     []string `json:"branches"`
	FilesChanged int      `json:"files_changed"`
	Insertions   int      `json:"insertions"`
	Deletions    int      `json:"deletions"`
}

// skip is a repository the document reports nothing for, and why. config.yaml
// registers checkouts from every machine, so a path this one does not have is
// routine — and one broken checkout must not take the whole run down, so a git
// that fails for any other reason is a skip too rather than a provider failure.
// A registration bound to no repository — Root, or a project with no path —
// is a skip with no path, named so the reader knows it was not scanned.
type skip struct {
	Name   string `json:"name"`
	Path   string `json:"path,omitempty"`
	Reason string `json:"reason"`
}

// Run implements provider.Func.
func Run(ctx context.Context, env provider.Env, _ map[string]any, w provider.Window) (json.RawMessage, error) {
	cfg, err := tkconfig.Load(env.TKConfig)
	if err != nil {
		return nil, err
	}
	doc := document{Repos: []repo{}, Skipped: []skip{}}
	bound, unbound := cfg.Repositories()
	for _, u := range unbound {
		doc.Skipped = append(doc.Skipped, skip{Name: u.Name, Reason: u.Reason})
	}
	targets := make([]repo, 0, len(bound)+1)
	for _, name := range bound {
		targets = append(targets, repo{Name: name, Path: cfg.Projects[name].Path})
	}
	if env.ReportsDir != "" {
		if _, err := os.Stat(env.ReportsDir); err == nil {
			targets = append(targets, repo{Name: reportsName, Path: env.ReportsDir})
		}
	}

	for _, target := range targets {
		if _, err := os.Stat(target.Path); err != nil {
			doc.Skipped = append(doc.Skipped, skip{Name: target.Name, Path: target.Path, Reason: err.Error()})
			continue
		}
		out, err := gitLog(ctx, target.Path, w)
		if err != nil {
			doc.Skipped = append(doc.Skipped, skip{Name: target.Name, Path: target.Path, Reason: err.Error()})
			continue
		}
		target.Commits = parse(out, w)
		doc.Repos = append(doc.Repos, target)
	}
	return json.Marshal(doc)
}

func gitLog(ctx context.Context, path string, w provider.Window) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, gitTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", path, "log",
		"--since="+w.From.Format(time.RFC3339), "--until="+w.To.Format(time.RFC3339),
		"--all", "--decorate=short", logFormat, "--shortstat")
	cmd.Env = gitenv.Env()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git log: %w%s", err, tail(stderr.String()))
	}
	return stdout.String(), nil
}

func parse(out string, w provider.Window) []commit {
	commits := []commit{}
	for _, record := range strings.Split(out, recordSep) {
		if strings.TrimSpace(record) == "" {
			continue
		}
		head, rest, _ := strings.Cut(record, "\n")
		fields := strings.Split(head, fieldSep)
		if len(fields) != 4 {
			continue
		}
		date, err := time.Parse(time.RFC3339, fields[1])
		if err != nil {
			continue
		}
		// git's --until is inclusive and matches on the committer date; the
		// window is half-open over the author date the document reports, so the
		// bound is applied again here.
		if !w.Contains(date) {
			continue
		}
		c := commit{Hash: fields[0], Date: fields[1], Subject: fields[2], Branches: decorations(fields[3])}
		if m := shortstatRE.FindStringSubmatch(rest); m != nil {
			c.FilesChanged = atoi(m[1])
			c.Insertions = atoi(m[2])
			c.Deletions = atoi(m[3])
		}
		commits = append(commits, c)
	}
	return commits
}

// decorations turns %D into the names it carries, dropping the "HEAD -> "
// prefix so the branch reads as a branch.
func decorations(s string) []string {
	names := []string{}
	for _, d := range strings.Split(s, ",") {
		d = strings.TrimSpace(d)
		d = strings.TrimPrefix(d, "HEAD -> ")
		if d != "" {
			names = append(names, d)
		}
	}
	return names
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) > stderrTail {
		s = s[len(s)-stderrTail:]
	}
	return ": " + s
}
