package priorreports

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"shuttle/internal/provider"
)

func newReports(t *testing.T, names ...string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "daily")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("# "+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func run(t *testing.T, root string, args map[string]any) document {
	t.Helper()
	data, err := Run(context.Background(), provider.Env{ReportsDir: root}, args, provider.Window{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return doc
}

func TestTheLastReportsByName(t *testing.T) {
	root := newReports(t, "2026-08-30.md", "2026-08-31.md", "2026-09-01.md", "notes.txt")
	doc := run(t, root, map[string]any{"agent": "daily", "count": float64(2)})
	if len(doc.Reports) != 2 {
		t.Fatalf("reports = %+v, want two", doc.Reports)
	}
	if doc.Reports[0].File != "2026-09-01.md" || doc.Reports[1].File != "2026-08-31.md" {
		t.Fatalf("files = %q, %q, want the two most recent", doc.Reports[0].File, doc.Reports[1].File)
	}
	if doc.Reports[0].Content != "# 2026-09-01.md" {
		t.Fatalf("content = %q, want the file's body", doc.Reports[0].Content)
	}
}

func TestCountDefaultsToOne(t *testing.T) {
	root := newReports(t, "2026-08-31.md", "2026-09-01.md")
	doc := run(t, root, map[string]any{"agent": "daily"})
	if len(doc.Reports) != 1 || doc.Reports[0].File != "2026-09-01.md" {
		t.Fatalf("reports = %+v, want only the most recent", doc.Reports)
	}
}

// An agent that has never produced a report is the first run, not a failure.
func TestMissingDirectoryEmitsAnEmptyDocument(t *testing.T) {
	data, err := Run(context.Background(), provider.Env{ReportsDir: t.TempDir()},
		map[string]any{"agent": "daily"}, provider.Window{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(data) != `{"reports":[]}` {
		t.Fatalf("data = %s, want an empty document", data)
	}
}

func TestArgsAreValidated(t *testing.T) {
	root := newReports(t)
	for _, args := range []map[string]any{
		{},
		{"agent": ""},
		{"agent": "daily", "count": "two"},
	} {
		if _, err := Run(context.Background(), provider.Env{ReportsDir: root}, args, provider.Window{}); err == nil {
			t.Errorf("args %v: want an error", args)
		}
	}
}

// The agent name is a path segment under the reports root, so a traversal is
// refused before the filesystem is touched.
func TestTraversingAgentNamesAreRefused(t *testing.T) {
	root := newReports(t)
	outside := filepath.Join(filepath.Dir(root), "private")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret.md"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, agent := range []string{"../private", "../../private", "daily/../../private", "/etc"} {
		_, err := Run(context.Background(), provider.Env{ReportsDir: root},
			map[string]any{"agent": agent}, provider.Window{})
		if err == nil || !strings.Contains(err.Error(), "args.agent") {
			t.Errorf("agent %q: err = %v, want one naming args.agent", agent, err)
		}
	}
}

// A name that passes the rule can still be a symlink out of the reports repo.
func TestSymlinkedAgentDirectoryOutsideTheRootIsRefused(t *testing.T) {
	root := newReports(t)
	outside := filepath.Join(filepath.Dir(root), "elsewhere")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "2026-09-01.md"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escapee")); err != nil {
		t.Fatal(err)
	}
	data, err := Run(context.Background(), provider.Env{ReportsDir: root},
		map[string]any{"agent": "escapee"}, provider.Window{})
	if err == nil {
		t.Fatalf("data = %s, want an error", data)
	}
	if strings.Contains(string(data), "secret") {
		t.Fatalf("data = %s, want nothing from outside the root", data)
	}
}

func TestSymlinkedReportOutsideTheRootIsNotRead(t *testing.T) {
	root := newReports(t, "2026-09-01.md")
	outside := filepath.Join(filepath.Dir(root), "secret.md")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "daily", "2026-09-02.md")); err != nil {
		t.Fatal(err)
	}
	data, err := Run(context.Background(), provider.Env{ReportsDir: root},
		map[string]any{"agent": "daily"}, provider.Window{})
	if err == nil {
		t.Fatalf("data = %s, want an error", data)
	}
	if strings.Contains(string(data), "secret") {
		t.Fatalf("data = %s, want nothing from outside the root", data)
	}
}
