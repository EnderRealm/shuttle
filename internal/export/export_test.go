package export

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A live run renames its staging away while the export walks runs/: a file
// or directory listed and then gone before it is read is logged as vanished,
// and the rest of runs/ and all of reports/ are still copied.
func TestCopyTreeSkipsAnEntryThatVanishes(t *testing.T) {
	home := t.TempDir()
	destination := t.TempDir()
	src := testSource(t, home)
	write := func(path string) { writeSource(t, path, path) }
	kept := filepath.Join(src.Runs, "daily", "run-1", "result.json")
	gone := filepath.Join(src.Runs, "daily", "run-1", ".result-123")
	staging := filepath.Join(src.Runs, "daily", "run-2.observer-evidence")
	report := filepath.Join(src.Reports, "daily", "2026-11-26.md")
	for _, path := range []string{kept, gone, filepath.Join(staging, "evidence.json"), report} {
		write(path)
	}

	original := beforeRead
	beforeRead = func(path string) {
		switch path {
		case gone:
			if err := os.Remove(gone); err != nil {
				t.Fatal(err)
			}
		case staging:
			if err := os.Rename(staging, filepath.Join(src.Runs, "daily", "run-2", "observer")); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() { beforeRead = original })
	if err := os.MkdirAll(filepath.Join(src.Runs, "daily", "run-2"), 0o700); err != nil {
		t.Fatal(err)
	}

	var log bytes.Buffer
	result, err := Run(context.Background(), src, destination, &log)
	if err != nil {
		t.Fatalf("Run: %v\n%s", err, log.String())
	}
	for _, want := range []string{"vanished\t" + gone + "\n", "vanished\t" + staging + "\n"} {
		if !strings.Contains(log.String(), want) {
			t.Errorf("log = %q, want %q", log.String(), want)
		}
	}
	if result.Runs.Copied != 1 || result.Reports.Copied != 1 {
		t.Fatalf("result = %+v, want run-1's result and the report copied", result)
	}
	for _, path := range []string{kept, report} {
		rel, err := filepath.Rel(home, path)
		if err != nil {
			t.Fatal(err)
		}
		if data, err := os.ReadFile(filepath.Join(destination, rel)); err != nil || string(data) != path {
			t.Errorf("copy of %s = %q, %v", rel, data, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(destination, "runs", "daily", "run-1", ".result-123")); !os.IsNotExist(err) {
		t.Errorf("vanished file reached the copy: %v", err)
	}
}

// testSource is a state root under home with a ledger and nothing else.
func testSource(t *testing.T, home string) Source {
	t.Helper()
	src := Source{
		Ledger:  filepath.Join(home, "runs.db"),
		Runs:    filepath.Join(home, "runs"),
		Reports: filepath.Join(home, "reports"),
		Staging: filepath.Join(home, StagingDir),
	}
	db, err := sql.Open("sqlite", "file:"+src.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE runs (id TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return src
}

func writeSource(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A run's out/ is the model's to write, so after the walk lists an entry the
// model can swap it, or a directory above it, for a symlink out of the tree.
// The unsandboxed export reads through an os.Root on the tree: neither swap
// carries the outside file into the copy, and the export still completes.
func TestCopyTreeRefusesASwappedInSymlink(t *testing.T) {
	home := t.TempDir()
	destination := t.TempDir()
	src := testSource(t, home)
	outside := t.TempDir()
	const secret = "a protected host file"
	writeSource(t, filepath.Join(outside, "report.md"), secret)
	writeSource(t, filepath.Join(outside, "result.json"), secret)

	swappedFile := filepath.Join(src.Runs, "daily", "run-1", "out", "report.md")
	swappedParent := filepath.Join(src.Runs, "daily", "run-2")
	kept := filepath.Join(src.Runs, "daily", "run-3", "result.json")
	report := filepath.Join(src.Reports, "daily", "2026-11-26.md")
	for _, path := range []string{swappedFile, filepath.Join(swappedParent, "result.json"), kept, report} {
		writeSource(t, path, "run state")
	}

	original := beforeRead
	beforeRead = func(path string) {
		switch path {
		case swappedFile:
			if err := os.Remove(swappedFile); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(outside, "report.md"), swappedFile); err != nil {
				t.Fatal(err)
			}
		case filepath.Join(swappedParent, "result.json"):
			// The directory was listed and entered; the file in it is next.
			if err := os.Rename(swappedParent, filepath.Join(t.TempDir(), "run-2")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, swappedParent); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() { beforeRead = original })

	var log bytes.Buffer
	result, err := Run(context.Background(), src, destination, &log)
	if err != nil {
		t.Fatalf("Run: %v\n%s", err, log.String())
	}
	for _, want := range []string{"skipped\t" + swappedFile + "\t", "skipped\t" + filepath.Join(swappedParent, "result.json") + "\t"} {
		if !strings.Contains(log.String(), want) {
			t.Errorf("log = %q, want %q", log.String(), want)
		}
	}
	if result.Runs.Copied != 1 || result.Runs.Skipped != 2 || result.Reports.Copied != 1 {
		t.Fatalf("result = %+v, want run-3 and the report copied and both swaps skipped", result)
	}
	err = filepath.WalkDir(destination, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !d.Type().IsRegular() {
			t.Errorf("%s in the copy is a %s", path, d.Type())
			return nil
		}
		data, err := os.ReadFile(path)
		if strings.Contains(string(data), secret) {
			t.Errorf("%s in the copy carries the outside file", path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src.Staging)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging %s = %v, %v; want it under the state root and emptied", src.Staging, entries, err)
	}
}

// files is every regular file under root and its contents.
func files(t *testing.T, root string) map[string]string {
	t.Helper()
	got := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		data, err := os.ReadFile(path)
		got[path] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// The share is not this host's to vouch for: a directory symlink on it, there
// before the export or swapped in while it runs, must not carry a write out
// of the destination, and one found beforehand refuses the export before any
// of the previous copy is replaced.
func TestCopyRefusesADestinationSymlink(t *testing.T) {
	t.Run("there before the export", func(t *testing.T) {
		home := t.TempDir()
		destination := t.TempDir()
		src := testSource(t, home)
		run := filepath.Join(src.Runs, "daily", "run-1", "result.json")
		writeSource(t, run, "run state")
		if _, err := Run(context.Background(), src, destination, io.Discard); err != nil {
			t.Fatal(err)
		}
		ledger, err := os.ReadFile(filepath.Join(destination, ledgerFile))
		if err != nil {
			t.Fatal(err)
		}

		outside := t.TempDir()
		writeSource(t, filepath.Join(outside, "run-1", "result.json"), "outside original")
		before := files(t, outside)
		daily := filepath.Join(destination, runsDir, "daily")
		if err := os.Rename(daily, filepath.Join(t.TempDir(), "daily")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, daily); err != nil {
			t.Fatal(err)
		}
		writeSource(t, run, "run state, amended")
		db, err := sql.Open("sqlite", "file:"+src.Ledger)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO runs VALUES ('run-1')`); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		_, err = Run(context.Background(), src, destination, io.Discard)
		if err == nil || !strings.Contains(err.Error(), daily) {
			t.Fatalf("Run = %v, want a refusal naming %s", err, daily)
		}
		if got := files(t, outside); !maps.Equal(got, before) {
			t.Fatalf("outside the destination = %v, want %v", got, before)
		}
		if got, err := os.ReadFile(filepath.Join(destination, ledgerFile)); err != nil || !bytes.Equal(got, ledger) {
			t.Fatalf("previous ledger copy replaced (%v)", err)
		}
	})

	t.Run("swapped in during the export", func(t *testing.T) {
		home := t.TempDir()
		destination := t.TempDir()
		src := testSource(t, home)
		writeSource(t, filepath.Join(src.Runs, "daily", "run-1", "result.json"), "run state")
		run2 := filepath.Join(src.Runs, "daily", "run-2")
		writeSource(t, filepath.Join(run2, "result.json"), "run state")
		outside := t.TempDir()
		writeSource(t, filepath.Join(outside, "run-2", "result.json"), "outside original")
		before := files(t, outside)

		original := beforeRead
		beforeRead = func(path string) {
			if path != run2 {
				return
			}
			// run-1 is copied, so the copy's daily/ is there to swap.
			daily := filepath.Join(destination, runsDir, "daily")
			if err := os.Rename(daily, filepath.Join(t.TempDir(), "daily")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, daily); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() { beforeRead = original })

		if _, err := Run(context.Background(), src, destination, io.Discard); err == nil {
			t.Fatal("Run followed a symlink the share swapped in, want it refused")
		}
		if got := files(t, outside); !maps.Equal(got, before) {
			t.Fatalf("outside the destination = %v, want %v", got, before)
		}
	})
}
