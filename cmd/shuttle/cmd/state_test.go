package cmd

import (
	"bytes"
	"database/sql"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/output"
)

// exportEnv is a state root with a ledger, and a config.json whose
// export.destination is an existing directory, returned.
func exportEnv(t *testing.T) string {
	t.Helper()
	shuttleEnv(t)
	destination := t.TempDir()
	setExportDestination(t, destination)
	path, err := config.LedgerPath()
	if err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return destination
}

func setExportDestination(t *testing.T, destination string) {
	t.Helper()
	writeConfig(t, `{"export": {"destination": `+strconv.Quote(destination)+`}}`)
}

// runExport runs `shuttle state export` and returns what it printed.
func runExport(t *testing.T) (string, error) {
	t.Helper()
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetArgs([]string{"state", "export"})
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	})
	err := rootCmd.Execute()
	return out.String(), err
}

func mustExport(t *testing.T) string {
	t.Helper()
	out, err := runExport(t)
	if err != nil {
		t.Fatalf("state export: %v\n%s", err, out)
	}
	return out
}

func writeTestFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// The daemon holds the ledger open and writes it while the export runs. The
// copy has to be the ledger as of one commit: each of the writer's
// transactions adds a row and counts it, so a copy taken part way through one
// would have the two disagree.
func TestStateExportLedgerIsConsistent(t *testing.T) {
	destination := exportEnv(t)
	path, err := config.LedgerPath()
	if err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	writer, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	writer.SetMaxOpenConns(1)
	if _, err := writer.Exec(`CREATE TABLE export_probe (n INTEGER PRIMARY KEY, pad BLOB NOT NULL);
		CREATE TABLE export_counter (total INTEGER NOT NULL);
		INSERT INTO export_counter VALUES (0)`); err != nil {
		t.Fatal(err)
	}

	var committed atomic.Int64
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			tx, err := writer.Begin()
			if err != nil {
				done <- err
				return
			}
			if _, err := tx.Exec(`INSERT INTO export_probe (pad) VALUES (randomblob(4096))`); err != nil {
				tx.Rollback()
				done <- err
				return
			}
			if _, err := tx.Exec(`UPDATE export_counter SET total = total + 1`); err != nil {
				tx.Rollback()
				done <- err
				return
			}
			if err := tx.Commit(); err != nil {
				done <- err
				return
			}
			committed.Add(1)
		}
	}()
	var stopOnce sync.Once
	stopWriter := func() {
		stopOnce.Do(func() {
			close(stop)
			if err := <-done; err != nil {
				t.Errorf("writer: %v", err)
			}
		})
	}
	defer stopWriter()
	for committed.Load() < 500 {
		select {
		case err := <-done:
			t.Fatalf("writer stopped: %v", err)
		default:
		}
	}

	before := committed.Load()
	out := mustExport(t)
	after := committed.Load()
	stopWriter()
	if after == before {
		t.Fatalf("the writer committed nothing while the export ran (%d), so the copy was not taken under writes", before)
	}

	copied := filepath.Join(destination, "runs.db")
	db, err := sql.Open("sqlite", "file:"+copied+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var integrity, mode string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity_check = %q, %v", integrity, err)
	}
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "delete" {
		t.Fatalf("journal_mode = %q, %v; want the copy one self-contained file", mode, err)
	}
	var rows, total int64
	if err := db.QueryRow(`SELECT count(*) FROM export_probe`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT total FROM export_counter`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if rows != total {
		t.Fatalf("copy has %d rows and counts %d: not one commit's ledger", rows, total)
	}
	if rows < before || rows > after {
		t.Fatalf("copy has %d rows; the writer had committed %d when the export began and %d when it ended", rows, before, after)
	}
	var version string
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil || version == "" {
		t.Fatalf("copy schema_version = %q, %v", version, err)
	}
	for _, sidecar := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(copied + sidecar); !os.IsNotExist(err) {
			t.Errorf("%s%s beside the copy: %v", copied, sidecar, err)
		}
	}
	if !strings.Contains(out, "ledger\t"+copied) {
		t.Errorf("output = %q, want the copy named", out)
	}
}

// Each run's retained files and the reports repository, its .git included,
// are copied beside the ledger; a second export rewrites only what changed.
// A symlink is not followed out of the tree.
func TestStateExportCopiesRunsAndReports(t *testing.T) {
	destination := exportEnv(t)
	runs, err := config.RunsDir()
	if err != nil {
		t.Fatal(err)
	}
	reports, err := config.ReportsDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := output.InitReports(reports); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(reports, "daily", "2026-11-26.md"), "# report\n")
	files := map[string]string{
		"daily/run-1/result.json":   `{"status":"completed"}`,
		"daily/run-1/out/report.md": "# one\n",
		"daily/run-2/result.json":   `{"status":"failed"}`,
	}
	for rel, body := range files {
		writeTestFile(t, filepath.Join(runs, rel), body)
	}
	// A share keeps a coarser mtime than APFS: run-2's copy is given one that
	// differs below the second, and must still count as unchanged.
	second := time.Now().Add(-time.Hour).Truncate(time.Second)
	run2 := filepath.Join(runs, "daily", "run-2", "result.json")
	if err := os.Chtimes(run2, second.Add(900*time.Millisecond), second.Add(900*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	writeTestFile(t, outside, "not run state")
	if err := os.Symlink(outside, filepath.Join(runs, "daily", "run-2", "escape")); err != nil {
		t.Fatal(err)
	}
	reportFiles := regularFiles(t, reports)

	out := mustExport(t)
	if want := "runs\tcopied 3\tunchanged 0\tskipped 1\n"; !strings.Contains(out, want) {
		t.Fatalf("output = %q, want %q", out, want)
	}
	if want := "reports\tcopied " + strconv.Itoa(len(reportFiles)) + "\tunchanged 0\tskipped 0\n"; !strings.Contains(out, want) {
		t.Fatalf("output = %q, want %q", out, want)
	}
	for rel, body := range files {
		assertFile(t, filepath.Join(destination, "runs", rel), body)
	}
	if got := regularFiles(t, filepath.Join(destination, "reports")); !slices.Equal(got, reportFiles) {
		t.Fatalf("reports copy = %v, want %v", got, reportFiles)
	}
	for _, rel := range reportFiles {
		want, err := os.ReadFile(filepath.Join(reports, rel))
		if err != nil {
			t.Fatal(err)
		}
		assertFile(t, filepath.Join(destination, "reports", rel), string(want))
	}
	if !slices.Contains(reportFiles, filepath.Join(".git", "HEAD")) {
		t.Fatalf("reports files = %v, want the repository's .git", reportFiles)
	}
	if _, err := os.Lstat(filepath.Join(destination, "runs", "daily", "run-2", "escape")); !os.IsNotExist(err) {
		t.Fatalf("symlink reached the copy: %v", err)
	}
	info, err := os.Stat(filepath.Join(destination, "runs", "daily", "run-1", "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("copied run file mode = %04o, want it private", perm)
	}

	run2Copy := filepath.Join(destination, "runs", "daily", "run-2", "result.json")
	truncated := second.Add(400 * time.Millisecond)
	if err := os.Chtimes(run2Copy, truncated, truncated); err != nil {
		t.Fatal(err)
	}
	files["daily/run-1/result.json"] = `{"status":"completed","amended":true}`
	files["daily/run-3/result.json"] = `{"status":"completed"}`
	for _, rel := range []string{"daily/run-1/result.json", "daily/run-3/result.json"} {
		writeTestFile(t, filepath.Join(runs, rel), files[rel])
	}
	out = mustExport(t)
	if want := "runs\tcopied 2\tunchanged 2\tskipped 1\n"; !strings.Contains(out, want) {
		t.Fatalf("second export output = %q, want %q", out, want)
	}
	if want := "reports\tcopied 0\tunchanged " + strconv.Itoa(len(reportFiles)) + "\tskipped 0\n"; !strings.Contains(out, want) {
		t.Fatalf("second export output = %q, want %q", out, want)
	}
	for rel, body := range files {
		assertFile(t, filepath.Join(destination, "runs", rel), body)
	}
	if info, err := os.Stat(run2Copy); err != nil || !info.ModTime().Equal(truncated) {
		t.Fatalf("run-2 copy = %v, %v; want it left alone at %s", info, err, truncated)
	}
}

// An unmounted share is a missing directory; creating it would write the copy
// to the local disk. A destination any part of which cannot be written, its
// root or a directory deep in the previous copy, fails before any of the
// previous copy is replaced.
func TestStateExportRefusesMissingDestination(t *testing.T) {
	destination := exportEnv(t)
	runs, err := config.RunsDir()
	if err != nil {
		t.Fatal(err)
	}
	reports, err := config.ReportsDir()
	if err != nil {
		t.Fatal(err)
	}
	ledgerPath, err := config.LedgerPath()
	if err != nil {
		t.Fatal(err)
	}
	run := filepath.Join(runs, "daily", "run-1", "result.json")
	writeTestFile(t, run, `{"status":"completed"}`)
	report := filepath.Join(reports, "daily", "2026-11-26.md")
	writeTestFile(t, report, "# report\n")

	missing := filepath.Join(destination, "unmounted")
	setExportDestination(t, missing)
	out, err := runExport(t)
	if err == nil || !strings.Contains(err.Error(), missing) {
		t.Fatalf("export to a missing destination = %v, want a failure naming it\n%s", err, out)
	}
	if _, err := os.Lstat(missing); !os.IsNotExist(err) {
		t.Fatalf("missing destination after the export: %v, want it still absent", err)
	}

	setExportDestination(t, destination)
	mustExport(t)
	previous := snapshot(t, destination)
	// Every part of the source changes, so an export that wrote anything at
	// all would show in the copy.
	writeTestFile(t, run, `{"status":"completed","amended":true}`)
	writeTestFile(t, report, "# report, amended\n")
	writer, err := sql.Open("sqlite", "file:"+ledgerPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`CREATE TABLE export_probe (n INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	unwritable := func(t *testing.T, dir string) {
		t.Helper()
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(dir, 0o700) })
		out, err := runExport(t)
		if err == nil || !strings.Contains(err.Error(), dir) {
			t.Fatalf("export with %s unwritable = %v, want a failure naming it\n%s", dir, err, out)
		}
		if got := snapshot(t, destination); !maps.Equal(got, previous) {
			t.Fatalf("previous copy changed: %v, want %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(previous)))
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	unwritable(t, destination)
	unwritable(t, filepath.Join(destination, "reports", "daily"))
}

// regularFiles lists the regular files under root, relative and sorted.
func regularFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		files = append(files, rel)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(files)
	return files
}

// snapshot is every entry under root and, for a file, its contents.
func snapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	entries := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			data, err := os.ReadFile(path)
			entries[path] = string(data)
			return err
		}
		entries[path] = d.Type().String()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("%s = %q, want %q", path, data, want)
	}
}
