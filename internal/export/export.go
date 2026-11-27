package export

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	sqlite "modernc.org/sqlite"
)

// StagingDir is the directory under the state root the ledger's backup is
// staged in before it goes to the share. It is under the state root because
// the backup is the whole ledger, and every agent sandbox denies it by name,
// whatever the state root is.
const StagingDir = "export-staging"

// The copy's layout under the destination, the state root's own.
const (
	ledgerFile = "runs.db"
	runsDir    = "runs"
	reportsDir = "reports"
)

// Source is what an export copies, the live ledger and the two trees, and
// Staging, the state root's StagingDir.
type Source struct {
	Ledger  string
	Runs    string
	Reports string
	Staging string
}

// Counts is what the copy of one tree did. A file the same size and
// modification second as its copy is unchanged and not rewritten.
type Counts struct {
	Copied    int
	Unchanged int
	Skipped   int
}

func (c Counts) String() string {
	return fmt.Sprintf("copied %d\tunchanged %d\tskipped %d", c.Copied, c.Unchanged, c.Skipped)
}

// Result is what one export wrote.
type Result struct {
	Ledger  string
	Runs    Counts
	Reports Counts
}

// Run copies src into destination, which must already exist: it is a mounted
// share, and creating it would put the copy on the local disk under an
// unmounted mount point. Every write goes through an os.Root on destination,
// so a symlink on the share cannot carry a write out of it. Every directory
// of the previous copy must be writable, and none of it a symlink, before
// anything is replaced. The ledger is copied first, so every
// run its copy records has its directory copied after it. log receives each
// entry skipped.
func Run(ctx context.Context, src Source, destination string, log io.Writer) (Result, error) {
	if err := checkDestination(destination); err != nil {
		return Result{}, err
	}
	dest, err := os.OpenRoot(destination)
	if err != nil {
		return Result{}, fmt.Errorf("export: destination: %w", err)
	}
	defer dest.Close()
	if err := checkWritable(dest); err != nil {
		return Result{}, err
	}
	if err := copyLedger(ctx, src.Ledger, src.Staging, dest); err != nil {
		return Result{}, err
	}
	result := Result{Ledger: filepath.Join(destination, ledgerFile)}
	if result.Runs, err = copyTree(src.Runs, dest, runsDir, log); err != nil {
		return result, err
	}
	if result.Reports, err = copyTree(src.Reports, dest, reportsDir, log); err != nil {
		return result, err
	}
	return result, nil
}

func checkDestination(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("export: destination %q is not an absolute path", dir)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("export: destination: %w; it is never created here, so mount the share first", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("export: destination %s is not a directory", dir)
	}
	return nil
}

// checkWritable refuses a destination the export could only partly update:
// its root, or any directory the previous copy has under runs/ or reports/,
// that this user cannot write and search. It refuses a symlink anywhere in the
// previous copy too: the export writes none, so one there came from the
// share, and following it would write outside the destination. It runs before
// anything is replaced, so such a refusal leaves the previous copy as it was.
func checkWritable(dest *os.Root) error {
	if err := writable(dest.Name()); err != nil {
		return err
	}
	if err := notSymlink(dest, ledgerFile); err != nil {
		return err
	}
	for _, tree := range []string{runsDir, reportsDir} {
		if err := checkTree(dest, tree); err != nil {
			return err
		}
	}
	return nil
}

// checkTree walks rel under dest top down, never following a symlink.
func checkTree(dest *os.Root, rel string) error {
	info, err := dest.Lstat(rel)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("export: the previous copy: %w", err)
	}
	if err := notSymlink(dest, rel); err != nil || !info.IsDir() {
		return err
	}
	path := filepath.Join(dest.Name(), rel)
	if err := writable(path); err != nil {
		return err
	}
	dir, err := dest.Open(rel)
	if err != nil {
		return fmt.Errorf("export: the previous copy: %w", err)
	}
	entries, err := dir.ReadDir(-1)
	dir.Close()
	if err != nil {
		return fmt.Errorf("export: the previous copy: %w", err)
	}
	for _, entry := range entries {
		if err := checkTree(dest, filepath.Join(rel, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func notSymlink(dest *os.Root, rel string) error {
	info, err := dest.Lstat(rel)
	if err == nil && info.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("export: %s is a symlink, which no export writes; nothing was replaced", filepath.Join(dest.Name(), rel))
	}
	return nil
}

func writable(dir string) error {
	if err := unix.Access(dir, unix.W_OK|unix.X_OK); err != nil {
		return fmt.Errorf("export: %s is not writable: %w; nothing was replaced", dir, err)
	}
	return nil
}

// copyLedger backs the ledger up into a private directory under staging, so
// SQLite never writes to the share, then copies that file over the previous
// copy.
func copyLedger(ctx context.Context, source, staging string, dest *os.Root) error {
	if _, err := os.Stat(source); err != nil {
		return fmt.Errorf("export: ledger: %w", err)
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return fmt.Errorf("export: %w", err)
	}
	dir, err := os.MkdirTemp(staging, "ledger-")
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}
	defer os.RemoveAll(dir)
	staged := filepath.Join(dir, ledgerFile)
	if err := backup(ctx, source, staged); err != nil {
		return fmt.Errorf("export: back up %s: %w", source, err)
	}
	in, err := os.Open(staged)
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}
	return writeFile(in, dest, ledgerFile, info)
}

// backupConn is the online backup modernc.org/sqlite's driver connection
// offers.
type backupConn interface {
	NewBackup(dstURI string) (*sqlite.Backup, error)
}

// backup copies the ledger at source to target through SQLite's online
// backup. A single step of every page runs in one read transaction, so the
// copy is the ledger as of one commit whatever another connection writes
// meanwhile. The source is opened read-only, never through ledger.Open: an
// export must not migrate, checkpoint or otherwise write the live ledger.
func backup(ctx context.Context, source, target string) error {
	db, err := sql.Open("sqlite", "file:"+source+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = conn.Raw(func(driverConn any) error {
		bc, ok := driverConn.(backupConn)
		if !ok {
			return errors.New("the sqlite driver offers no online backup")
		}
		b, err := bc.NewBackup(target)
		if err != nil {
			return err
		}
		if _, err := b.Step(-1); err != nil {
			b.Finish()
			return err
		}
		return b.Finish()
	})
	if err != nil {
		return err
	}
	return rollbackJournal(target)
}

// rollbackJournal takes the copy out of the ledger's WAL mode, making it one
// self-contained file: opened where it lies it grows no -wal or -shm beside
// it, and a restore copies it back as it is.
func rollbackJournal(path string) error {
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		return err
	}
	defer db.Close()
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode=DELETE`).Scan(&mode); err != nil {
		return err
	}
	if mode != "delete" {
		return fmt.Errorf("journal mode is %s, want delete", mode)
	}
	return db.Close()
}

// copyTree copies the regular files under src to dst under dest. Directories
// are made private. Nothing else is copied: a symlink is not followed, so nothing
// outside the tree reaches the copy, and a socket or pipe has no contents to
// keep. It never deletes: a file pruned from src stays in the copy. An absent
// src is a state root with nothing in it yet. An entry listed and then gone
// before it is read, as a live run renames its .result-* and evidence staging
// away, is logged as vanished rather than failing the night's export.
//
// Every entry is looked at and opened through an os.Root on src, which
// refuses a path that leaves the tree. A run's out/ is the model's to write,
// so a file or a directory above it can be swapped for a symlink after the
// walk listed it; such an entry is skipped, and nothing outside the tree is
// read by this unsandboxed process.
func copyTree(src string, dest *os.Root, dst string, log io.Writer) (Counts, error) {
	var c Counts
	if _, err := os.Lstat(src); errors.Is(err, fs.ErrNotExist) {
		return c, nil
	}
	root, err := os.OpenRoot(src)
	if err != nil {
		return c, fmt.Errorf("export: %w", err)
	}
	defer root.Close()
	err = filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err == nil {
			beforeRead(path)
		}
		if vanished(path, err) {
			// For a directory whose listing failed, nil skips its contents.
			fmt.Fprintf(log, "vanished\t%s\n", path)
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		// SkipDir only for a directory: for a file it would skip the rest of
		// the file's directory.
		skip := func(reason string) error {
			c.Skipped++
			fmt.Fprintf(log, "skipped\t%s\t%s\n", path, reason)
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		target := filepath.Join(dst, rel)
		info, err := root.Lstat(rel)
		switch {
		case vanished(path, err):
			fmt.Fprintf(log, "vanished\t%s\n", path)
			return nil
		case err != nil && swapped(src, rel):
			return skip("replaced by a symlink since it was listed")
		case err != nil:
			return err
		case info.IsDir():
			if err := dest.MkdirAll(target, 0o700); err != nil {
				return fmt.Errorf("make %s: %w", filepath.Join(dest.Name(), target), err)
			}
			return nil
		case !info.Mode().IsRegular():
			return skip(info.Mode().Type().String())
		case unchanged(dest, target, info):
			c.Unchanged++
			return nil
		}
		in, err := openListed(root, rel, info)
		switch {
		case vanished(path, err):
			fmt.Fprintf(log, "vanished\t%s\n", path)
			return nil
		case errors.Is(err, errReplaced), err != nil && swapped(src, rel):
			return skip("replaced since it was listed")
		case err != nil:
			return err
		}
		defer in.Close()
		if err := writeFile(in, dest, target, info); err != nil {
			return err
		}
		c.Copied++
		return nil
	})
	if err != nil {
		return c, fmt.Errorf("export: copy %s: %w", src, err)
	}
	return c, nil
}

// errReplaced is a listed regular file that is something else by the time it
// is opened.
var errReplaced = errors.New("replaced since it was listed")

// openListed opens rel under root only if it is still the regular file listed
// as listed. O_NONBLOCK keeps a pipe swapped in from blocking the open.
func openListed(root *os.Root, rel string, listed fs.FileInfo) (*os.File, error) {
	f, err := root.OpenFile(rel, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || !os.SameFile(info, listed) {
		f.Close()
		return nil, errReplaced
	}
	return f, nil
}

// swapped reports whether rel, or a directory on the way to it under src, is
// now a symlink: what os.Root refuses to follow out of the tree.
func swapped(src, rel string) bool {
	path := src
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		path = filepath.Join(path, part)
		info, err := os.Lstat(path)
		if err != nil {
			return false
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return true
		}
	}
	return false
}

// beforeRead runs as the walk reaches each entry, before it is read. A test
// removes the entry there, which is the window a live run renames in.
var beforeRead = func(string) {}

// vanished reports whether err is src having gone since it was listed. The
// source is looked at again, so a missing directory on the destination side
// is never mistaken for it.
func vanished(src string, err error) bool {
	if !errors.Is(err, fs.ErrNotExist) {
		return false
	}
	_, statErr := os.Lstat(src)
	return errors.Is(statErr, fs.ErrNotExist)
}

// unchanged compares modification times to the second, the granularity both
// APFS and the share keep: SMB carries 100ns timestamps, so the nanosecond
// mtime writeFile sets reads back truncated, and an exact comparison would
// recopy every file every night. A rewrite within the same second that keeps
// the size is missed until the file next changes, the trade rsync's
// --modify-window makes.
func unchanged(dest *os.Root, target string, info fs.FileInfo) bool {
	copied, err := dest.Lstat(target)
	return err == nil && copied.Mode().IsRegular() && copied.Size() == info.Size() &&
		copied.ModTime().Truncate(time.Second).Equal(info.ModTime().Truncate(time.Second))
}

// writeFile replaces dst, relative to dest, with a copy of in, the file info
// describes, through a temporary file beside it, so a copy that fails part way
// leaves the previous dst whole. The copy takes the source's modification
// time, which the next export compares, and its permissions with group and
// other cleared: the run state is private. Both are set on the open file, so
// a name swapped under it cannot redirect them.
func writeFile(in io.Reader, dest *os.Root, dst string, info fs.FileInfo) error {
	fail := func(err error) error { return fmt.Errorf("export: write %s: %w", filepath.Join(dest.Name(), dst), err) }
	tmp, name, err := createTemp(dest, dst)
	if err != nil {
		return fail(err)
	}
	defer dest.Remove(name)
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fail(err)
	}
	if err := tmp.Chmod(info.Mode().Perm() &^ 0o077); err != nil {
		tmp.Close()
		return fail(err)
	}
	mtime := unix.NsecToTimeval(info.ModTime().UnixNano())
	if err := unix.Futimes(int(tmp.Fd()), []unix.Timeval{mtime, mtime}); err != nil {
		tmp.Close()
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		return fail(err)
	}
	if err := dest.Rename(name, dst); err != nil {
		return fail(err)
	}
	return nil
}

// createTemp is os.CreateTemp under dest: a new file beside dst, named after
// it, that nothing else has open.
func createTemp(dest *os.Root, dst string) (*os.File, string, error) {
	for range 100 {
		name := filepath.Join(filepath.Dir(dst), fmt.Sprintf(".%s-%d", filepath.Base(dst), rand.Uint64()))
		f, err := dest.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return f, name, err
	}
	return nil, "", fmt.Errorf("no unused temporary name beside %s", dst)
}
