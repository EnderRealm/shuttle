package sandbox

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/text/unicode/norm"
)

// SegmentTmpDir is a hosted segment's TMPDIR under its run directory.
func SegmentTmpDir(runDir string) string { return filepath.Join(runDir, "tmp") }

// WorktreesDir is the directory under a root that WorktreePath derives every
// worktree beneath.
const WorktreesDir = "worktrees"

// WorktreePath mirrors WeftKit's hosted WorktreeProvider derivation.
func WorktreePath(root, repo, ticket string) (string, error) {
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("weft root and repository must be absolute")
	}
	canonical, err := CanonicalRepository(repo)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(canonical))
	repoDir := filepath.Base(canonical) + "-" + hex.EncodeToString(sum[:4])
	return filepath.Join(filepath.Clean(root), WorktreesDir, repoDir, strings.ReplaceAll(ticket, "/", "-")), nil
}

// RepositoryID is weft-service/v2 §4.1's repository_id for a canonical path:
// the worktree-root name, its basename restricted to [A-Za-z0-9-] and cut to
// 55 bytes so the whole id stays within the 64-byte protocol id limit. It is
// pure over the path given, so a binding whose checkout is missing still has
// the id its checkout would have.
func RepositoryID(canonical string) string {
	base := []byte(filepath.Base(canonical))
	for i, c := range base {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' {
			base[i] = '-'
		}
	}
	if len(base) > 55 {
		base = base[:55]
	}
	sum := sha256.Sum256([]byte(canonical))
	return string(base) + "-" + hex.EncodeToString(sum[:4])
}

// CanonicalRepository collapses symlink and case aliases to the one path used
// for the repository reservation and hosted worker argv. Swift's standardized
// path drops macOS's /private alias, so the host does the same after
// resolution.
func CanonicalRepository(repo string) (string, error) {
	if !filepath.IsAbs(repo) {
		return "", fmt.Errorf("repository %q is not absolute", repo)
	}
	canonical, err := filepath.EvalSymlinks(filepath.Clean(repo))
	if err != nil {
		return "", fmt.Errorf("resolve repository %q: %w", repo, err)
	}
	if canonical, err = filesystemSpelling(canonical); err != nil {
		return "", fmt.Errorf("resolve repository %q: %w", repo, err)
	}
	if strings.HasPrefix(canonical, "/private/") {
		canonical = strings.TrimPrefix(canonical, "/private")
	}
	return canonical, nil
}

// Covers reports whether tree equals or contains path, by spelling or, since
// an alias reaches the same directory, by file identity.
func Covers(tree, path string) (bool, error) {
	if tree == path || strings.HasPrefix(path, tree+"/") {
		return true, nil
	}
	return coversByIdentity(tree, path)
}

// coversByIdentity reports whether entry equals or contains path by file
// identity: the deepest existing ancestor of entry is matched with
// os.SameFile against each real ancestor of path, which sees through
// symlink, firmlink, case and Unicode-normalization aliases, and the
// not-yet-created components below each are compared literally.
func coversByIdentity(entry, path string) (bool, error) {
	anchor, rest, err := existingAncestor(entry)
	if err != nil {
		return false, err
	}
	anchorInfo, err := os.Stat(anchor)
	if err != nil {
		return false, err
	}
	dir, below, err := existingAncestor(path)
	if err != nil {
		return false, err
	}
	// The path's real ancestors, not its spelling's: a symlink's parent does
	// not contain the symlink's target.
	if dir, err = filepath.EvalSymlinks(dir); err != nil {
		return false, err
	}
	for {
		info, err := os.Stat(dir)
		if err != nil {
			return false, err
		}
		if os.SameFile(anchorInfo, info) && len(rest) <= len(below) && slices.EqualFunc(rest, below[:len(rest)], sameName) {
			return true, nil
		}
		if dir == "/" {
			return false, nil
		}
		below = append([]string{filepath.Base(dir)}, below...)
		dir = filepath.Dir(dir)
	}
}

// sameName compares components that do not exist yet as a case- and
// normalization-insensitive volume would; folding only errs toward refusing,
// which a caller judging containment may.
func sameName(a, b string) bool { return strings.EqualFold(norm.NFC.String(a), norm.NFC.String(b)) }

// existingAncestor is path's deepest existing ancestor and the components
// below it, which do not exist yet. A dangling symlink is refused: compared
// as a literal component it would pass, and once its target is created it
// may reach anywhere.
func existingAncestor(path string) (string, []string, error) {
	var rest []string
	for {
		_, err := os.Stat(path)
		if err == nil {
			return path, rest, nil
		}
		if !os.IsNotExist(err) || path == "/" {
			return "", nil, err
		}
		if _, err := os.Lstat(path); err == nil {
			return "", nil, fmt.Errorf("%s is a dangling symlink", path)
		}
		rest = append([]string{filepath.Base(path)}, rest...)
		path = filepath.Dir(path)
	}
}
