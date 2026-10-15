package weftremote

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
)

// folderHandleTTL is how long a folder handle stays valid (§9.2).
const folderHandleTTL = 900 * time.Second

var folderHandleShape = regexp.MustCompile(`^hd-[0-9a-f]{16}$`)

// folderHandle is what a handle binds: a canonical directory and the device
// and inode it was when the handle was minted.
type folderHandle struct {
	path     string
	dev, ino uint64
	expires  time.Time
}

// folderHandles are the handles this host process minted. They are opaque
// and live only as long as the process: none outlives its 900 seconds anyway.
type folderHandles struct {
	mu  sync.Mutex
	m   map[string]folderHandle
	now func() time.Time
}

func newFolderHandles() *folderHandles {
	return &folderHandles{m: map[string]folderHandle{}, now: time.Now}
}

// mint binds a new handle to the directory canonical names, as info found it.
func (h *folderHandles) mint(canonical string, info os.FileInfo) (string, error) {
	dev, ino, err := fileIdentity(info)
	if err != nil {
		return "", err
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	handle := "hd-" + hex.EncodeToString(b[:])
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	for k, v := range h.m {
		if now.After(v.expires) {
			delete(h.m, k)
		}
	}
	h.m[handle] = folderHandle{path: canonical, dev: dev, ino: ino, expires: now.Add(folderHandleTTL)}
	return handle, nil
}

// resolve is the directory a handle names, validated again (§9.2): known,
// unexpired, its path still canonical — no component became a symlink — and
// still the directory it was. Anything else is handle_invalid.
func (h *folderHandles) resolve(handle string) (string, error) {
	invalid := func(format string, args ...any) error {
		return &ledger.WeftRefusal{Code: "handle_invalid", Message: fmt.Sprintf(format, args...)}
	}
	if !folderHandleShape.MatchString(handle) {
		return "", invalid("%q is not a folder handle: a path is never accepted as a working directory; browse GET /v2/folders for a handle", handle)
	}
	h.mu.Lock()
	b, ok := h.m[handle]
	now := h.now()
	h.mu.Unlock()
	switch {
	case !ok:
		return "", invalid("folder handle %s is not one this host holds; browse GET /v2/folders again", handle)
	case now.After(b.expires):
		return "", invalid("folder handle %s for %s expired at %s; browse GET /v2/folders again",
			handle, b.path, b.expires.UTC().Format("2006-01-02T15:04:05.000Z"))
	}
	canonical, info, err := canonicalDir(b.path)
	if err != nil {
		return "", invalid("folder handle %s names %s, which is no longer a directory here: %v", handle, b.path, err)
	}
	if canonical != b.path {
		return "", invalid("folder handle %s names %s, which now resolves to %s", handle, b.path, canonical)
	}
	dev, ino, err := fileIdentity(info)
	if err != nil {
		return "", err
	}
	if dev != b.dev || ino != b.ino {
		return "", invalid("folder handle %s names %s, which is no longer the directory it was when the handle was issued", handle, b.path)
	}
	return canonical, nil
}

func fileIdentity(info os.FileInfo) (dev, ino uint64, err error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("%s has no file identity", info.Name())
	}
	return uint64(st.Dev), st.Ino, nil
}

// canonicalDir is path canonicalized as a repository path is (§6.2:
// symlinks resolved, the volume's own spelling, leading /private dropped),
// required to be a directory.
func canonicalDir(path string) (string, os.FileInfo, error) {
	canonical, err := sandbox.CanonicalRepository(path)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", nil, err
	}
	if !info.IsDir() {
		return "", nil, fmt.Errorf("%s is not a directory", canonical)
	}
	return canonical, info, nil
}

// repositoryAt is the registered repository a canonical path lies in, and
// the project that names it (§9.3): the registered checkout that contains
// the path — the innermost, where checkouts nest — or the one whose .git the
// path's enclosing worktree names. A path in neither is unregistered: all
// three empty.
func repositoryAt(c *catalog, path string) (repositoryID, repository, project string) {
	var best *catalogProject
	for _, p := range c.projects {
		if p.Repository == "" || (p.State != projectAvailable && p.State != projectEmpty) {
			continue
		}
		if path == p.Repository || strings.HasPrefix(path, p.Repository+"/") {
			if best == nil || len(p.Repository) > len(best.Repository) {
				best = p
			}
		}
	}
	if best == nil {
		if named := worktreeRepository(path); named != "" {
			for _, p := range c.projects {
				if p.Repository == named && (p.State == projectAvailable || p.State == projectEmpty) {
					best = p
					break
				}
			}
		}
	}
	if best == nil {
		return "", "", ""
	}
	return best.RepositoryID, best.Repository, best.Name
}

// worktreeRepository is the canonical checkout whose .git the worktree
// enclosing path names in its .git file — gitdir: <checkout>/.git/worktrees/
// <name>, whose commondir leads back to <checkout>/.git — or "" when path is
// in no linked worktree.
func worktreeRepository(path string) string {
	for dir := path; ; dir = filepath.Dir(dir) {
		info, err := os.Lstat(filepath.Join(dir, ".git"))
		if err == nil {
			if !info.Mode().IsRegular() {
				return ""
			}
			doc, err := os.ReadFile(filepath.Join(dir, ".git"))
			if err != nil {
				return ""
			}
			gitdir, ok := strings.CutPrefix(strings.TrimSpace(string(doc)), "gitdir: ")
			if !ok {
				return ""
			}
			if !filepath.IsAbs(gitdir) {
				gitdir = filepath.Join(dir, gitdir)
			}
			common := filepath.Dir(filepath.Dir(gitdir))
			if rel, err := os.ReadFile(filepath.Join(gitdir, "commondir")); err == nil {
				common = strings.TrimSpace(string(rel))
				if !filepath.IsAbs(common) {
					common = filepath.Join(gitdir, common)
				}
			}
			common = filepath.Clean(common)
			if filepath.Base(common) != ".git" {
				return ""
			}
			canonical, err := sandbox.CanonicalRepository(filepath.Dir(common))
			if err != nil {
				return ""
			}
			return canonical
		}
		if dir == "/" {
			return ""
		}
	}
}

// folderEntry is one directory as the picker lists it, with a fresh handle.
func (s *Service) folderEntry(c *catalog, name, canonical string, info os.FileInfo) (map[string]any, error) {
	handle, err := s.folders.mint(canonical, info)
	if err != nil {
		return nil, err
	}
	repositoryID, _, project := repositoryAt(c, canonical)
	return map[string]any{
		"name": name, "handle": handle, "path": canonical,
		"repository_id": nullableText(repositoryID), "project": nullableText(project),
	}, nil
}

// handleFolders is the host folder picker (§9.2): home, a handle's
// directory, or a typed Studio path validated the same way, with each
// subdirectory's handle and actual path, its repository and project, and
// shortcuts to home and every registered checkout.
func (s *Service) handleFolders(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Has("handle") && q.Has("path") {
		s.v2Error(w, http.StatusBadRequest, "malformed", "name a folder by handle or by path, not both")
		return
	}
	home, err := os.UserHomeDir()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	var dir string
	switch {
	case q.Has("handle"):
		dir, err = s.folders.resolve(q.Get("handle"))
		var refusal *ledger.WeftRefusal
		if errors.As(err, &refusal) {
			s.v2Error(w, http.StatusConflict, refusal.Code, refusal.Message)
			return
		}
		if err != nil {
			s.v2Internal(w, err)
			return
		}
	case q.Has("path"):
		typed := q.Get("path")
		if !filepath.IsAbs(typed) || slices.Contains(strings.Split(typed, "/"), "..") {
			s.v2Error(w, http.StatusBadRequest, "malformed", fmt.Sprintf("%q is not an absolute Studio path without '..'", typed))
			return
		}
		canonical, _, err := canonicalDir(typed)
		if err != nil {
			s.v2Error(w, http.StatusNotFound, "folder_not_found", fmt.Sprintf("%s is not a directory on host %s: %v", typed, s.hostID, err))
			return
		}
		dir = canonical
	default:
		canonical, _, err := canonicalDir(home)
		if err != nil {
			s.v2Internal(w, err)
			return
		}
		dir = canonical
	}
	c, err := s.readCatalog()
	if err != nil {
		s.catalogError(w, err)
		return
	}
	info, err := os.Stat(dir)
	if err != nil {
		s.v2Error(w, http.StatusNotFound, "folder_not_found", fmt.Sprintf("%s is not a directory on host %s: %v", dir, s.hostID, err))
		return
	}
	listed, err := os.ReadDir(dir)
	if err != nil {
		s.v2Error(w, http.StatusNotFound, "folder_not_found", fmt.Sprintf("%s cannot be read on host %s: %v", dir, s.hostID, err))
		return
	}
	entries := []map[string]any{}
	for _, e := range listed {
		if !e.IsDir() && e.Type()&fs.ModeSymlink == 0 {
			continue
		}
		canonical, sub, err := canonicalDir(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		entry, err := s.folderEntry(c, e.Name(), canonical, sub)
		if err != nil {
			s.v2Internal(w, err)
			return
		}
		entries = append(entries, entry)
	}
	self, err := s.folderEntry(c, filepath.Base(dir), dir, info)
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	var parent any
	if dir != "/" {
		if canonical, up, err := canonicalDir(filepath.Dir(dir)); err == nil {
			entry, err := s.folderEntry(c, filepath.Base(canonical), canonical, up)
			if err != nil {
				s.v2Internal(w, err)
				return
			}
			parent = entry["handle"]
		}
	}
	shortcuts := []map[string]any{}
	if canonical, info, err := canonicalDir(home); err == nil {
		entry, err := s.folderEntry(c, "Home", canonical, info)
		if err != nil {
			s.v2Internal(w, err)
			return
		}
		shortcuts = append(shortcuts, entry)
	}
	seen := map[string]bool{}
	for _, p := range c.projects {
		if p.Repository == "" || seen[p.Repository] || (p.State != projectAvailable && p.State != projectEmpty) {
			continue
		}
		seen[p.Repository] = true
		canonical, info, err := canonicalDir(p.Repository)
		if err != nil {
			continue
		}
		entry, err := s.folderEntry(c, p.Name, canonical, info)
		if err != nil {
			s.v2Internal(w, err)
			return
		}
		shortcuts = append(shortcuts, entry)
	}
	s.stateMu.RLock()
	asOf, err := s.store.WeftAsOf()
	s.stateMu.RUnlock()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	s.read(w, asOf, map[string]any{"folder": map[string]any{
		"handle": self["handle"], "path": dir, "parent": parent,
		"repository_id": self["repository_id"], "project": self["project"],
		"entries": entries, "shortcuts": shortcuts,
		"expires_at": s.folders.now().Add(folderHandleTTL).UTC().Format("2006-01-02T15:04:05.000Z"),
	}})
}
