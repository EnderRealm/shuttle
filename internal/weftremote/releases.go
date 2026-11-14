package weftremote

import (
	"net/http"
	"os"
	"strings"
)

const releasesPrefix = "/releases/weft/"

// serveReleases serves one regular file of the weft release root
// (weft.release_root), read-only. The root is opened with os.OpenRoot, so a
// name with "..", or a symlink leading out of the root, cannot reach beyond
// it. A directory is never listed.
func (s *Service) serveReleases(w http.ResponseWriter, r *http.Request, path string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		s.v2Error(w, http.StatusMethodNotAllowed, "method_not_allowed", "releases are read-only")
		return
	}
	name := strings.TrimPrefix(path, releasesPrefix)
	if s.cfg.Weft.ReleaseRoot == "" || name == path || name == "" || strings.HasPrefix(name, "/") {
		s.v2Error(w, http.StatusNotFound, "not_found", "resource not found")
		return
	}
	root, err := os.OpenRoot(s.cfg.Weft.ReleaseRoot)
	if err != nil {
		s.v2Error(w, http.StatusNotFound, "not_found", "resource not found")
		return
	}
	defer root.Close()
	f, err := root.Open(name)
	if err != nil {
		s.v2Error(w, http.StatusNotFound, "not_found", "resource not found")
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		s.v2Error(w, http.StatusNotFound, "not_found", "resource not found")
		return
	}
	http.ServeContent(w, r, info.Name(), info.ModTime(), f)
}
