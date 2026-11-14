package weftremote

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func releaseFixture(t *testing.T) (*serviceFixture, string) {
	t.Helper()
	f := newServiceFixture(t)
	dir := filepath.Join(f.root, "release")
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"manifest.json": `{"version":"1"}`, "weft.zip": "zipbytes",
		filepath.Join(f.root, "secret.txt"): "secret",
	} {
		p := name
		if !filepath.IsAbs(p) {
			p = filepath.Join(dir, name)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(f.root, "secret.txt"), filepath.Join(dir, "out")); err != nil {
		t.Fatal(err)
	}
	f.service.cfg.Weft.ReleaseRoot = dir
	return f, dir
}

func releaseGet(f *serviceFixture, path, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.service.ServeHTTP(rec, req)
	return rec
}

func TestServeWeftReleasesServesManifest(t *testing.T) {
	f, _ := releaseFixture(t)
	for path, want := range map[string]string{
		"/releases/weft/manifest.json": `{"version":"1"}`, "/releases/weft/weft.zip": "zipbytes",
	} {
		rec := releaseGet(f, path, "secret-token")
		if rec.Code != http.StatusOK || rec.Body.String() != want {
			t.Fatalf("%s = %d %q, want 200 %q", path, rec.Code, rec.Body.String(), want)
		}
	}
}

func TestServeWeftReleasesRequiresToken(t *testing.T) {
	f, _ := releaseFixture(t)
	for _, token := range []string{"", "wrong"} {
		rec := releaseGet(f, "/releases/weft/manifest.json", token)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("token %q = %d, want 401", token, rec.Code)
		}
	}
}

func TestServeWeftReleasesRefusesEscape(t *testing.T) {
	f, _ := releaseFixture(t)
	for _, path := range []string{
		"/releases/weft/../secret.txt", "/releases/weft/sub/../../secret.txt",
		"/releases/weft/%2e%2e/secret.txt", "/releases/weft//etc/passwd",
		"/releases/weft/out", "/releases/weft/sub", "/releases/weft/sub/", "/releases/weft/",
	} {
		rec := releaseGet(f, path, "secret-token")
		if rec.Code == http.StatusOK || rec.Body.String() == "secret" {
			t.Fatalf("%s = %d %q, want refused", path, rec.Code, rec.Body.String())
		}
	}
}
