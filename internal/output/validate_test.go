package output

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeOut lays out an out/ directory holding the given files.
func writeOut(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "out")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// mustFail asserts the directory is refused and that the refusal classifies as
// the §7.6 output class, which every one of these cases must produce.
func mustFail(t *testing.T, dir string) error {
	t.Helper()
	_, err := Validate(dir)
	if err == nil {
		t.Fatal("got no error, want a validation failure")
	}
	var oerr *Error
	if !errors.As(err, &oerr) {
		t.Fatalf("got %T, want it to classify as %s", err, Class)
	}
	return err
}

// validReport clears the 200-byte floor a non-silent run carries.
const validReport = "# Daily\n\n" + `Yesterday the estate landed eleven commits across three checkouts, closed four
tickets and opened one incident. The open thread from the previous report, the
flaky sandbox profile, is still open.` + "\n"

func TestValidateAccepts(t *testing.T) {
	dir := writeOut(t, map[string]string{
		ReportFile:   validReport,
		ManifestFile: `{"email":{"subject":"shuttle daily 2026-09-01"},"silent":false}`,
	})
	out, err := Validate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if out.Silent {
		t.Error("silent: got true, want false")
	}
	if out.Subject != "shuttle daily 2026-09-01" {
		t.Errorf("subject: got %q", out.Subject)
	}
	if string(out.Report) != validReport {
		t.Error("report: not the file's bytes")
	}
}

// A silent run has nothing worth sending, so neither the subject nor the
// 200-byte floor applies (§9.1).
func TestValidateAcceptsSilentShortReport(t *testing.T) {
	dir := writeOut(t, map[string]string{
		ReportFile:   "# Daily\n\nNothing happened.\n",
		ManifestFile: `{"silent":true}`,
	})
	out, err := Validate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Silent {
		t.Error("silent: got false, want true")
	}
	if out.Subject != "" {
		t.Errorf("subject: got %q, want empty", out.Subject)
	}
}

func TestValidateFileSet(t *testing.T) {
	t.Run("extra file", func(t *testing.T) {
		dir := writeOut(t, map[string]string{
			ReportFile:   validReport,
			ManifestFile: `{"silent":true}`,
			"notes.txt":  "scratch",
		})
		if err := mustFail(t, dir); !strings.Contains(err.Error(), "notes.txt") {
			t.Errorf("error must name the extra file: %v", err)
		}
	})

	t.Run("extra directory", func(t *testing.T) {
		dir := writeOut(t, map[string]string{
			ReportFile:   validReport,
			ManifestFile: `{"silent":true}`,
		})
		if err := os.Mkdir(filepath.Join(dir, "drafts"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := mustFail(t, dir); !strings.Contains(err.Error(), "drafts") {
			t.Errorf("error must name the extra entry: %v", err)
		}
	})

	t.Run("missing report", func(t *testing.T) {
		dir := writeOut(t, map[string]string{ManifestFile: `{"silent":true}`})
		if err := mustFail(t, dir); !strings.Contains(err.Error(), ReportFile) {
			t.Errorf("error must name the missing file: %v", err)
		}
	})

	t.Run("missing manifest", func(t *testing.T) {
		dir := writeOut(t, map[string]string{ReportFile: validReport})
		if err := mustFail(t, dir); !strings.Contains(err.Error(), ManifestFile) {
			t.Errorf("error must name the missing file: %v", err)
		}
	})

	// A symlink named report.md would otherwise pull content from outside the
	// run directory into the report.
	t.Run("symlinked report", func(t *testing.T) {
		dir := writeOut(t, map[string]string{ManifestFile: `{"silent":true}`})
		target := filepath.Join(t.TempDir(), "secret.md")
		if err := os.WriteFile(target, []byte(validReport), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, ReportFile)); err != nil {
			t.Fatal(err)
		}
		if err := mustFail(t, dir); !strings.Contains(err.Error(), "not a regular file") {
			t.Errorf("got %v, want a regular-file refusal", err)
		}
	})
}

func TestValidateSchema(t *testing.T) {
	tests := []struct {
		name     string
		report   string
		manifest string
		want     string
	}{
		{
			name:     "silent absent",
			report:   validReport,
			manifest: `{"email":{"subject":"daily"}}`,
			want:     "silent: required",
		},
		{
			name:     "subject missing when not silent",
			report:   validReport,
			manifest: `{"silent":false}`,
			want:     "email.subject: required",
		},
		{
			name:     "subject over 120 characters",
			report:   validReport,
			manifest: `{"email":{"subject":"` + strings.Repeat("a", 121) + `"},"silent":false}`,
			want:     "121 characters, at most 120",
		},
		{
			name:     "subject with a newline",
			report:   validReport,
			manifest: `{"email":{"subject":"daily\nBcc: someone@example.com"},"silent":false}`,
			want:     "contains a newline",
		},
		{
			name:     "subject with an unprintable character",
			report:   validReport,
			manifest: `{"email":{"subject":"daily\u0007"},"silent":false}`,
			want:     "unprintable character",
		},
		{
			name:     "report over 200 KB",
			report:   strings.Repeat("x", (200<<10)+1),
			manifest: `{"email":{"subject":"daily"},"silent":false}`,
			want:     "at most 204800",
		},
		{
			// Refused on its size before it is decoded, so the oversize is
			// what surfaces rather than the 120-rune subject rule.
			name:     "manifest over 4 KiB",
			report:   validReport,
			manifest: `{"email":{"subject":"` + strings.Repeat("a", 4<<10) + `"},"silent":false}`,
			want:     "at most 4096",
		},
		{
			name:     "report under 200 bytes when not silent",
			report:   "# Daily\n\nToo short.\n",
			manifest: `{"email":{"subject":"daily"},"silent":false}`,
			want:     "at least 200",
		},
		{
			name:     "unknown manifest key",
			report:   validReport,
			manifest: `{"silent":true,"webhook":"https://evil.example"}`,
			want:     "webhook",
		},
		{
			name:     "trailing content after the manifest",
			report:   validReport,
			manifest: `{"silent":true}{"silent":false}`,
			want:     "trailing content",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeOut(t, map[string]string{ReportFile: tc.report, ManifestFile: tc.manifest})
			if err := mustFail(t, dir); !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateRejectsInvalidUTF8(t *testing.T) {
	dir := writeOut(t, map[string]string{
		ReportFile:   strings.Repeat("a", 200) + "\xff",
		ManifestFile: `{"email":{"subject":"daily"},"silent":false}`,
	})
	if err := mustFail(t, dir); !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("got %v, want a UTF-8 refusal", err)
	}
}
