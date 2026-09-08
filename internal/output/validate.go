package output

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"shuttle/internal/ledger"
)

const (
	// Class is the §7.6 failure class an out/ failure is recorded under.
	Class = ledger.ClassOutput

	// ReportFile and ManifestFile are the only two entries out/ may hold
	// (§9.1); the run contract in §7.3 names them to the model.
	ReportFile   = "report.md"
	ManifestFile = "outputs.json"

	maxReportBytes  = 200 << 10
	minReportBytes  = 200
	maxSubjectRunes = 120

	// maxManifestBytes caps outputs.json. The schema is
	// {"email":{"subject":"…"},"silent":false}, under 600 bytes even with a
	// 120-rune subject of four-byte runes, so 4 KiB leaves room for whitespace
	// and still bounds the read (§9.1).
	maxManifestBytes = 4 << 10
)

// Error is a §9.1 validation failure. The runner matches it with errors.As to
// classify the run, so the message reads as the ledger row it will write and
// always names the file at fault.
type Error struct {
	File string
	Err  error
}

func (e *Error) Error() string { return fmt.Sprintf("output %s: %v", e.File, e.Err) }

func (e *Error) Unwrap() error { return e.Err }

// Outputs is a validated out/ directory: the manifest fields §9.2 and §10 act
// on, and the report bytes they file and render.
type Outputs struct {
	Silent  bool
	Subject string
	Report  []byte
}

// manifest is outputs.json's fixed schema. silent is a pointer because it is
// required: an absent key must be distinguishable from an explicit false, or a
// model that omits it would silently get a report emailed (§9.1).
type manifest struct {
	Email  *manifestEmail `json:"email"`
	Silent *bool          `json:"silent"`
}

type manifestEmail struct {
	Subject string `json:"subject"`
}

// Validate holds out/ against §9.1: the file set, the manifest schema and the
// report's encoding and size. Every failure carries Class.
func Validate(outDir string) (*Outputs, error) {
	entries, err := os.ReadDir(outDir)
	if err != nil {
		return nil, &Error{File: outDir, Err: err}
	}
	present := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if name != ReportFile && name != ManifestFile {
			// The agent had no reason to write anything else (§9.1).
			return nil, &Error{File: name, Err: fmt.Errorf("out/ may contain only %s and %s", ReportFile, ManifestFile)}
		}
		// Type() reports the lstat mode, so a symlink named report.md pointing
		// outside the run directory is refused rather than followed into
		// arbitrary host content.
		if !e.Type().IsRegular() {
			return nil, &Error{File: name, Err: errors.New("not a regular file")}
		}
		present[name] = true
	}
	for _, name := range []string{ReportFile, ManifestFile} {
		if !present[name] {
			return nil, &Error{File: name, Err: errors.New("missing")}
		}
	}

	m, err := readManifest(filepath.Join(outDir, ManifestFile))
	if err != nil {
		return nil, err
	}
	if m.Silent == nil {
		return nil, &Error{File: ManifestFile, Err: errors.New("silent: required")}
	}
	out := &Outputs{Silent: *m.Silent}
	if !out.Silent {
		subject, err := checkSubject(m.Email)
		if err != nil {
			return nil, err
		}
		out.Subject = subject
	}

	report, err := readAtMost(filepath.Join(outDir, ReportFile), maxReportBytes)
	if err != nil {
		return nil, &Error{File: ReportFile, Err: err}
	}
	if !utf8.Valid(report) {
		return nil, &Error{File: ReportFile, Err: errors.New("not valid UTF-8")}
	}
	// A silent run may report nothing at all; a run that is about to be emailed
	// may not (§9.1).
	if !out.Silent && len(report) < minReportBytes {
		return nil, &Error{File: ReportFile, Err: fmt.Errorf("%d bytes, at least %d when silent is false", len(report), minReportBytes)}
	}
	out.Report = report
	return out, nil
}

// readManifest decodes outputs.json strictly: an unknown key is a refusal
// rather than a silently ignored field, config.Load's rule.
func readManifest(path string) (*manifest, error) {
	data, err := readAtMost(path, maxManifestBytes)
	if err != nil {
		return nil, &Error{File: ManifestFile, Err: err}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var m manifest
	if err := dec.Decode(&m); err != nil {
		return nil, &Error{File: ManifestFile, Err: err}
	}
	// Decode reads one JSON value; anything after it would otherwise slip past
	// the strict decode above.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, &Error{File: ManifestFile, Err: errors.New("trailing content after the manifest object")}
	}
	return &m, nil
}

// readAtMost reads path, refusing anything past limit before it is held in
// memory. out/ is written by the sandboxed model, so a huge or sparse file
// there has to end as an output failure rather than as an out-of-memory in the
// privileged harness (§9.1). Reading limit+1 bytes is what separates a file
// exactly at the limit from one over it; the refusal names the limit rather
// than a size, since a bounded read never learns the file's real length.
func readAtMost(path string, limit int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("at most %d bytes", limit)
	}
	return data, nil
}

// checkSubject applies §9.1's subject rule: required when silent is false, 1 to
// 120 characters, printable, no newlines. The length is counted in runes, since
// the cap is on what a mail client shows, not on bytes.
func checkSubject(email *manifestEmail) (string, error) {
	if email == nil || email.Subject == "" {
		return "", &Error{File: ManifestFile, Err: errors.New("email.subject: required when silent is false")}
	}
	subject := email.Subject
	// Checked before the printable sweep so a header-splitting subject says so.
	if strings.ContainsAny(subject, "\r\n") {
		return "", &Error{File: ManifestFile, Err: errors.New("email.subject: contains a newline")}
	}
	if n := utf8.RuneCountInString(subject); n > maxSubjectRunes {
		return "", &Error{File: ManifestFile, Err: fmt.Errorf("email.subject: %d characters, at most %d", n, maxSubjectRunes)}
	}
	for _, r := range subject {
		if !unicode.IsPrint(r) {
			return "", &Error{File: ManifestFile, Err: fmt.Errorf("email.subject: unprintable character %q", r)}
		}
	}
	return subject, nil
}
