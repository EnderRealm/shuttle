// Package loomworkreport shells out to `loom work-report` and stores its JSON
// verbatim (§8.3). loom's module path is the bare `loom`, so the report cannot
// be produced in process.
package loomworkreport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"shuttle/internal/provider"
)

const (
	dbFile = "summaries.db"

	// stderrTail bounds what a failure carries into the ledger's error
	// signature (§6.2).
	stderrTail = 500
)

// Run implements provider.Func.
func Run(ctx context.Context, env provider.Env, _ map[string]any, w provider.Window) (json.RawMessage, error) {
	if env.LoomBin == "" {
		return nil, errors.New("paths.loom_bin: not configured")
	}
	// --db explicitly rather than LOOM_HOME: the database this reports over is
	// the one config.json names, whatever the daemon's environment says.
	cmd := exec.CommandContext(ctx, env.LoomBin, "work-report",
		"--since", w.From.Format(time.RFC3339),
		"--until", w.To.Format(time.RFC3339),
		"--db", filepath.Join(env.LoomHome, dbFile))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s work-report: %w%s", env.LoomBin, err, tail(stderr.String()))
	}
	report := bytes.TrimSpace(stdout.Bytes())
	if err := checkOneObject(report); err != nil {
		return nil, fmt.Errorf("%s work-report: %w", env.LoomBin, err)
	}
	return json.RawMessage(report), nil
}

// checkOneObject holds the output to what §8.3 promises: exactly one JSON
// object, so a document with a banner line or a truncated write is a provider
// failure rather than a bundle the model has to make sense of.
func checkOneObject(report []byte) error {
	dec := json.NewDecoder(bytes.NewReader(report))
	var v json.RawMessage
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("stdout is not JSON: %w", err)
	}
	if len(v) == 0 || v[0] != '{' {
		return errors.New("stdout is not a JSON object")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("stdout carries more than one JSON value")
	}
	return nil
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) > stderrTail {
		s = s[len(s)-stderrTail:]
	}
	return ": " + s
}
