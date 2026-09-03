package cmd

import (
	"database/sql"
	"testing"
)

func TestEscapeControl(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"ordinary text", "died: the runtime exited", "died: the runtime exited"},
		{"utf-8 survives", "provider: 429 — slow down", "provider: 429 — slow down"},
		{"escape sequence", "auth: \x1b[2J\x1b]0;pwned\a", `auth: \x1b[2J\x1b]0;pwned\a`},
		{"newline", "line one\nline two", `line one\nline two`},
		{"tab", "col\tcol", `col\tcol`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeControl(tt.value); got != tt.want {
				t.Fatalf("escapeControl(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestNullOrEscapesTheValue(t *testing.T) {
	if got := nullOr(sql.NullString{}); got != "-" {
		t.Fatalf("unset column = %q, want a dash", got)
	}
	// delivery_error is an email provider's response verbatim; the escaping
	// belongs here, at the render boundary, not on the way into the ledger.
	got := nullOr(sql.NullString{String: "401 \x1b[31munauthorized", Valid: true})
	if got != `401 \x1b[31munauthorized` {
		t.Fatalf("nullOr = %q, want the escape sequence escaped", got)
	}
}
