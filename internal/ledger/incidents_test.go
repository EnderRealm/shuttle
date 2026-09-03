package ledger

import (
	"strings"
	"testing"
)

func TestSignature(t *testing.T) {
	tests := []struct {
		name    string
		class   string
		errText string
		want    string
	}{
		{"class only", ClassAuth, "", ClassAuth},
		{"first line only", ClassDied, "Exit 1\nstack trace here", "died: exit"},
		{"paths stripped", ClassProvider, "provider loom-sessions: open /Users/steve/.loom/summaries.db: no such file", "provider: provider loom-sessions: open no such file"},
		{"digits stripped", ClassDied, "exit status 137 after 900s", "died: exit status after s"},
		{"whitespace collapsed", ClassOutput, "out/report.md:   too    large", "output: too large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Signature(tt.class, tt.errText); got != tt.want {
				t.Fatalf("Signature(%q, %q) = %q, want %q", tt.class, tt.errText, got, tt.want)
			}
		})
	}
}

func TestSignatureIsCapped(t *testing.T) {
	got := Signature(ClassDied, strings.Repeat("a", 400))
	if len([]rune(got)) != signatureMax {
		t.Fatalf("length = %d, want %d", len([]rune(got)), signatureMax)
	}
}

func TestIncidentCountsRepeats(t *testing.T) {
	s := newStore(t)
	failNth(t, s, 1, ClassDied)
	second := failNth(t, s, 2, ClassDied)

	incidents, err := s.OpenIncidents()
	if err != nil {
		t.Fatalf("open incidents: %v", err)
	}
	if len(incidents) != 1 {
		t.Fatalf("incidents = %d, want the repeat counted rather than duplicated", len(incidents))
	}
	got := incidents[0]
	if got.Count != 2 {
		t.Fatalf("count = %d, want 2", got.Count)
	}
	if got.LastRunID.String != second {
		t.Fatalf("last_run_id = %q, want the most recent run", got.LastRunID.String)
	}
	if got.Signature != Signature(ClassDied, "the runtime exited 1") {
		t.Fatalf("signature = %q", got.Signature)
	}

	// A different failure is a different incident, not a bump.
	failNth(t, s, 3, ClassProvider)
	incidents, err = s.OpenIncidents()
	if err != nil {
		t.Fatalf("open incidents: %v", err)
	}
	if len(incidents) != 2 {
		t.Fatalf("incidents = %d, want one per signature", len(incidents))
	}
}

func TestAckClosesIncidentAndTheNextOccurrenceOpensANewOne(t *testing.T) {
	s := newStore(t)
	failNth(t, s, 1, ClassDied)
	incidents, err := s.OpenIncidents()
	if err != nil {
		t.Fatalf("open incidents: %v", err)
	}
	closed := incidents[0].ID

	if err := s.Ack(closed); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := s.Ack(closed); err == nil {
		t.Fatal("acking a closed incident succeeded, want an error")
	}
	if incidents, err = s.OpenIncidents(); err != nil || len(incidents) != 0 {
		t.Fatalf("open incidents after ack = %d (%v), want none", len(incidents), err)
	}

	failNth(t, s, 2, ClassDied)
	incidents, err = s.OpenIncidents()
	if err != nil {
		t.Fatalf("open incidents: %v", err)
	}
	if len(incidents) != 1 {
		t.Fatalf("incidents = %d, want the next occurrence to open a new one", len(incidents))
	}
	if incidents[0].ID == closed {
		t.Fatal("the acknowledged incident was reopened, want a new row")
	}
	if incidents[0].Count != 1 {
		t.Fatalf("count = %d, want a fresh incident", incidents[0].Count)
	}
}
