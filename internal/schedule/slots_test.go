package schedule

import (
	"testing"
	"time"

	"shuttle/internal/agentdef"
)

// agent builds a definition directly: the slot maths reads the manifest's
// triggers and the name, so a directory on disk would add nothing.
func agent(name string, triggers ...agentdef.Trigger) *agentdef.Definition {
	return &agentdef.Definition{
		Dir:      "agents/" + name,
		Name:     name,
		Manifest: agentdef.Manifest{Name: name, Triggers: triggers},
	}
}

func cronTrigger(expression, timezone string) agentdef.Trigger {
	return agentdef.Trigger{Kind: "cron", Expression: expression, Timezone: timezone}
}

func mustParse(t *testing.T, value string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return at
}

func TestJitterIsDeterministicAndBounded(t *testing.T) {
	// The literal is the point: FNV-1a of "daily" mod 300 is 198 in every
	// process and every Go version, so swapping the hash breaks this test
	// rather than silently moving the agent's start minute.
	if got := Jitter("daily"); got != 198*time.Second {
		t.Fatalf("Jitter(\"daily\") = %s, want 3m18s", got)
	}
	for _, name := range []string{"", "daily", "miner", "monitor", "a-very-long-agent-name"} {
		got := Jitter(name)
		if got != Jitter(name) {
			t.Fatalf("Jitter(%q) is not deterministic", name)
		}
		if got < 0 || got >= jitterSeconds*time.Second {
			t.Fatalf("Jitter(%q) = %s, want [0s, 5m0s)", name, got)
		}
	}
}

func TestDue(t *testing.T) {
	daily := agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles"))
	tests := []struct {
		name string
		def  *agentdef.Definition
		last string
		now  string
		want []string
	}{
		{
			name: "nothing due before the slot",
			def:  daily,
			last: "2026-09-01T04:30:00-07:00",
			now:  "2026-09-02T04:29:00-07:00",
		},
		{
			name: "nothing due until the jitter has passed",
			def:  daily,
			last: "2026-09-01T04:30:00-07:00",
			now:  "2026-09-02T04:32:00-07:00",
		},
		{
			name: "due once the jitter has passed",
			def:  daily,
			last: "2026-09-01T04:30:00-07:00",
			now:  "2026-09-02T04:36:00-07:00",
			want: []string{"2026-09-02T04:30:00-07:00"},
		},
		{
			name: "a sleep leaves every missed slot due, oldest first",
			def:  daily,
			last: "2026-08-30T04:30:00-07:00",
			now:  "2026-09-02T04:36:00-07:00",
			want: []string{
				"2026-08-31T04:30:00-07:00",
				"2026-09-01T04:30:00-07:00",
				"2026-09-02T04:30:00-07:00",
			},
		},
		{
			name: "an agent that has never fired has no history to replay",
			def:  daily,
			now:  "2026-09-02T12:00:00-07:00",
		},
		{
			name: "two triggers that match the same instant are one slot",
			def: agent("daily",
				cronTrigger("30 4 * * *", "America/Los_Angeles"),
				cronTrigger("30 11 * * *", "UTC"),
			),
			last: "2026-09-01T04:30:00-07:00",
			now:  "2026-09-02T04:36:00-07:00",
			want: []string{"2026-09-02T04:30:00-07:00"},
		},
		{
			name: "the repeated fall-back hour is one slot",
			def:  agent("daily", cronTrigger("30 1 * * *", "America/Los_Angeles")),
			last: "2026-10-31T01:30:00-07:00",
			now:  "2026-11-01T12:00:00-08:00",
			want: []string{"2026-11-01T01:30:00-07:00"},
		},
		{
			name: "the spring-forward slot does not exist",
			def:  agent("daily", cronTrigger("30 2 * * *", "America/Los_Angeles")),
			last: "2026-03-07T02:30:00-08:00",
			now:  "2026-03-08T23:00:00-07:00",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var last time.Time
			if tt.last != "" {
				last = mustParse(t, tt.last)
			}
			slots, err := Due(tt.def, last, mustParse(t, tt.now))
			if err != nil {
				t.Fatalf("due: %v", err)
			}
			var got []string
			for _, at := range slots {
				got = append(got, at.Format(time.RFC3339))
			}
			if len(got) != len(tt.want) {
				t.Fatalf("due = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("due = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestManualSlot(t *testing.T) {
	tests := []struct {
		name    string
		def     *agentdef.Definition
		date    string
		want    string
		wantErr bool
	}{
		{
			name: "the day's slot",
			def:  agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles")),
			date: "2026-09-01",
			want: "2026-09-01T04:30:00-07:00",
		},
		{
			name: "a slot at midnight belongs to its own day",
			def:  agent("daily", cronTrigger("0 0 * * *", "America/Los_Angeles")),
			date: "2026-09-01",
			want: "2026-09-01T00:00:00-07:00",
		},
		{
			name: "the earliest slot across triggers",
			def: agent("daily",
				cronTrigger("30 4 * * *", "America/Los_Angeles"),
				cronTrigger("0 2 * * *", "America/Los_Angeles"),
			),
			date: "2026-09-01",
			want: "2026-09-01T02:00:00-07:00",
		},
		{
			name:    "a spring-forward day has no 02:30",
			def:     agent("daily", cronTrigger("30 2 * * *", "America/Los_Angeles")),
			date:    "2026-03-08",
			wantErr: true,
		},
		{
			name:    "a weekday expression has no slot on a Sunday",
			def:     agent("daily", cronTrigger("30 4 * * 1-5", "America/Los_Angeles")),
			date:    "2026-09-06",
			wantErr: true,
		},
		{
			name:    "not a date",
			def:     agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles")),
			date:    "yesterday",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at, err := ManualSlot(tt.def, tt.date)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ManualSlot(%q) = %s, want an error", tt.date, at.Format(time.RFC3339))
				}
				return
			}
			if err != nil {
				t.Fatalf("ManualSlot(%q): %v", tt.date, err)
			}
			if got := at.Format(time.RFC3339); got != tt.want {
				t.Fatalf("ManualSlot(%q) = %s, want %s", tt.date, got, tt.want)
			}
		})
	}
}

func TestManualSlotNowIsTheTriggerZone(t *testing.T) {
	def := agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles"))
	at, err := ManualSlotNow(def, mustParse(t, "2026-09-02T18:07:31Z").Add(500*time.Millisecond))
	if err != nil {
		t.Fatalf("manual slot now: %v", err)
	}
	if got := at.Format(time.RFC3339); got != "2026-09-02T11:07:31-07:00" {
		t.Fatalf("slot = %s, want the trigger zone truncated to the second", got)
	}
}
