package ledger

import (
	"database/sql"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// claim claims a slot and fails the test if the slot was already taken.
func claim(t *testing.T, s *Store, agent, at string) *Run {
	t.Helper()
	run, ok, err := s.Claim(agent, slot(t, at), TriggerCron)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !ok {
		t.Fatalf("claim %s at %s: already claimed", agent, at)
	}
	return run
}

func countRuns(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM runs`).Scan(&n); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	return n
}

func TestRunIDFormat(t *testing.T) {
	s := newStore(t)
	run := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	// The slot is rendered in its own location, so the id reads as the wall
	// clock the operator scheduled, not as UTC.
	if !regexp.MustCompile(`^daily-20260902-043000-[0-9a-f]{4}$`).MatchString(run.ID) {
		t.Fatalf("run id = %q, want <agent>-20260902-043000-<4 hex>", run.ID)
	}
	if run.ScheduledFor != "2026-09-02T04:30:00-07:00" {
		t.Fatalf("scheduled_for = %q, want the slot with its offset", run.ScheduledFor)
	}
}

func TestSecondClaimOfSlotIsNoOp(t *testing.T) {
	s := newStore(t)
	first := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")

	run, ok, err := s.Claim("daily", slot(t, "2026-09-02T04:30:00-07:00"), TriggerCron)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if ok || run != nil {
		t.Fatalf("second claim = (%v, %t), want (nil, false)", run, ok)
	}
	if n := countRuns(t, s); n != 1 {
		t.Fatalf("runs = %d, want 1", n)
	}
	if _, err := s.Get(first.ID); err != nil {
		t.Fatalf("the first row must survive the no-op: %v", err)
	}
}

func TestManualClaimIsASeparateSlotRow(t *testing.T) {
	s := newStore(t)
	claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	// The unique index carries trigger_kind: `shuttle run --slot` re-reports a
	// day the cron slot already covered, and that is a second run on purpose.
	if _, ok, err := s.Claim("daily", slot(t, "2026-09-02T04:30:00-07:00"), TriggerManual); err != nil || !ok {
		t.Fatalf("manual claim = (%t, %v), want claimed", ok, err)
	}
	if n := countRuns(t, s); n != 2 {
		t.Fatalf("runs = %d, want 2", n)
	}
}

func TestSkipIsTerminalAndRepeatsAreNoOps(t *testing.T) {
	s := newStore(t)
	run, ok, err := s.Skip("daily", slot(t, "2026-09-02T04:30:00-07:00"), TriggerCron, SkipMissed)
	if err != nil || !ok {
		t.Fatalf("skip = (%t, %v), want recorded", ok, err)
	}
	got, err := s.Get(run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != StatusSkipped || got.SkipReason.String != SkipMissed {
		t.Fatalf("status/reason = %s/%s, want %s/%s", got.Status, got.SkipReason.String, StatusSkipped, SkipMissed)
	}
	if _, ok, err := s.Skip("daily", slot(t, "2026-09-02T04:30:00-07:00"), TriggerCron, SkipOverlap); err != nil || ok {
		t.Fatalf("repeat skip = (%t, %v), want the no-op", ok, err)
	}
	if n := countRuns(t, s); n != 1 {
		t.Fatalf("runs = %d, want 1", n)
	}
	// §5: last_fired_slot advances only on a claim; a skipped slot leaves it.
	agent, err := s.Agent("daily")
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	if agent.LastFiredSlot.Valid {
		t.Fatalf("last_fired_slot = %q after a skip, want unset", agent.LastFiredSlot.String)
	}
}

func TestClaimAdvancesLastFiredSlot(t *testing.T) {
	s := newStore(t)
	claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	agent, err := s.Agent("daily")
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	if agent.LastFiredSlot.String != "2026-09-02T04:30:00-07:00" {
		t.Fatalf("last_fired_slot = %q, want the claimed slot", agent.LastFiredSlot.String)
	}
}

func TestStateMachine(t *testing.T) {
	moves := map[string]func(*Store, string) error{
		StatusRunning: func(s *Store, id string) error {
			return s.MarkRunning(id, 4242, "/tmp/run")
		},
		StatusCompleted: func(s *Store, id string) error {
			return s.Complete(id, Result{})
		},
		StatusFailed: func(s *Store, id string) error {
			return s.Fail(id, ClassDied, "exit 1", Result{})
		},
	}
	tests := []struct {
		from    string
		to      string
		wantErr bool
	}{
		{StatusClaimed, StatusRunning, false},
		{StatusClaimed, StatusCompleted, false},
		{StatusClaimed, StatusFailed, false},
		{StatusRunning, StatusRunning, true},
		{StatusRunning, StatusCompleted, false},
		{StatusRunning, StatusFailed, false},
		{StatusCompleted, StatusRunning, true},
		{StatusCompleted, StatusCompleted, true},
		{StatusCompleted, StatusFailed, true},
		{StatusFailed, StatusRunning, true},
		{StatusFailed, StatusCompleted, true},
		{StatusFailed, StatusFailed, true},
		{StatusUnknown, StatusRunning, true},
		{StatusUnknown, StatusCompleted, true},
		{StatusUnknown, StatusFailed, true},
	}
	for _, tt := range tests {
		t.Run(tt.from+" to "+tt.to, func(t *testing.T) {
			s := newStore(t)
			run := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
			driveTo(t, s, run.ID, tt.from)

			err := moves[tt.to](s, run.ID)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("move %s to %s succeeded, want a refusal", tt.from, tt.to)
				}
				// The refusal must name the run and both statuses.
				for _, want := range []string{run.ID, tt.from, tt.to} {
					if !regexp.MustCompile(regexp.QuoteMeta(want)).MatchString(err.Error()) {
						t.Fatalf("error %q does not name %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("move %s to %s: %v", tt.from, tt.to, err)
			}
			got, err := s.Get(run.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.Status != tt.to {
				t.Fatalf("status = %s, want %s", got.Status, tt.to)
			}
		})
	}
}

// driveTo moves a freshly claimed run into status.
func driveTo(t *testing.T, s *Store, id, status string) {
	t.Helper()
	var err error
	switch status {
	case StatusClaimed:
		return
	case StatusRunning:
		err = s.MarkRunning(id, 4242, "/tmp/run")
	case StatusCompleted:
		err = s.Complete(id, Result{})
	case StatusFailed:
		err = s.Fail(id, ClassDied, "exit 1", Result{})
	case StatusUnknown:
		err = s.MarkUnknown(id)
	default:
		t.Fatalf("cannot drive a run to %s", status)
	}
	if err != nil {
		t.Fatalf("drive to %s: %v", status, err)
	}
}

func TestInFlightListsClaimedAndRunningInOrder(t *testing.T) {
	s := newStore(t)
	claimed := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	running := claim(t, s, "daily", "2026-09-03T04:30:00-07:00")
	if err := s.MarkRunning(running.ID, 4242, "/tmp/run"); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	done := claim(t, s, "daily", "2026-09-04T04:30:00-07:00")
	if err := s.Complete(done.ID, Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	failed := claim(t, s, "daily", "2026-09-05T04:30:00-07:00")
	if err := s.Fail(failed.ID, ClassDied, "exit 1", Result{}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if _, _, err := s.Skip("daily", slot(t, "2026-09-06T04:30:00-07:00"), TriggerCron, SkipMissed); err != nil {
		t.Fatalf("skip: %v", err)
	}

	rows, err := s.InFlight()
	if err != nil {
		t.Fatalf("in flight: %v", err)
	}
	// Every row shares the fixed clock's claimed_at, so the id is the
	// tiebreaker and the order is still deterministic.
	want := []string{claimed.ID, running.ID}
	slices.Sort(want)
	var got []string
	for _, row := range rows {
		got = append(got, row.ID)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("in flight = %v, want %v in claimed_at, id order", got, want)
	}
}

func TestMarkUnknownClosesInFlightRunsOnly(t *testing.T) {
	s := newStore(t)
	claimed := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	running := claim(t, s, "daily", "2026-09-03T04:30:00-07:00")
	if err := s.MarkRunning(running.ID, 4242, "/tmp/run"); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	for _, id := range []string{claimed.ID, running.ID} {
		if err := s.MarkUnknown(id); err != nil {
			t.Fatalf("mark unknown: %v", err)
		}
		got, err := s.Get(id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status != StatusUnknown {
			t.Fatalf("status = %s, want %s", got.Status, StatusUnknown)
		}
		if !got.EndedAt.Valid {
			t.Fatal("ended_at unset on an unknown row")
		}
	}

	done := claim(t, s, "daily", "2026-09-04T04:30:00-07:00")
	if err := s.Complete(done.ID, Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// A terminal row never moves again (§6.1), so the pass cannot rewrite a
	// run its predecessor finished.
	err := s.MarkUnknown(done.ID)
	if err == nil {
		t.Fatal("mark unknown over a completed row succeeded, want a refusal")
	}
	for _, want := range []string{done.ID, StatusCompleted, StatusUnknown} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
	if err := s.MarkUnknown("no-such-run"); err == nil {
		t.Fatal("mark unknown over a missing id succeeded, want an error")
	}
}

func TestCostDistinguishesUnpricedFromZero(t *testing.T) {
	s := newStore(t)
	unpriced := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	if err := s.Complete(unpriced.ID, Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	free := claim(t, s, "daily", "2026-09-03T04:30:00-07:00")
	if err := s.Complete(free.ID, Result{CostUSD: sql.NullFloat64{Float64: 0, Valid: true}}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	got, err := s.Get(unpriced.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.CostUSD.Valid {
		t.Fatalf("cost = %v, want NULL for a run that reported none", got.CostUSD)
	}
	got, err = s.Get(free.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.CostUSD.Valid || got.CostUSD.Float64 != 0 {
		t.Fatalf("cost = %v, want a present zero", got.CostUSD)
	}
}

func TestDeliveryIsIndependentOfRunStatus(t *testing.T) {
	s := newStore(t)
	run := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	if err := s.Complete(run.ID, Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := s.SetDelivery(run.ID, DeliveryFailed, "401 unauthorized"); err != nil {
		t.Fatalf("set delivery: %v", err)
	}
	if err := s.SetOutputsApplied(run.ID, json.RawMessage(`{"file":"daily/2026-09-01.md"}`)); err != nil {
		t.Fatalf("set outputs: %v", err)
	}

	got, err := s.Get(run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != StatusCompleted {
		t.Fatalf("status = %s, want the run to stay completed", got.Status)
	}
	if got.DeliveryStatus.String != DeliveryFailed || got.DeliveryError.String != "401 unauthorized" {
		t.Fatalf("delivery = %s/%s, want failed with the reason", got.DeliveryStatus.String, got.DeliveryError.String)
	}
	if got.DeliveredAt.Valid {
		t.Fatalf("delivered_at = %q on a failed delivery, want unset", got.DeliveredAt.String)
	}
	if got.OutputsApplied.String != `{"file":"daily/2026-09-01.md"}` {
		t.Fatalf("outputs_applied = %q", got.OutputsApplied.String)
	}

	if err := s.SetDelivery(run.ID, DeliveryDelivered, ""); err != nil {
		t.Fatalf("set delivery: %v", err)
	}
	got, err = s.Get(run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.DeliveredAt.Valid || got.DeliveryError.Valid {
		t.Fatalf("delivered run: delivered_at = %v, error = %v", got.DeliveredAt, got.DeliveryError)
	}
}

func TestRunsListsMostRecentFirst(t *testing.T) {
	s := newStore(t)
	first := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	s.now = func() time.Time { return testClock.Add(time.Hour) }
	second := claim(t, s, "daily", "2026-09-03T04:30:00-07:00")
	other := claim(t, s, "miner", "2026-09-03T05:30:00-07:00")

	runs, err := s.Runs("", 0)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	if len(runs) != 3 || runs[len(runs)-1].ID != first.ID {
		t.Fatalf("runs = %v, want the oldest claim last", runs)
	}
	runs, err = s.Runs("miner", 20)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != other.ID {
		t.Fatalf("filtered runs = %v, want only the miner's", runs)
	}
	runs, err = s.Runs("daily", 1)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != second.ID {
		t.Fatalf("limited runs = %v, want the newest daily run", runs)
	}
}

func TestWriteTimestampsAreUTC(t *testing.T) {
	s := newStore(t)
	// Claimed and stamped from a clock in the scheduler's own zone: the write
	// columns are ordered as text, so they must not carry its offset.
	s.now = func() time.Time { return slot(t, "2026-11-01T01:30:00-07:00") }
	run := claim(t, s, "daily", "2026-11-01T01:30:00-07:00")
	if err := s.Complete(run.ID, Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, err := s.Get(run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	for _, ts := range []string{got.ClaimedAt, got.EndedAt.String} {
		if ts != "2026-11-01T08:30:00Z" {
			t.Fatalf("write timestamp = %q, want it in UTC", ts)
		}
	}
	// §5 needs the slot's own offset, so scheduled_for keeps it.
	if got.ScheduledFor != "2026-11-01T01:30:00-07:00" {
		t.Fatalf("scheduled_for = %q, want the slot with its offset", got.ScheduledFor)
	}
}

func TestSpendSinceSumsEveryAgent(t *testing.T) {
	s := newStore(t)
	spent, err := s.SpendSince(testClock.Add(-24 * time.Hour))
	if err != nil {
		t.Fatalf("spend: %v", err)
	}
	// An empty window is unpriced, not free (§12).
	if spent.Valid {
		t.Fatalf("spend over an empty ledger = %v, want NULL", spent)
	}
	priced := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	if err := s.Complete(priced.ID, Result{CostUSD: sql.NullFloat64{Float64: 0.75, Valid: true}}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	other := claim(t, s, "miner", "2026-09-02T04:30:00-07:00")
	if err := s.Complete(other.ID, Result{CostUSD: sql.NullFloat64{Float64: 0.25, Valid: true}}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// A run that reported no cost contributes nothing rather than poisoning
	// the sum.
	unpriced := claim(t, s, "daily", "2026-09-02T05:30:00-07:00")
	if err := s.Complete(unpriced.ID, Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	spent, err = s.SpendSince(testClock.Add(-24 * time.Hour))
	if err != nil {
		t.Fatalf("spend: %v", err)
	}
	if !spent.Valid || spent.Float64 != 1.0 {
		t.Fatalf("spend = %v, want 1", spent)
	}
	spent, err = s.SpendSince(testClock.Add(time.Second))
	if err != nil {
		t.Fatalf("spend: %v", err)
	}
	if spent.Valid {
		t.Fatalf("spend after the window = %v, want NULL", spent)
	}
}

// lastFiredSlot reads the scheduler's catch-up cursor for an agent.
func lastFiredSlot(t *testing.T, s *Store, agent string) string {
	t.Helper()
	row, err := s.Agent(agent)
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	return row.LastFiredSlot.String
}

func TestManualClaimLeavesLastFiredSlotAlone(t *testing.T) {
	s := newStore(t)
	claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	if got := lastFiredSlot(t, s, "daily"); got != "2026-09-02T04:30:00-07:00" {
		t.Fatalf("last_fired_slot after a cron claim = %q, want the slot", got)
	}
	// A manual run bypasses the schedule (§5), so it must not move the
	// scheduler's cursor — forward, which would suspend the agent until the
	// slot it names, or backward, which would re-walk days already behind it.
	if _, ok, err := s.Claim("daily", slot(t, "2026-12-01T04:30:00-08:00"), TriggerManual); err != nil || !ok {
		t.Fatalf("manual claim = (%t, %v), want claimed", ok, err)
	}
	if got := lastFiredSlot(t, s, "daily"); got != "2026-09-02T04:30:00-07:00" {
		t.Fatalf("last_fired_slot after a manual claim = %q, want the cron slot", got)
	}
}

func TestLastRecordedSlotIsTheNewestCronRow(t *testing.T) {
	s := newStore(t)
	if got, err := s.LastRecordedSlot("daily"); err != nil || got != "" {
		t.Fatalf("last recorded slot on an empty ledger = (%q, %v), want (\"\", nil)", got, err)
	}
	// A skipped slot leaves last_fired_slot alone but is still recorded, so it
	// is the cursor.
	if _, _, err := s.Skip("daily", slot(t, "2026-09-02T04:30:00-07:00"), TriggerCron, SkipPaused); err != nil {
		t.Fatalf("skip: %v", err)
	}
	if got, err := s.LastRecordedSlot("daily"); err != nil || got != "2026-09-02T04:30:00-07:00" {
		t.Fatalf("last recorded slot = (%q, %v), want the skipped slot", got, err)
	}
	if _, ok, err := s.Claim("daily", slot(t, "2026-12-01T04:30:00-08:00"), TriggerManual); err != nil || !ok {
		t.Fatalf("manual claim = (%t, %v), want claimed", ok, err)
	}
	if got, err := s.LastRecordedSlot("daily"); err != nil || got != "2026-09-02T04:30:00-07:00" {
		t.Fatalf("last recorded slot after a manual row = (%q, %v), want the cron slot", got, err)
	}
}

func TestClaimGatedRefusesASecondSlotWhileOneIsInFlight(t *testing.T) {
	s := newStore(t)
	run, reason, err := s.ClaimGated("daily", slot(t, "2026-09-02T04:30:00-07:00"), TriggerCron, Limits{})
	if err != nil || reason != "" || run == nil {
		t.Fatalf("first claim = (%v, %q, %v), want the run", run, reason, err)
	}
	// A different slot, so the unique index cannot be the arbiter: the gate
	// and the insert being one transaction is what refuses it.
	run, reason, err = s.ClaimGated("daily", slot(t, "2026-09-02T05:30:00-07:00"), TriggerManual, Limits{})
	if err != nil || run != nil || reason != SkipOverlap {
		t.Fatalf("second claim = (%v, %q, %v), want a skipped overlap", run, reason, err)
	}
	rows, err := s.Runs("daily", 0)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want the claim and its skipped row", len(rows))
	}
}

func TestClaimGatedRepeatsASlotOnce(t *testing.T) {
	s := newStore(t)
	done := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	if err := s.Complete(done.ID, Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// The slot is already in the ledger: the unique index refuses it, which is
	// neither a claim nor a gate refusal, so both come back empty.
	run, reason, err := s.ClaimGated("daily", slot(t, "2026-09-02T04:30:00-07:00"), TriggerCron, Limits{})
	if err != nil || run != nil || reason != "" {
		t.Fatalf("repeat = (%v, %q, %v), want (nil, \"\", nil)", run, reason, err)
	}
	if n := countRuns(t, s); n != 1 {
		t.Fatalf("runs = %d, want 1", n)
	}
}
