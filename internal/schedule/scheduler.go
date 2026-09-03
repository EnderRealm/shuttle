package schedule

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/ledger"
)

// Ledger is the subset of the run ledger the scheduler uses, declared here on
// the consumer side.
type Ledger interface {
	Agent(name string) (*ledger.Agent, error)
	LastRecordedSlot(agent string) (string, error)
	ClaimGated(agent string, slot time.Time, triggerKind string, limits ledger.Limits) (*ledger.Run, string, error)
	Skip(agent string, slot time.Time, triggerKind, reason string) (*ledger.Run, bool, error)
}

var _ Ledger = (*ledger.Store)(nil)

// defaultLog is where a Scheduler with no Log writes.
var defaultLog = log.New(os.Stderr, "", log.LstdFlags)

// Scheduler is the §5 loop: one pass per tick, per agent. Limits arrive as
// plain values rather than as a config struct, so slot maths stays independent
// of where the operator's tunables come from.
type Scheduler struct {
	Ledger        Ledger
	Defs          []*agentdef.Definition
	Loc           *time.Location // the day boundary the daily limits are measured from
	MaxRunsPerDay int            // config limits.daily_runs_per_agent; <= 0 disables
	MaxSpendUSD   float64        // config limits.daily_spend_usd; <= 0 disables
	Now           func() time.Time
	Log           *log.Logger

	// Launch owns the run it is handed: the implementation must move that row
	// to a terminal status (§6.1) even when it fails to start, because a row
	// left claimed makes every later slot for the agent an overlap skip (§5
	// step 4) until a daemon restart's Reconcile clears it. nil claims the slot
	// and launches nothing.
	Launch func(*agentdef.Definition, *ledger.Run) error

	// firstSeen is the catch-up cursor for an agent the ledger has no slot
	// for: the tick that first saw it. Without it the cursor is re-derived as
	// now on every pass, so the next slot is always still ahead and a new
	// agent never fires at all. "Seen but never fired" is not a run and has no
	// ledger row (§6.1), so this is process memory; a daemon restart re-seeds
	// it, which costs a new agent at most the slot it was waiting on.
	firstSeen map[string]time.Time
}

// Tick is one pass of the loop. An error from one agent is collected and the
// pass continues: one broken definition must not stop every other agent.
func (s *Scheduler) Tick() error {
	var errs []error
	for _, def := range s.Defs {
		if err := s.tickAgent(def); err != nil {
			errs = append(errs, fmt.Errorf("agent %s: %w", def.Name, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Scheduler) tickAgent(def *agentdef.Definition) error {
	last, err := s.cursor(def.Name)
	if err != nil {
		return err
	}
	slots, err := Due(def, last, s.now())
	if err != nil {
		return err
	}
	if len(slots) == 0 {
		return nil
	}
	// §5 step 3, catch-up-one: the machine having slept through a run of slots
	// is not a reason to run them, only to record that they passed.
	for _, at := range slots[:len(slots)-1] {
		if err := s.skip(def.Name, at, ledger.SkipMissed); err != nil {
			return err
		}
	}
	newest := slots[len(slots)-1]
	run, reason, err := s.Claim(def.Name, newest, ledger.TriggerCron)
	if err != nil {
		return err
	}
	if reason != "" {
		s.logf("agent=%s slot=%s action=skip reason=%s", def.Name, slotText(newest), reason)
		return nil
	}
	if run == nil {
		// The unique index refused a slot already in the ledger. That is the
		// arbiter working, not a failure of this tick.
		s.logf("agent=%s slot=%s action=claim result=already-recorded", def.Name, slotText(newest))
		return nil
	}
	s.logf("agent=%s slot=%s action=claim run=%s", def.Name, slotText(newest), run.ID)
	if s.Launch == nil {
		return nil
	}
	return s.Launch(def, run)
}

// cursor is where the next batch of due slots is computed from: the later of
// the agent's last_fired_slot and the newest slot the scheduler has already
// recorded. The two differ because §5 deliberately does not advance
// last_fired_slot on a skip, so an agent whose slots are all being gated —
// paused, overlapping, over a limit — would otherwise re-derive the same
// growing slot set on every 60-second tick forever. The recorded slot normally
// wins; last_fired_slot stays in the comparison to guard a legacy or
// hand-edited row that has no run behind it.
func (s *Scheduler) cursor(agent string) (time.Time, error) {
	row, err := s.Ledger.Agent(agent)
	if err != nil {
		return time.Time{}, err
	}
	var last time.Time
	if row.LastFiredSlot.Valid {
		last, err = time.Parse(time.RFC3339, row.LastFiredSlot.String)
		if err != nil {
			return time.Time{}, fmt.Errorf("last_fired_slot %q: %w", row.LastFiredSlot.String, err)
		}
	}
	recorded, err := s.Ledger.LastRecordedSlot(agent)
	if err != nil {
		return time.Time{}, err
	}
	if recorded != "" {
		// Compared as instants: the two columns carry different offsets across
		// a DST change, so comparing the text would order them wrongly.
		at, err := time.Parse(time.RFC3339, recorded)
		if err != nil {
			return time.Time{}, fmt.Errorf("scheduled_for %q: %w", recorded, err)
		}
		if at.After(last) {
			last = at
		}
	}
	if last.IsZero() {
		// An agent with nothing in the ledger has no catch-up basis, so it
		// fires at its next slot rather than replaying its history.
		if s.firstSeen == nil {
			s.firstSeen = map[string]time.Time{}
		}
		if _, ok := s.firstSeen[agent]; !ok {
			s.firstSeen[agent] = s.now()
		}
		last = s.firstSeen[agent]
	}
	return last, nil
}

// Claim puts one slot through the §5 gate and the ledger's atomic claim. It
// returns the claimed run, or a nil run and the skip reason the gate recorded,
// or a nil run and an empty reason when the slot was already in the ledger.
// `shuttle run` goes through it too: a manual run bypasses the schedule but not
// the overlap check, the limits or the ledger (§5), and sharing this is what
// keeps the manual path from drifting from the daemon's.
func (s *Scheduler) Claim(agent string, at time.Time, triggerKind string) (*ledger.Run, string, error) {
	return s.Ledger.ClaimGated(agent, at, triggerKind, ledger.Limits{
		MaxRunsPerDay: s.MaxRunsPerDay,
		MaxSpendUSD:   s.MaxSpendUSD,
		DayStart:      s.DayStart(),
	})
}

// DayStart is midnight in the scheduler's zone: the boundary the daily limits,
// and `shuttle status`'s spend, are measured from.
func (s *Scheduler) DayStart() time.Time {
	now := s.now().In(s.loc())
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
}

func (s *Scheduler) skip(agent string, at time.Time, reason string) error {
	_, ok, err := s.Ledger.Skip(agent, at, ledger.TriggerCron, reason)
	if err != nil {
		return err
	}
	if !ok {
		s.logf("agent=%s slot=%s action=skip reason=%s result=already-recorded", agent, slotText(at), reason)
		return nil
	}
	s.logf("agent=%s slot=%s action=skip reason=%s", agent, slotText(at), reason)
	return nil
}

func (s *Scheduler) now() time.Time {
	if s.Now == nil {
		return time.Now()
	}
	return s.Now()
}

func (s *Scheduler) loc() *time.Location {
	if s.Loc == nil {
		return time.Local
	}
	return s.Loc
}

// logf writes one decision line. Every branch of a tick logs, because these
// lines are the daemon's debugging record: launchd points its stdout and
// stderr at ~/.shuttle/daemon.log (§13). A Scheduler with no Log falls back to
// stderr rather than panicking.
func (s *Scheduler) logf(msg string, args ...any) {
	l := s.Log
	if l == nil {
		l = defaultLog
	}
	l.Printf(msg, args...)
}

// slotText renders a slot the way the ledger stores it, so a log line and a
// scheduled_for column can be grepped with the same string.
func slotText(at time.Time) string {
	return at.Format(time.RFC3339)
}
