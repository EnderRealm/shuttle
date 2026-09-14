package deliver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"shuttle/internal/ledger"
)

const (
	notifyIncident  = "incident"
	notifyAuth      = "auth"
	notifyPause     = "pause"
	notifyRecovered = "recovered"
)

// Notifier uses loom's osascript, persistent state and cooldown approach.
// The lock spans load/send/save because daemon and manual runs share the file.
type Notifier struct {
	StatePath string
	Enabled   bool
	Cooldown  time.Duration
	Now       func() time.Time
	// Send overrides the native transport; nil uses macOS osascript.
	Send func(context.Context, string, string) error
}

// Notify consumes a committed ledger transition. Repeats of an open incident
// are silent; auth and auto-pause have their own kinds. Recovery bypasses the
// cooldown and clears that agent's failure cooldowns, like loom's recovery.
func (n *Notifier) Notify(ctx context.Context, event ledger.Event) (bool, error) {
	if !n.Enabled {
		return false, nil
	}
	kind := notifyIncident
	body := fmt.Sprintf("%s failed: %s. Incident %d; run %s.", event.Agent, event.Signature, event.IncidentID, event.RunID)
	switch {
	case event.Recovered:
		kind = notifyRecovered
		body = fmt.Sprintf("%s recovered. Run %s completed.", event.Agent, event.RunID)
	case event.Class == ledger.ClassAuth:
		kind = notifyAuth
		body += " Paused for auth; repair the credential, then shuttle resume " + event.Agent + "."
	case event.PausedReason != "":
		kind = notifyPause
		body += " Paused for " + event.PausedReason + "; shuttle resume " + event.Agent + "."
	case !event.NewIncident:
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(n.StatePath), 0o700); err != nil {
		return false, err
	}
	lock, err := os.OpenFile(n.StatePath+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false, err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return false, err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	state := map[string]time.Time{}
	data, err := os.ReadFile(n.StatePath)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if err == nil {
		if err := json.Unmarshal(data, &state); err != nil {
			return false, fmt.Errorf("notify state: %w", err)
		}
		if state == nil {
			state = map[string]time.Time{}
		}
	}
	now := time.Now()
	if n.Now != nil {
		now = n.Now()
	}
	prefix := event.Agent + "\x00"
	key := prefix + kind + "\x00" + event.Signature
	if kind == notifyRecovered {
		key = prefix + kind + "\x00" + event.RunID
	}
	last, seen := state[key]
	if seen && (kind == notifyRecovered || (n.Cooldown >= 0 && now.Sub(last) < n.Cooldown)) {
		return false, nil
	}
	send := n.Send
	if send == nil {
		send = osaNotify
	}
	if err := send(ctx, "Shuttle: "+event.Agent, body); err != nil {
		return false, err
	}
	if kind == notifyRecovered {
		for k := range state {
			if strings.HasPrefix(k, prefix) {
				delete(state, k)
			}
		}
	}
	state[key] = now
	data, err = json.MarshalIndent(state, "", "  ")
	if err != nil {
		return true, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(n.StatePath), ".notify-*")
	if err != nil {
		return true, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return true, err
	}
	if err := tmp.Close(); err != nil {
		return true, err
	}
	return true, os.Rename(tmp.Name(), n.StatePath)
}

// User-visible text is argv data, never interpolated into AppleScript source.
func osaNotify(ctx context.Context, title, body string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/osascript", "-e", `on run argv
display notification (item 2 of argv) with title (item 1 of argv)
end run`, title, body)
	cmd.WaitDelay = time.Second
	return cmd.Run()
}
