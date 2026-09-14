package runner

import (
	"context"
	"log"
	"path/filepath"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/deliver"
	"shuttle/internal/ledger"
)

// ConfigureOutcomes attaches the configured notification policy to all terminal
// writes through this store, including FailEvent and CompleteEvent. A notification
// failure is logged after commit; it cannot turn a completed run into a failure.
// Call once before sharing the store between scheduled/manual run workers.
func ConfigureOutcomes(store *ledger.Store, cfg *config.Config, home string, logger *log.Logger) *deliver.Notifier {
	notifier := &deliver.Notifier{
		StatePath: filepath.Join(home, "notify.state"),
		Enabled:   cfg.Notify.Enabled,
		Cooldown:  time.Duration(cfg.Notify.CooldownMinutes) * time.Minute,
	}
	if logger == nil {
		logger = log.Default()
	}
	store.OnTerminal = func(event ledger.Event) {
		emitted, err := notifier.Notify(context.Background(), event)
		logger.Printf("run=%s agent=%s notification=%t incident=%d pause=%s recovery=%t error=%v", event.RunID, event.Agent, emitted, event.IncidentID, event.PausedReason, event.Recovered, err)
	}
	return notifier
}
