package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// Class is the §7.6 failure class a provider failure is recorded under. It is
// the same string as the ledger's, kept here so a provider package needs
// nothing from the ledger to name its own class.
const Class = "provider"

// Env is the harness state a provider reads. Every path is resolved from
// config.json (§4.2) rather than looked up, and the logger is the run's, so a
// skipped line lands in the run's stderr log rather than the daemon's.
type Env struct {
	LoomHome   string
	TKConfig   string
	LoomBin    string
	ReportsDir string
	Logger     *log.Logger
}

// Func is one provider. The window is resolved before the call: a provider
// never sees the clock, only the span it must report (§8.1).
type Func func(ctx context.Context, env Env, args map[string]any, w Window) (json.RawMessage, error)

// Document is the §8 envelope every provider's file carries, so the prompt can
// describe them all the same way.
type Document struct {
	Provider    string          `json:"provider"`
	Window      Window          `json:"window"`
	GeneratedAt time.Time       `json:"generated_at"`
	Data        json.RawMessage `json:"data"`
}

// Error names the provider that failed. The runner wraps every provider failure
// in it so the ledger's failure_class and the operator's message both say which
// input went wrong (§7.1 step 3).
type Error struct {
	Provider string
	Err      error
}

func (e *Error) Error() string { return fmt.Sprintf("provider %s: %v", e.Provider, e.Err) }

func (e *Error) Unwrap() error { return e.Err }
