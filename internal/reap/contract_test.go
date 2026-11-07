package reap

import (
	"path/filepath"
	"testing"

	"shuttle/internal/runtime/claude"
)

// The settings argument is the pass's identity condition, so these two
// constants are a contract with the runtime rather than local strings: an argv
// that stopped carrying --settings <run_dir>/settings.json would leave every
// live run's process unidentifiable, and the pass would close its row without
// killing anything.
func TestSettingsArgumentIsTheRuntimeArgvs(t *testing.T) {
	dir := t.TempDir()
	o := claude.Options{
		Bin:            filepath.Join(dir, "claude"),
		RunDir:         dir,
		Auth:           claude.AuthAPIKey,
		Model:          "haiku",
		MaxTurns:       1,
		MaxBudgetUSD:   0.05,
		Tools:          []string{"Read"},
		TimeoutSeconds: 30,
		APIKey:         "stub-key",
		Prompt:         "summarise the day",
		PATH:           "/usr/bin:/bin",
		HomeDir:        dir,
		Timezone:       "America/Los_Angeles",
	}
	if got, want := o.SettingsPath(), filepath.Join(dir, settingsFile); got != want {
		t.Fatalf("settings path = %s, want %s", got, want)
	}
	argv, err := claude.Argv(o)
	if err != nil {
		t.Fatalf("argv: %v", err)
	}
	// Adjacent, which is how runArgv matches it: a flag and its value.
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == settingsFlag && argv[i+1] == o.SettingsPath() {
			return
		}
	}
	t.Fatalf("argv = %q, want the adjacent pair %s %s", argv, settingsFlag, o.SettingsPath())
}
