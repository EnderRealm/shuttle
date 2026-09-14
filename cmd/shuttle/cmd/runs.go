package cmd

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/runner"
)

var (
	runsAgent string
	runsLimit int
)

var runsCmd = &cobra.Command{
	Use:   "runs",
	Short: "Ledger rows, most recent first",
	// The ledger errors name the database and the row; cobra printing them a
	// second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openLedger()
		if err != nil {
			return err
		}
		defer store.Close()
		runs, err := store.Runs(runsAgent, runsLimit)
		if err != nil {
			return err
		}
		for _, r := range runs {
			fmt.Printf("%s\t%s\t%s\t%s\t%s\n",
				escapeControl(r.ID), escapeControl(r.Status), escapeControl(r.ScheduledFor),
				formatCost(r.CostUSD), nullOr(r.DeliveryStatus))
		}
		return nil
	},
}

func init() {
	runsCmd.Flags().StringVar(&runsAgent, "agent", "", "Only this agent's runs")
	runsCmd.Flags().IntVarP(&runsLimit, "limit", "n", 20, "Rows to print")
	rootCmd.AddCommand(runsCmd)
}

// openLedger opens ~/.shuttle/runs.db. The caller closes it.
func openLedger() (*ledger.Store, error) {
	path, err := config.LedgerPath()
	if err != nil {
		return nil, err
	}
	cfg, err := config.Load()
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if cfg == nil {
		cfg = &config.Config{}
	}
	store, err := ledger.Open(path)
	if err != nil {
		return nil, err
	}
	runner.ConfigureOutcomes(store, cfg, filepath.Dir(path), log.Default())
	return store, nil
}

// formatCost applies §12: two decimals, and a run that reported no cost prints
// as unpriced. weft's distinction — unpriced is not free, and $0.00 would claim
// the run was.
func formatCost(cost sql.NullFloat64) string {
	if !cost.Valid {
		return "unpriced"
	}
	return fmt.Sprintf("$%.2f", cost.Float64)
}

// nullOr renders a nullable column, using a dash for a column the run never set.
func nullOr(v sql.NullString) string {
	if !v.Valid {
		return "-"
	}
	return escapeControl(v.String)
}

// escapeControl renders a ledger string for a terminal. Columns such as
// error_signature, delivery_error and session_id carry text the harness did
// not author — a model runtime's stderr, an email provider's response — so a
// value can hold escape sequences that would otherwise reach the terminal, and
// tabs or newlines that would break both this command's tab-separated rows and
// `show`'s aligned columns. strconv.Quote escapes every control character and
// leaves printable text, including UTF-8, alone; the quotes it wraps the value
// in are dropped because the columns are not quoted otherwise.
func escapeControl(v string) string {
	q := strconv.Quote(v)
	return q[1 : len(q)-1]
}
