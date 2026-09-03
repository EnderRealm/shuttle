package cmd

import (
	"database/sql"
	"fmt"

	"github.com/spf13/cobra"
)

var showCmd = &cobra.Command{
	Use:   "show <run-id>",
	Short: "Print one run's ledger row",
	Args:  cobra.ExactArgs(1),
	// The ledger errors name the run id; cobra printing them a second time,
	// with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openLedger()
		if err != nil {
			return err
		}
		defer store.Close()
		r, err := store.Get(args[0])
		if err != nil {
			return err
		}
		fields := []struct {
			name  string
			value string
		}{
			{"id", escapeControl(r.ID)},
			{"agent", escapeControl(r.Agent)},
			{"trigger", escapeControl(r.TriggerKind)},
			{"scheduled_for", escapeControl(r.ScheduledFor)},
			{"claimed_at", escapeControl(r.ClaimedAt)},
			{"started_at", nullOr(r.StartedAt)},
			{"ended_at", nullOr(r.EndedAt)},
			{"status", escapeControl(r.Status)},
			{"skip_reason", nullOr(r.SkipReason)},
			{"failure_class", nullOr(r.FailureClass)},
			{"error_signature", nullOr(r.ErrorSignature)},
			{"pid", nullOrInt(r.PID)},
			{"run_dir", nullOr(r.RunDir)},
			{"session_id", nullOr(r.SessionID)},
			{"num_turns", nullOrInt(r.NumTurns)},
			{"cost", formatCost(r.CostUSD)},
			{"input_tokens", nullOrInt(r.InputTokens)},
			{"output_tokens", nullOrInt(r.OutputTokens)},
			{"silent", fmt.Sprintf("%t", r.Silent)},
			{"outputs_applied", nullOr(r.OutputsApplied)},
			{"delivery_status", nullOr(r.DeliveryStatus)},
			{"delivery_error", nullOr(r.DeliveryError)},
			{"delivered_at", nullOr(r.DeliveredAt)},
		}
		for _, f := range fields {
			fmt.Printf("%-16s %s\n", f.name, f.value)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(showCmd)
}

func nullOrInt(v sql.NullInt64) string {
	if !v.Valid {
		return "-"
	}
	return fmt.Sprintf("%d", v.Int64)
}
