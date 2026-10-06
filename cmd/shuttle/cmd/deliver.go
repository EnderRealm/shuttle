package cmd

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/deliver"
	"shuttle/internal/ledger"
	"shuttle/internal/output"
)

// deliverTestAgent names the synthetic sender in X-Shuttle-Agent and the run
// id, so a test message cannot be mistaken for an agent's report.
const deliverTestAgent = "deliver-test"

var deliverTest bool

// newDeliverClient is the §10.1 client the command sends through; a test
// points it at a local server.
var newDeliverClient = deliver.NewCloudflare

var deliverCmd = &cobra.Command{
	Use:   "deliver --test",
	Short: "Send one test email through the configured sender",
	Long: `Send one test email through the §10.1 Cloudflare client, from the configured
sender to the configured recipients (docs/phase1-design.md §12).

The message carries X-Shuttle-Run and X-Shuttle-Agent like a report does, with a
synthetic run id under the agent name "deliver-test". No ledger row is written.
This is the check that the onboarded domain, the verified destination and the
Keychain token work together before the first scheduled delivery.`,
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !deliverTest {
			return errors.New("deliver: --test is the only delivery this command makes")
		}
		cfg, err := config.Load()
		if err != nil {
			return err
		}
		now := time.Now()
		runID, err := ledger.NewRunID(deliverTestAgent, now)
		if err != nil {
			return err
		}
		text := fmt.Sprintf("# Shuttle delivery test\n\nSent by `shuttle deliver --test` at %s as run %s.\nNo agent produced it and the ledger records nothing.\n",
			now.Format(time.RFC3339), runID)
		html, err := output.Render([]byte(text), nil)
		if err != nil {
			return err
		}
		receipt, err := newDeliverClient(cfg).Send(cmd.Context(), deliver.Message{
			RunID: runID, Agent: deliverTestAgent, Subject: "Shuttle delivery test " + runID,
			Text: []byte(text), HTML: html,
		})
		out := cmd.OutOrStdout()
		fmt.Fprintf(out, "%s\thttp %d\tattempts %d\n", runID, receipt.Status, receipt.Attempts)
		for _, line := range []struct {
			label string
			addrs []string
		}{{"delivered", receipt.Delivered}, {"queued", receipt.Queued}, {"bounced", receipt.Bounced}} {
			if len(line.addrs) > 0 {
				fmt.Fprintf(out, "%s\t%s\n", line.label, strings.Join(line.addrs, ", "))
			}
		}
		return err
	},
}

func init() {
	deliverCmd.Flags().BoolVar(&deliverTest, "test", false, "Send one test email (§10.1)")
	rootCmd.AddCommand(deliverCmd)
}
