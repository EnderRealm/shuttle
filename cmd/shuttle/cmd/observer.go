package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	observercontract "shuttle/internal/observer"
)

// observerCallTimeout bounds one call on the door. The door applies the call
// under the notebook's lock and commits it, retrying a moved revision, and
// each git call there is bounded by a minute.
const observerCallTimeout = 5 * time.Minute

var observerCmd = &cobra.Command{
	Use:   "observer",
	Short: "Register, activate, retire and reopen questions through the running Shuttle",
	Long: `Hand a question, an experiment, a retirement or a reopen to the notebook
Shuttle holds for an observer (docs/observer-contract.md §12). The call goes to
the running daemon over its observer door, $SHUTTLE_OBSERVER_SOCKET or
/Users/Shared/weft/observer/door.sock; the daemon validates it, applies it at
the notebook's current revision and answers with the entry's id and state. No
file under the state root is read here.

  shuttle observer register [question.json] --request-id ID
  shuttle observer activate [experiment.json] --request-id ID
  shuttle observer retire <entry-id> --reason TEXT --request-id ID
  shuttle observer reopen <entry-id> --condition NAME --request-id ID

A payload is an observer/v1 object, read from the file named or from stdin. The
request id makes a repeat safe: the same call again returns the original
answer, and a changed one under the same id is refused. A refusal exits with
its §8 status: 10 stale revision, 11 retired, 12 no entry, 14 request id
reused with changed content. Any other failure exits 1 naming the field.`,
}

var observerRegisterCmd = &cobra.Command{
	Use:           "register [question.json]",
	Short:         "Register a standing question",
	Args:          cobra.MaximumNArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return observerPayloadCall(cmd, args, observercontract.DoorRegister)
	},
}

var observerActivateCmd = &cobra.Command{
	Use:           "activate [experiment.json]",
	Short:         "Attach an experiment to a question, pending or live as it declares",
	Args:          cobra.MaximumNArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return observerPayloadCall(cmd, args, observercontract.DoorActivate)
	},
}

var observerRetireCmd = &cobra.Command{
	Use:           "retire <entry-id> --reason TEXT",
	Short:         "Take a question off the list, reporting the actions already in flight",
	Args:          cobra.ExactArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		reason, _ := cmd.Flags().GetString("reason")
		return observerCall(cmd, observercontract.DoorCall{
			Operation: observercontract.DoorRetire, EntryID: args[0], Reason: reason,
		})
	},
}

var observerReopenCmd = &cobra.Command{
	Use:           "reopen <entry-id> --condition NAME",
	Short:         "Return an answered question to observing under a named condition",
	Args:          cobra.ExactArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		condition, _ := cmd.Flags().GetString("condition")
		return observerCall(cmd, observercontract.DoorCall{
			Operation: observercontract.DoorReopen, EntryID: args[0], Condition: condition,
		})
	},
}

func observerPayloadCall(cmd *cobra.Command, args []string, operation string) error {
	var payload []byte
	var err error
	if len(args) == 1 && args[0] != "-" {
		payload, err = os.ReadFile(args[0])
	} else {
		payload, err = io.ReadAll(cmd.InOrStdin())
	}
	if err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	// The door validates the payload; it only has to travel as JSON.
	if !json.Valid(payload) {
		return errors.New("payload: not a JSON value")
	}
	return observerCall(cmd, observercontract.DoorCall{Operation: operation, Payload: payload})
}

func observerCall(cmd *cobra.Command, call observercontract.DoorCall) error {
	call.Request.RequestID, _ = cmd.Flags().GetString("request-id")
	ctx, cancel := context.WithTimeout(cmd.Context(), observerCallTimeout)
	defer cancel()
	resp, err := observercontract.CallDoor(ctx, config.ObserverSocket(), call)
	if err != nil {
		return err
	}
	switch resp.Status {
	case observercontract.DoorOK:
		fmt.Fprintln(cmd.OutOrStdout(), string(resp.Answer))
		return nil
	case observercontract.DoorRefused:
		return exitError{code: resp.Exit, error: errors.New(resp.Error)}
	default:
		return errors.New(resp.Error)
	}
}

func init() {
	for _, sub := range []*cobra.Command{observerRegisterCmd, observerActivateCmd, observerRetireCmd, observerReopenCmd} {
		sub.Flags().String("request-id", "", "The id a repeat of this call is answered under")
		observerCmd.AddCommand(sub)
	}
	observerRetireCmd.Flags().String("reason", "", "Why the question comes off the list")
	observerReopenCmd.Flags().String("condition", "", "re-activated, method-revised or contradicted")
	rootCmd.AddCommand(observerCmd)
}
