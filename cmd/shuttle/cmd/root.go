package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"shuttle/internal/version"
)

var helpText = `shuttle - scheduled, sandboxed, ledgered unattended agent runs

Usage: shuttle <command> [args]

Implemented: 'version', 'agents', 'daemon', 'status', 'runs', 'show', 'ack',
'preflight', 'pause', 'resume', 'install', 'uninstall', 'deliver', 'reconcile',
'updater', 'weft', 'observer' and 'run'.
Manual and daemon claims execute the same retained, sandboxed pipeline; --dry-run
stops after inputs and prompt. The rest is the phase 1 surface (docs/phase1-design.md §12) that the
remaining phase 1 tickets fill in.

Daemon:
  daemon                       The scheduler; launchd runs this

Runs:
  run <agent> [--slot D] [--dry-run] [--no-deliver] [--unsafe-no-sandbox]
  runs [--agent A] [-n 20]     Ledger rows
  show <run-id>                One run's ledger row
  reconcile <run-id>           Resume a failed observer run's notebook and
                                 ticket actions from their recorded stage
  agents                       Load and validate every definition
  preflight                    The per-run checks without a run

Operations:
  status [--json]              Daemon, agents, next slot, last outcome, streak,
                                 paused, open incidents, today's spend; --json
                                 prints them as one JSON object
  ack <incident-id>            Acknowledge an open incident
  pause <agent> | resume <agent>
  prune --older 30d            Drop run directories past the retention window
  deliver --test               One test email through the configured sender

Observer notebooks:
  observer register|activate [payload.json] --request-id ID
  observer retire <entry-id> --reason TEXT --request-id ID
  observer reopen <entry-id> --condition NAME --request-id ID
                               Hand a question to a notebook Shuttle holds,
                                 through the running daemon's observer door

Hosted weft:
  weft bind <origin-url>       Provision a hosted checkout, bind it in
                                 config.json, restart the daemon and report
                                 the project from /v2/projects
  weft unbind <project>        Remove a binding the same way; the checkout
                                 stays on disk

Lifecycle:
  install [daemon|updater]     Create the state root and the reports repo;
                                 'daemon' also installs the launchd jobs,
                                 'updater' the updater's alone
  uninstall                    Remove the updater, watchdog and daemon jobs
  updater daemon | once        Install new releases: the launchd job, or one
                                 check by hand (docs/releasing.md)
  version                      Print the version string

Global flags:
  -h, --help                   Show help

Environment:
  SHUTTLE_HOME          state root (default: ~/.shuttle)
  SHUTTLE_AGENTS_DIR    agent definitions (default: ~/code/shuttle/agents)
  SHUTTLE_OBSERVER_SOCKET
                        the observer door
                        (default: /Users/Shared/weft/observer/door.sock)
`

var rootCmd = &cobra.Command{
	Use:     "shuttle",
	Short:   "Scheduled, sandboxed, ledgered unattended agent runs",
	Long:    helpText,
	Version: version.String(),
}

func init() {
	// Override only the root's help so subcommands keep cobra's built-in
	// auto-generated help (which respects subcommand flags and Long text).
	defaultHelp := rootCmd.HelpFunc()
	rootCmd.SetHelpFunc(func(c *cobra.Command, args []string) {
		if c == rootCmd {
			fmt.Println(helpText)
			return
		}
		defaultHelp(c, args)
	})
}

// quietError is a failure whose text the log already carries: the command
// still exits non-zero, but Execute does not print it again.
type quietError struct{ error }

// reportError prints what Execute prints for a failed command.
func reportError(w io.Writer, err error) {
	var quiet quietError
	if errors.As(err, &quiet) {
		return
	}
	fmt.Fprintln(w, err)
}

// exitError ends the process with code rather than 1: the observer commands
// surface each refusal class under its own exit (docs/observer-contract.md §8).
type exitError struct {
	code int
	error
}

func (e exitError) Unwrap() error { return e.error }

// exitCode is the status Execute exits with for a failed command.
func exitCode(err error) int {
	var exit exitError
	if errors.As(err, &exit) {
		return exit.code
	}
	return 1
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		reportError(os.Stderr, err)
		os.Exit(exitCode(err))
	}
}
