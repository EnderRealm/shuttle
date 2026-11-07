package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"shuttle/internal/version"
)

var helpText = `shuttle - scheduled, sandboxed, ledgered unattended agent runs

Usage: shuttle <command> [args]

Implemented: 'version', 'agents', 'daemon', 'status', 'runs', 'show', 'ack',
'preflight', 'pause', 'resume', 'install', 'uninstall', 'deliver', 'reconcile',
'updater' and 'run'.
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
  status                       Agents, next slot, last outcome, streak, paused,
                                 open incidents, today's spend
  ack <incident-id>            Acknowledge an open incident
  pause <agent> | resume <agent>
  prune --older 30d            Drop run directories past the retention window
  deliver --test               One test email through the configured sender

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

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
