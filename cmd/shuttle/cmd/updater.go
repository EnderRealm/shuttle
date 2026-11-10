package cmd

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/launchd"
	"shuttle/internal/ledger"
	"shuttle/internal/updater"
)

// reexecDelay lets the updater that spawned the helper finish its cycle,
// which releases the lock and the ledger, before its job is replaced.
const reexecDelay = 2 * time.Second

var updaterCmd = &cobra.Command{
	Use:   "updater",
	Short: "Install new releases without killing what the daemon runs",
	Long: `Poll the latest release and install a newer one, draining first
(docs/releasing.md).

  shuttle updater daemon   stay running; launchd runs this
  shuttle updater once     one check and, if a release is due, one install

Every step, decision and refusal is written to updater.log under the state root.`,
}

// runUpdater runs fn against the host's updater, logging to updater.log and
// to any extra writers. The interrupt context is what lets a drain end with
// its pause lifted when the job is stopped.
func runUpdater(fn func(*updater.Updater, context.Context) error, extra ...io.Writer) error {
	binary, err := resolvedExecutable()
	if err != nil {
		return err
	}
	home, err := config.Home()
	if err != nil {
		return err
	}
	if err := ledger.EnsureHome(home); err != nil {
		return err
	}
	logFile, err := updater.OpenLog(home)
	if err != nil {
		return err
	}
	defer logFile.Close()
	u, err := updater.New(binary, io.MultiWriter(append(extra, logFile)...))
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return fn(u, ctx)
}

var updaterDaemonCmd = &cobra.Command{
	Use:           "daemon",
	Short:         "Run the updater; launchd runs this",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(*cobra.Command, []string) error {
		return runUpdater((*updater.Updater).Daemon)
	},
}

var updaterOnceCmd = &cobra.Command{
	Use:           "once",
	Short:         "Run one update check and exit",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return runUpdater((*updater.Updater).Tick, cmd.ErrOrStderr())
	},
}

// updaterRebootstrapCmd replaces the daemon and watchdog jobs under this
// binary and leaves the updater's alone: the updater runs it, and replacing
// its own job would end the cycle that is waiting on the result.
var updaterRebootstrapCmd = &cobra.Command{
	Use:           "rebootstrap",
	Short:         "Replace the daemon and watchdog jobs under this binary (internal)",
	Hidden:        true,
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(*cobra.Command, []string) error {
		return rebootstrapDaemon()
	},
}

// rebootstrapDaemon is bootout and bootstrap of the daemon and watchdog jobs
// under the running binary, which is what restarts the daemon: see
// launchdServices.Restart for why nothing lighter does.
func rebootstrapDaemon() error {
	specs, err := launchSpecs(func(s launchd.Spec) bool { return !isUpdaterSpec(s) })
	if err != nil {
		return err
	}
	return bootstrapSpecs(specs)
}

// updaterReexecCmd is the detached helper that replaces the updater's own job
// once a cycle has finished with it. Its output is redirected to updater.log
// by the spawning process, and it is the only process left to report whether
// the job came back.
var updaterReexecCmd = &cobra.Command{
	Use:           "reexec",
	Short:         "Replace the updater's own job under this binary (internal)",
	Hidden:        true,
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		time.Sleep(reexecDelay)
		return installUpdater(cmd)
	},
}

func init() {
	updaterCmd.AddCommand(updaterDaemonCmd, updaterOnceCmd, updaterRebootstrapCmd, updaterReexecCmd)
	rootCmd.AddCommand(updaterCmd)
}
