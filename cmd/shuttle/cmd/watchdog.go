package cmd

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/launchd"
)

const watchdogInterval = 5 * time.Second

var (
	watchdogProcess   = launchd.ProcessFor
	watchdogKickstart = launchd.Kickstart
)

var watchdogCmd = &cobra.Command{
	Use:           "watchdog",
	Short:         "Keep the launchd scheduler process running",
	Hidden:        true,
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New(cmd.ErrOrStderr(), "", log.LstdFlags)
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ticker := time.NewTicker(watchdogInterval)
		defer ticker.Stop()
		state := &watchState{}
		for {
			watchDaemonOnce(logger, state)
			select {
			case <-ctx.Done():
				logger.Print("watchdog stop")
				return nil
			case <-ticker.C:
			}
		}
	},
}

// watchState is what the watchdog carries from one tick to the next.
type watchState struct {
	// failing is set once a tick has found the daemon down on a recorded
	// start failure, so that its coming up is logged once.
	failing bool
}

// watchDaemonOnce kickstarts a processless daemon. A daemon down on a recorded
// start failure is kickstarted without a line: the daemon printed that failure
// when it first hit it and prints it again only when it changes, so daemon.log
// carries one line per distinct failure and one more when the daemon is up.
func watchDaemonOnce(logger *log.Logger, state *watchState) {
	_, running, err := watchdogProcess(launchd.DaemonLabel)
	if err != nil {
		logger.Printf("watchdog: inspect %s: %v", launchd.DaemonLabel, err)
		return
	}
	_, failed := startFailure()
	if running {
		// A record still standing is a daemon still starting: it clears the
		// record once its loop is running.
		if state.failing && !failed {
			logger.Printf("watchdog: %s is up after a start failure", launchd.DaemonLabel)
			state.failing = false
		}
		return
	}
	if err := watchdogKickstart(launchd.DaemonLabel); err != nil {
		logger.Printf("watchdog: kickstart %s: %v", launchd.DaemonLabel, err)
		return
	}
	if failed {
		state.failing = true
		return
	}
	logger.Printf("watchdog: kickstarted %s", launchd.DaemonLabel)
}

func init() {
	rootCmd.AddCommand(watchdogCmd)
}
