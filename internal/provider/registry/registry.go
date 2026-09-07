// Package registry names the six input providers (§8). It is the only place
// that knows which package answers which manifest name, so the runner resolves
// an input without importing every provider.
package registry

import (
	"shuttle/internal/provider"
	"shuttle/internal/provider/gitlog"
	"shuttle/internal/provider/loomsessions"
	"shuttle/internal/provider/loomworkreport"
	"shuttle/internal/provider/priorreports"
	"shuttle/internal/provider/tkjournal"
	"shuttle/internal/provider/tktickets"
)

// Providers is keyed by the manifest's inputs[].provider. agentdef validates a
// manifest against the same set (AllowedProviders), so a name that loads is a
// name that runs.
var Providers = map[string]provider.Func{
	"loom-sessions":   loomsessions.Run,
	"loom-workreport": loomworkreport.Run,
	"tk-journal":      tkjournal.Run,
	"tk-tickets":      tktickets.Run,
	"git-log":         gitlog.Run,
	"prior-reports":   priorreports.Run,
}
