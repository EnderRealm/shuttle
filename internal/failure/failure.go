// Package failure holds the values the ledger's failure_class column takes. It
// imports nothing so a producer can report a class without depending on the
// ledger that records it.
package failure

// Failure classes, §7.6. Nothing is ever classified as "flake".
const (
	Preflight = "preflight"
	Auth      = "auth"
	Provider  = "provider"
	Died      = "died"
	NoResult  = "no_result"
	Stuck     = "stuck"
	Budget    = "budget"
	Output    = "output"
	Sandbox   = "sandbox"
)

// Weft's additions to the §7.6 set.
const (
	Protocol = "protocol"
	Unknown  = "unknown"
)
