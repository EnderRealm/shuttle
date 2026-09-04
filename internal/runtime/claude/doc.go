// Package claude owns the claude runtime: the argv builder for a bare,
// api_key-authenticated headless run, the stream-json event parser that
// reads stdout as it arrives, and process control including timeout and
// process-group kill.
//
// Nothing in this package is an OS boundary. The settings.json permission
// document and the deny list are application-level controls the CLI applies to
// itself, so a launch with no Options.Launcher holds the invoking user's
// filesystem and network access. Options.Launcher is the seam the §11 sandbox
// plugs into: the argv is built behind whatever prefix the caller passes, and
// the process control stays here. The prefix is an argv prefix and nothing
// more, so it cannot express §11's own shape — srt -s <run>/srt.json -c
// <run>/run.sh, whose -c takes one quotable string; writing Argv's slice to
// run.sh (§7.4) and pointing srt at it is the sandbox ticket's, not this
// package's. Choosing that prefix, and generating the profile it names, belong
// to the caller — as does classifying the launcher's own failure: a launcher
// that refuses to start exits non-zero with no result line, which this package
// cannot tell from the model binary dying and so classifies as died rather
// than §7.6's sandbox class.
package claude
