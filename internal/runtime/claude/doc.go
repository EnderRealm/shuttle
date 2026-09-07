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
// the process control stays here. internal/sandbox owns that prefix — srt -s
// <run>/srt.json -- — and the profile it names; choosing it belongs to the
// caller, as does classifying the launcher's own failure: a launcher that
// refuses to start exits non-zero with no result line, which this package
// cannot tell from the model binary dying and so classifies as died rather
// than §7.6's sandbox class. That is what internal/sandbox's preflight is for,
// since it runs before the launch and can say srt itself refused.
package claude
