// Package claude owns the claude runtime: the argv builder for a bare,
// api_key-authenticated headless run, the stream-json event parser that
// reads stdout as it arrives, and process control including timeout and
// process-group kill.
package claude
