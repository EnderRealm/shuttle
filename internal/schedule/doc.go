// Package schedule owns slot maths: cron triggers evaluated as literal
// wall clock in a named timezone, per-agent deterministic jitter,
// catch-up-one after the machine slept, and the overlap rule of one run
// per agent at a time with no queue.
package schedule
