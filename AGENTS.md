# AGENTS.md

## Project Overview

shuttle is the harness that runs agents unattended: a scheduler that fires cron slots,
a sandbox that wraps every model run, a ledger that records what happened and what it
cost, and delivery that gets the result off the machine. Phase 1 delivers one thing — a
nightly email summarising the previous day across the coding-agent estate — produced by
a run that is scheduled, sandboxed, budgeted, ledgered and delivered by shuttle.

The low-level design is `docs/phase1-design.md`; read it before changing the layout.

## Tickets

This project uses **tk** for task tracking — drive it via its MCP tools, never read or
edit the ticket store files directly. The model itself — statuses, types, hierarchy,
tools — comes from the `Tickets: the tk mental model` section of warp's `AGENTS.md`,
rendered into each agent's global instructions; if that section isn't in yours, go get
it rather than working from a guessed model.

## Skills

- `/capture`     — create a well-formed ticket (why + success contract).
- `/work`        — drive a ticket from open to done (implement → review loop → commit → push).
- `/brainstorm`  — refine a rough idea into a concrete design, then hand off to /capture.
- `/investigate` — disciplined debugging when the cause isn't obvious.

## Git hygiene

One ticket = one commit = one push. Commit messages reference the ticket id as
`[<ticket-id>]`. Not done until committed and pushed.

## Development Guidelines

<!-- Add your coding standards, architecture notes, etc. -->
