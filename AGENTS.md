# AGENTS.md

## Project Overview

<!-- Describe your project here -->

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
