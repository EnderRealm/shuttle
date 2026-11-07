# shuttle

A Go harness being built for scheduled, sandboxed, budgeted agent runs. Its first
target is a daily report across the coding-agent estate, assembled from loom, tk,
git history, and prior reports, then delivered by email.

## Current implementation

Agent validation, cron slot scheduling, the SQLite ledger, inspection commands,
restart reaping, prompt dry runs, and the live execution pipeline are implemented.
Manual and daemon claims use the same preflight, provider, sandboxed Claude,
classification, output-validation, and application path; every attempt retains
its inputs, prompt, transcript, result, and applied-output record.

When `config.json` contains a `weft` block, the installed daemon also hosts the
private weft service on the configured loopback or Tailscale address. It speaks
`weft-service/v2` (weft's `docs/service-v2-contract.md`) as a developmental
host: discovery, the full catalog of every registered project and ticket with
eligibility, the dashboard, run history, questions and the state stream, with
not-served capabilities answering 501. The catalog reads tk through
`pkg/ticket`, refuses a partial tk read rather than serving it, and rereads the
store every five seconds so edits made outside Shuttle reach clients as catalog
events. Launches, frontier selections, answers and resumes enter one durable
admission queue under a host-wide cap (default 3, `PUT /v2/queue/capacity`);
dispatch takes the repository reservation and a slot, mints the run, and
spawns a `weft-remote/v1` worker segment through the supervisor. Waiting work
survives clients closing and a daemon restart; `shuttle pause weft-worker`
pauses dispatch without refusing admission. The daemon log names the host,
the SHA-256 of each idempotency key, and every entry and run a request or
dispatch touched. `weft-remote/v1` routes keep serving reads; every v1
mutation answers 503 `host_paused`, though a recorded v1 idempotency key still
replays.
Opening an existing `runs.db` migrates it in place (§14.1 of that contract):
back it up first, as `runs.db.v1-<stamp>`.

Email delivery is still planned; a valid report remains pending for the sender
ticket, while `--no-deliver` records an intentional skip without losing the
retained output. `install daemon` installs the KeepAlive launchd job, a
companion watchdog that recovers macOS 26's deferred respawns, and the updater
that installs new releases; `uninstall` removes all three while preserving
state. `preflight [agent]` checks declared local
requirements without a model call, and `resume <agent>` clears a pause and its
failure counter. `pause` prevents new claims until that resume; `prune` and
`deliver` appear in the design/help text but are not implemented.

## Build and check

Requires Go 1.26. macOS is the target for Keychain, process-identity checks, and
the planned launchd integration. Git is needed to initialize reports and collect
repository history.

```sh
make dev
./shuttle version
./shuttle run --help
go test ./...
go vet ./...
```

`make dev` builds `./shuttle`. `make install` builds the binary into
`~/.local/bin`; run `shuttle install daemon` separately to install the service.
Once the updater runs, that binary is a dev build and the next poll replaces it
with the latest release ([docs/releasing.md](docs/releasing.md)).
The daemon environment is persisted to `~/.shuttle/daemon-env` and contains
only `SHUTTLE_HOME`, `SHUTTLE_AGENTS_DIR`, and the baked `PATH`; credentials
remain in Keychain. Default tests exercise local fixtures, temporary stores, and
stub processes. `SHUTTLE_LIVE_CLAUDE=1` enables a paid model test and
`SHUTTLE_LIVE_SRT=1` enables real sandbox tests; leave both unset for ordinary
checks.

## Configure and preview a daily run

From the repository root, select this checkout's agent definitions and initialize
the state directory:

```sh
export SHUTTLE_AGENTS_DIR="$PWD/agents"
./shuttle agents
./shuttle install
```

State defaults to `~/.shuttle`; set `SHUTTLE_HOME` to use another directory.
`install` creates that directory and a local `reports/` git repository with no
remote. It does not write `config.json` or configure credentials.

Create `config.json` in the state directory before a dry run. This minimal example
supplies the daily providers' paths; replace each absolute path with your own:

```json
{
  "paths": {
    "loom_home": "/Users/you/.loom",
    "loom_bin": "/Users/you/.local/bin/loom",
    "tk_config": "/Users/you/.ticket/config.yaml"
  }
}
```

The daily inputs need the existing loom data and a loom executable supporting
`work-report`, plus the tk configuration/store and its registered repositories.
The full configuration shape, including credential **Keychain item names**, is in
[the phase-one design](docs/phase1-design.md). Unknown config keys are rejected.
Agent lookup uses `SHUTTLE_AGENTS_DIR`, then `config.json`'s `agents_dir`, then
`~/code/shuttle/agents`.

Hosted weft runs add this block; the bearer token and the Claude OAuth token
are Keychain generic passwords and never appear in the file or launchd plist:

```json
{
  "weft": {
    "listen": "100.101.102.103:7431",
    "keychain_item": "weft-remote-token",
    "oauth_keychain_item": "weft-claude-oauth-token",
    "worker_bin": "/Users/you/.local/bin/weft-worker",
    "weft_root": "/Users/you/.shuttle/weft",
    "segment_timeout_seconds": 21600,
    "repositories": {"shuttle": "/Users/Shared/weft/repos/shuttle"},
    "agent_users": ["weftagent1", "weftagent2", "weftagent3"]
  }
}
```

`agent_users` are the hosted slots' macOS users, provisioned by
`scripts/agent-users/setup.sh` (`docs/agent-users.md`). Each live segment runs
as one of them, never shared: its worker is launched with `--agent-user <user>
--worktree-root /Users/Shared/weft/worktrees`, and every new run's worktree is
under that root. A user returns to the pool only once its segment has settled,
which waits until `sudo -n -u <user> pkill -KILL -U <user>` has left no process
of the user but the services launchd respawns in its session; a stop, a timeout
and a daemon restart wait the same way. A duplicate, a name that is not an
account name, the operator, a member of `admin` or `staff`, or an account whose
groups cannot be read refuses the load, naming the entry, as does a second name
with an earlier entry's uid. So does a `weft_root` at or inside
`/Users/Shared/weft/worktrees` or `/Users/Shared/weft/repos`, which agent users
can write. The
capacity is the ledger's, set with `PUT /v2/queue/capacity`: unset, it is three
or the number of agent users, whichever is fewer; the route refuses one above
that number, and a stored one above it stops the service from starting.

The `sandbox` block this key replaced is an unknown key now, which refuses the
load: when deploying a shuttle that runs segments as agent users, remove
`weft.sandbox` from `config.json` and add `weft.agent_users`. A run whose
worktree is under `weft_root`, launched before, is not answered or resumed: its
answer or resume fails its preflight, and only its cleanup, archive and
reconcile still run.

`oauth_keychain_item` (default `weft-claude-oauth-token`) names a login-Keychain
generic password holding a long-lived token from `claude setup-token`; the daemon
reads it at each launch and hands it to the worker as `CLAUDE_CODE_OAUTH_TOKEN`,
which reaches the agent user through the sudoers `env_keep`, because an agent
user cannot read the Keychain where the CLI keeps the subscription credential.
It is never in argv. The daemon refuses a listener outside loopback or
`100.64.0.0/10`, and refuses each segment before spawning the worker if that item
is missing or the worker does not list the `agent_user` capability.

`repositories` binds each hosted tk project to its checkout. A binding that is
not a path directly under `/Users/Shared/weft/repos`, where agent users can
reach it, or that is under the operator's home, refuses the load, naming the
path. Load checks the spelling only: agents can write `repos/`, so what is on
disk there never stops shuttle starting; the catalog checks each checkout on
every read and reports a missing, symlinked or foreign one unavailable. tk's `config.yaml` still
decides which projects exist and holds their tickets, but its paths are never
used: a tk project with no binding here is unavailable (`no_binding`), and
project terminals open in the bound checkout. The daemon reads `config.json`
at start, so restart it after adding a binding.
`scripts/agent-users/provision-checkout.sh <origin-url>` clones and protects
the checkout and prints the line to add (`docs/agent-users.md`). Nothing
updates a hosted checkout from origin yet: after pushing from elsewhere, run
`git -C /Users/Shared/weft/repos/<repo> pull --ff-only` before launching.

```sh
./shuttle run daily --dry-run
```

A dry run gathers real local inputs and writes `run.json`, `inputs/*.json`, and
`prompt.md` under the printed `runs/daily/<run-id>/` directory. It calls no model,
sends no email, and writes no ledger row. `--slot YYYY-MM-DD` selects a past slot
in the agent's trigger timezone; the report covers the preceding day. See the
[daily manifest](agents/daily/agent.json) and [prompt](agents/daily/AGENT.md).

## Inspect and debug

```sh
./shuttle status
./shuttle runs --agent daily -n 20
./shuttle show RUN_ID
```

Replace `RUN_ID` with a row printed by `runs`. These commands inspect the ledger
at `<state-root>/runs.db`; they do not list dry runs. Dry-run failures name the
provider, and its partially written run directory contains the inputs gathered
before the failure. CLI errors and scheduler decisions go to stderr. A foreground
`daemon` does not create `daemon.log` itself; the installed launchd job redirects
both stdout and stderr there.

[The release notes](docs/releasing.md) describe how a release is cut and how the
updater installs one without killing running work. [The sandbox notes](docs/sandbox.md) describe the runtime boundary and its measured
behavior. [The agent-user notes](docs/agent-users.md) describe the macOS users hosted
agents run as, how to provision them, and what that boundary does not protect.
[The observer contract](docs/observer-contract.md) describes the shared
question, evidence, assessment, and action records. The phase-one design includes
remaining work as well as implemented components.
