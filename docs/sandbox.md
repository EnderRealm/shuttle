# Sandbox

Status: measured 2026-09-07 on the Mac Studio against srt 1.0.0
(`/opt/homebrew/bin/srt`, the Homebrew symlink into npm's
`@anthropic-ai/sandbox-runtime`) and claude CLI 2.1.263
(`/opt/homebrew/bin/claude`), macOS. This is the record behind
`docs/phase1-design.md` §11: what the generated profile is, what was measured
rather than assumed, and what the profile does not close. Re-measure and re-date
this document when either binary moves.

## What is wrapped, and how

Every model run is launched behind an argv prefix from `internal/sandbox`:

```
/opt/homebrew/bin/srt -s <run>/srt.json -- /opt/homebrew/bin/claude -p …
```

`--`, not `-c`. srt's `-c` takes one string, runs it through `bash -c` and
silently ignores every positional argument after it (`srt -s p -c 'echo FROM-C'
/bin/echo IGNORED` prints only `FROM-C`), so a `-c <run>/run.sh` launcher would
drop the model argv the runtime appends to the prefix. With `--`, srt
shell-quotes the argv it is given and runs that, which is verified with
arguments containing spaces and with the `--bare -p …` flag set of the time. The
empty value of `--setting-sources ''`, which replaced `--bare` (§7.4), survives
the quoting as an empty argument rather than vanishing (2026-10-07: `srt -s p --
/bin/sh -c 'echo "argc=$# [$1][$2][$3]"' x a '' b` prints `argc=3 [a][][b]`).
`run.sh` is therefore neither written nor needed, and §7.2's run directory no
longer lists it.

`internal/sandbox` supplies the prefix, the profile and the preflight; no caller
wires them yet, since `internal/runner` is still a stub and `shuttle run` claims
a slot and executes nothing until the runner lands.

`srt.json` is generated per run by `sandbox.WriteProfile` before the launch, at
mode 0600. `sandbox.Check` is the §7.5 preflight: `srt -s <run>/srt.json -c
true`, 0.14 s measured here, with the environment built from scratch. A failure
there is the run's `sandbox` failure class (§7.6) and the error carries srt's own
stderr as the evidence, because that is the only place srt says what it refused.

The runtime's environment gained a ninth variable for this: `CLAUDE_CODE_TMPDIR`
(see TMPDIR below).

## srt 1.0.0, as measured

**The profile schema is required, not advisory.** 1.0.0 validates the settings
file with zod and requires `network.allowedDomains`, `network.deniedDomains`,
`filesystem.denyRead`, `filesystem.allowWrite` and `filesystem.denyWrite`.
`allowRead`, `allowUnixSockets` and `allowLocalBinding` are optional. The §11
profile as originally designed omitted `deniedDomains` and `denyWrite`, and srt
refuses it:

```
Invalid configuration in <file>:
  - network.deniedDomains: Required
  - filesystem.denyWrite: Required
Error: Could not load settings from <file> (missing, unreadable, or invalid). Refusing to run with the default config.
```

exit 1. Truncated JSON gives `Invalid JSON in config file …` and the same
refusal; a missing file and a wrong-typed key are refused the same way. This is
the property the preflight rests on — a profile that did not reach disk intact
cannot become a permissive run — and `TestCheckRefusesAMalformedProfile` holds
it. The one hole: an **unknown** key (`"bogus": 1`) is accepted silently, exit 0,
so a misspelt optional key is not caught by srt. That is why the generated
profile has a byte-for-byte golden test rather than a shape assertion.

**Exit codes.** srt exits with the child's exit code (`exit 7` → 7). A child
that dies by `SIGTERM` or `SIGINT` makes srt exit **0** silently
(`cli.js`: `if (signal === 'SIGINT' || signal === 'SIGTERM') process.exit(0)`);
other signals print `Process killed by signal: <sig>` and exit 1. The §7.6
consequence: a model process SIGTERM'd from outside the harness reads as
`no_result`, not `died`. The harness's own timeout kill is unaffected — it sets
`stuck` before either.

**Environment.** srt is argv[0], so it receives the from-scratch environment
`claude.Environ` builds and runs fine with only `PATH` and `HOME` set. It passes
that environment through to the child and adds `SANDBOX_RUNTIME=1`, the proxy
set (`HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`/`FTP_PROXY`/`NO_PROXY` and the
lower-case, GRPC, RSYNC, Docker and CloudSDK variants, all pointing at
`localhost:<port>` with a per-command token), `GIT_CONFIG_PARAMETERS` and
`GIT_SSH_COMMAND` — and it overrides `TMPDIR`.

**TMPDIR, both sides of it.** srt sets the *child's* `TMPDIR` to
`$CLAUDE_CODE_TMPDIR || $CLAUDE_TMPDIR || /tmp/claude` (`dist/sandbox/sandbox-utils.js`:
"TMPDIR is overridden so temp-file writers land in a path the FS sandbox
allows"). With the host `TMPDIR` set to `<run>/tmp`, the child still saw
`/tmp/claude`, which does not exist on this machine, so `touch $TMPDIR/x`
failed. `CLAUDE_CODE_TMPDIR=<run>/tmp` on srt's own environment is what makes
the child see `<run>/tmp`; that is the ninth variable `claude.Environ` now
carries.

Separately, srt itself — the host-side process, outside the sandbox — creates a
unix socket `srt-mux-<pid>-<n>.sock` under the *host* `TMPDIR` it was given,
i.e. `<run>/tmp`. A path over the darwin `sun_path` limit of 104 bytes including
the NUL makes srt fail with `Error: listen EINVAL: invalid argument <path>`
before it launches anything, so `internal/sandbox` refuses a run directory for
which `<run>/tmp/srt-mux-99999-99.sock` exceeds 103 bytes, naming the limit.
Production's is well inside it:
`/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/tmp/srt-mux-12345-0.sock`
is 84 bytes. It is also why the package's srt-dependent tests build their run
directory under `/tmp` rather than with `t.TempDir`, whose `/var/folders/…` root
alone would break the limit.

## Filesystem

The generated profile, for run directory `R` and home `H`:

```json
{
  "filesystem": {
    "denyRead": [
      "H/.ssh", "H/.gnupg", "H/Library/Keychains",
      "H/.claude/.credentials.json", "H/.claude/settings.json",
      "H/.ticket", "H/.loom", "H/.shuttle",
      "H/.codex", "H/.cursor", "H/code"
    ],
    "allowRead": ["R"],
    "allowWrite": ["R/out", "R/tmp", "R/claude-home"],
    "denyWrite": ["/tmp/claude", "/private/tmp/claude", "H/.claude/debug", "H/.npm/_logs"]
  },
  "network": {
    "allowedDomains": ["api.anthropic.com"],
    "deniedDomains": [],
    "allowUnixSockets": [],
    "allowLocalBinding": false
  }
}
```

Paths are absolute rather than §11's `~`-relative form, so the document does not
depend on the `HOME` srt expands it against.
`internal/sandbox/testdata/profile.golden.json` is this profile for the worked
example, compared byte for byte.

**Reads are deny-then-allow, and permissive by default.** Verified under the
deny list above with `allowRead: [R]`: `ls ~/code` denied, `ls ~/.shuttle`
denied, the run directory readable beneath the denied `~/.shuttle` — `allowRead`
takes precedence over `denyRead` — the credentials file denied, and `ls
~/.claude` still readable, since only the two named files under it are denied.
`~/.shuttle` is denied whole rather than as `runs.db` and `config.json` alone:
because everything not named is readable, naming the two files would have left
every other run's `prompt.md`, `inputs/`, `transcript.ndjson` and `out/` under
`~/.shuttle/runs/` readable, and the ledger's `-wal` and `-shm` sidecars with
them. Denying the directory covers all of it, and the run's own `allowRead` is
what keeps this run readable. The residual is the shape of the mechanism:
**everything not named in `denyRead` is readable by the sandboxed process.** The
CLI's own `settings.json`, which confines `Read` to `<run>/**`, is the second,
application-level layer over that, and it is a control the CLI applies to itself
rather than an OS boundary.

**Writes are the exact opposite: allow-listed.** srt always permits writes to
`/dev/stdout`, `/dev/stderr`, `/dev/null`, `/dev/tty`, `/dev/dtracehelper`,
`/dev/autofs_nowait`, `/tmp/claude`, `/private/tmp/claude`, `~/.npm/_logs` and
`~/.claude/debug`, whether or not a profile names them
(`getDefaultWritePaths`): `/tmp/claude` and `~/.claude/debug` were writable
under a profile that never mentioned them. `denyWrite` takes precedence over
those defaults — listing them there makes the write `Operation not permitted` —
which is why the generated profile denies the four that are not device nodes.
Without it, "the model writes nothing outside its run directory" would be
approximate.

`SHUTTLE_LIVE_SRT=1 go test ./internal/sandbox/ -run TestLiveSRT` proves the
live half: a `touch` of `<run>/inputs/probe` — inside the run directory, outside
the three writable subdirectories — fails, and a `touch` of `<run>/out/probe`
succeeds as the positive control. On the read side it asserts `ls ~/.shuttle` is
refused, against the real home rather than the test's run directory, which sits
under `/private/tmp`; `ls <run>` and `cat <run>/srt.json` are the positive
controls. The `~/.shuttle` assertion skips itself when the directory does not
exist on the host, where the `ls` would fail for the wrong reason.

## What the CLI writes under CLAUDE_CONFIG_DIR

Measured first with `--bare`, from a run directory `R`, with the environment
built from scratch and `HOME` pointed at an empty directory so a stray write
outside the run directory would show up:

```
printf '%s\n' '{"type":"user","message":{"role":"user","content":[{"type":"text","text":"Reply with exactly the word ok."}]}}' | \
env -i PATH=/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin HOME=<empty-dir> TMPDIR=$R/tmp CLAUDE_CODE_TMPDIR=$R/tmp CLAUDE_CONFIG_DIR=$R/claude-home ANTHROPIC_API_KEY=sk-ant-bogus DISABLE_TELEMETRY=1 CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 TZ=America/Los_Angeles \
/opt/homebrew/bin/srt -s $R/srt.json -- \
/opt/homebrew/bin/claude --bare -p --input-format stream-json --output-format stream-json --verbose --model haiku --max-turns 1 --max-budget-usd 0.05 --permission-mode dontAsk --tools Read --settings $R/settings.json --no-session-persistence
```

`CLAUDE_CODE_TMPDIR` matters to the measurement, not only to the run: without it
the child's `TMPDIR` is `/tmp/claude` whatever the host's is, so an in-srt run
that omits it establishes nothing about `<run>/tmp`. `R` is under `/tmp` so the
mux socket stays inside `sun_path`, and the run is bounded by
`perl -e 'alarm 60; exec @ARGV' --` (macOS has no `timeout`); the bogus key makes
the CLI retry 401s until the alarm kills it, exit 142.

Observed, both outside srt and inside it with `api.anthropic.com` allowed:

```
claude-home/.claude.json
claude-home/backups/.claude.json.backup.<epoch-ms>
claude-home/sessions/<pid>.json
tmp/claude-<uid>/          (in-srt only, and empty)
```

Nothing under `HOME`, nothing elsewhere in the run directory. The three files are
under `<run>/claude-home`; the fourth entry is an empty `claude-<uid>` directory
the CLI creates under the `TMPDIR` srt hands it, which `CLAUDE_CODE_TMPDIR`
makes `<run>/tmp` — it did not appear in the earlier measurement, where the
child's `TMPDIR` was `/tmp/claude`. `<run>/tmp` also holds srt's own
`srt-mux-<pid>-<n>.sock`, which is host-side and outside the sandbox. Both
`<run>/claude-home` and `<run>/tmp` are in `allowWrite` as directories, so the
`allowWrite` list matches the measured set.

**Re-measured without `--bare`** on claude CLI 2.1.289, 2026-10-07, after §7.4
dropped it: the same command with `--bare` removed, `--strict-mcp-config
--setting-sources ''` added, `--tools Read,Write`, and `"autoMemoryEnabled": false`
in `settings.json`. Again nothing under `HOME` and nothing elsewhere in the run
directory; the set under the two writable directories grew by three entries:

```
claude-home/.last-cleanup
claude-home/sessions/<pid>.<hash>.key   (outside srt only)
tmp/cc-socks/                            (in srt, empty)
tmp/cc-socks/<pid>.sock                  (outside srt only)
```

`tmp/cc-socks/` is the CLI's cross-session messaging inbox and the `.key` file is
its auth key; `--bare` skipped both (§7.4). Under srt the CLI creates the
directory but the socket bind fails, publishes no key, and carries on: the init
event has no `messaging_socket_path` and the run proceeds to the API as before.
Outside srt the socket lands at `<run>/tmp/cc-socks/<pid>.sock` while that path
fits in 103 bytes and at `/tmp/cc-socks-<uid>/<pid>.sock`, outside the run
directory, when it does not; production's
`/Users/steve/.shuttle/runs/daily/daily-20260902-043000-8f2a/tmp/cc-socks/99999.sock`
is 83 bytes, so a production run keeps it inside.

**Limitation, stated plainly:** the key was a bogus one. The run reached the API
and was refused (401 `authentication_failed`, retried until killed), so a
*completed turn* was not measured — the Keychain item `shuttle-anthropic-api-key`
(§4.2) does not exist on this machine yet and no `ANTHROPIC_API_KEY` was
available. Because `allowWrite` names `<run>/claude-home` as a directory, any
further file a completed turn writes under `CLAUDE_CONFIG_DIR` still lands
inside it; what this measurement cannot rule out is a completed turn writing
somewhere else entirely. Re-measure with the command above once the Keychain
item exists — the `SHUTTLE_LIVE_CLAUDE=1` test in `internal/runtime/claude` is
the natural place, with a `find` over the run directory and `HOME` afterwards —
and update this section.

## Network, and the proxy

On macOS the seatbelt profile permits egress only to srt's localhost proxy
ports; the proxy environment variables are convenience for tools that read
them, not the boundary. Measured inside the sandbox:

| Command | Result |
|---|---|
| `curl --noproxy '*' https://example.com` | `Could not resolve host` |
| `curl https://example.com` | `CONNECT tunnel failed, response 403` |
| `curl https://api.anthropic.com` with the domain allowed | HTTP 404 — reached |

**Does the claude CLI honour srt's proxy environment? Yes.** The
`CLAUDE_CONFIG_DIR` command above, run under `srt -s <profile> --` with
`allowedDomains: ["api.anthropic.com"]`, produced a `system/api_retry` event
with `error_status: 401, error: authentication_failed`: the request reached the
API, and since the seatbelt allows no route out except the proxy, it went
through the proxy. With `allowedDomains: []` the same command exits 1 with a
terminal result:

```json
{"type":"result","subtype":"success","is_error":true,"total_cost_usd":0,
 "result":"Failed to authenticate. API Error: 403 Connection blocked by network allowlist"}
```

Note the shape: the CLI labels a blocked connection as an authentication failure
with `subtype: "success"` and `is_error: true`. The parser's `Success()` already
requires `is_error` falsy, so this classifies as a failure rather than a
success.

The conclusion is stronger than the question: on macOS the network section is
enforcing by the seatbelt, independent of whether a tool honours the environment
— and the CLI honours it as well. Residuals: the allowlist is a domain filter,
not content inspection, so anything the model can read it can send to
`api.anthropic.com` by construction; and reads are permissive except the denied
list, above.

## SSH agent

`allowUnixSockets` is empty, and that — not `denyRead` — is what refuses the
agent. Inside the sandbox with the real `SSH_AUTH_SOCK` inherited, `ssh-add -l`
returns `Error connecting to agent: Operation not permitted`, exit 2. This is
the property that lets the harness, and never the model, own every write that
leaves the machine, and it is the reason the SSH-agent blocker recorded in
`warp/docs/host-session-isolation.md` does not apply to an unattended run.
`TestLiveSRT` asserts it, and skips the assertion when the test's own
environment has no `SSH_AUTH_SOCK` — with no agent to reach, a pass would prove
nothing.

## Hosted coding profile

Hosted segments no longer run under this profile: each runs as its own agent
user (`docs/agent-users.md`), and the supervisor writes no profile. What follows
is the profile segments launched before that ran under; `reap.Restart` still
sweeps the boundary of such a row left in flight.

Hosted `weft-remote/v1` segments used `CodingProfileJSON`, not the report profile
above. Shuttle authors one profile before spawning `weft-worker`; the worker is
given that path and must place every model launch behind `srt -s <profile> --`.
The host first runs `srt -s <profile> -c true`, and records a `sandbox` failure
without spawning the worker when the profile cannot be established.

The coding profile re-allows the selected checkout, ticket store, retained run
directory and derived worktree beneath the broad read denials. Writes are
limited to the worktree, the code repository's Git object/ref storage, ticket
files and tk state, Claude's subscription state, the segment temp directory,
the per-user Darwin temp directory and the configured toolchain caches. The
per-user temp directory is `confstr(_CS_DARWIN_USER_TEMP_DIR)` (what `getconf
DARWIN_USER_TEMP_DIR` prints), resolved to its `/private/var/folders/.../T`
path: SwiftPM's build service stages its build-manifest writes there and `swift
test` creates its build temp there, both ignoring `TMPDIR`, so without it
`swift build --disable-sandbox` fails with `EPERM`.

`/private/tmp` (what `/tmp` resolves to) is writable too, and that is a
deliberate widening of the write surface, not a free fix. Agents' tools and
project test suites hardcode `/tmp` and fail with `Operation not permitted`
without it; shuttle's own suite did. The worker environment still sets
`TMPDIR` and `CLAUDE_CODE_TMPDIR` to the segment temp directory, so tools that
honour them stay inside the segment. The residual risk: `/tmp` is shared by
every process of every user, so a segment can create or replace files at
predictable `/tmp` paths that unsandboxed processes on the host later read or
execute (other agent sessions, shuttle's own tooling, git hooks staged there).
`denyWrite` takes back only the paths known to matter: srt's default Claude
temp dir `/tmp/claude`, and a host Claude Code session's temp tree
`/tmp/claude-<uid>` (its scratchpads and task output; a host session runs
outside srt with `TMPDIR=/tmp`), named for the host uid. Each is denied in
both spellings, under `/tmp` and `/private/tmp`, and srt's deny rules take
precedence over `allowWrite`. A segment's own Claude temp is not under either:
`CLAUDE_CODE_TMPDIR` puts it in `<run_dir>/tmp`. The `/private/tmp` and
per-user temp grants are shared by every segment's profile, so a worktree
under either would be writable by every concurrent segment, and by every host
process. The preflight refuses one before launch, whatever the run
directory's placement: `requireCoding` tests each shared root against the
worktree the way it tests an `allow_write` entry, literally and then by file
identity, so a `weft.root` that reaches `/tmp` or the per-user temp dir through
a symlink, the `/private` alias or a case alias is refused too, and the error
names the shared root and the worktree. A run directory under `/tmp` is caught
as well: the preflight has the sandbox itself try the run directory, and
refuses a profile that lets the segment write it. Tests that run real srt
therefore keep their fixtures under `/var/tmp`, not `/tmp`.

`denyWrite` takes back
both repositories' Git configuration, hooks and indirection files, the linked
worktree's gitfile, Claude's global `.claude.json`, commands/plugins/rules,
every project's auto-memory directory and installed work-policy files, and
every sibling worktree admin directory enumerated while the host's durable
repository reservation is held. Authentication refreshes and session records
under `~/.claude/` remain writable; executable MCP configuration and persistent
instructions do not.

Repository reservations use the checkout's symlink-resolved canonical path,
with macOS's `/private` alias normalized the same way as the worktree
derivation. Two tk projects that name one checkout through different aliases
therefore contend on one reservation rather than authoring incomplete sibling
deny lists concurrently. The lock key itself is the directory's Darwin
device/inode identity, so case aliases on the default case-insensitive volume
also contend even though the worker argv retains a readable canonical path.

Before profile generation, an existing worktree's gitfile must name the admin
directory derived from its basename. For a new worktree that expected admin
name must be absent. This fails closed on the suffix Git would otherwise assign
for a stale registration, before a worker or model is launched.

The worker process starts behind a separate inherited pipe gate. Shuttle
records the process PID in the ledger before releasing that gate; if the daemon
dies in the start/record window, EOF closes the gate and the worker executable
is never reached.

`TestCodingProfileProtectsGitIndirectionsAndSiblingWorktrees` asserts that
generated shape. `SHUTTLE_LIVE_SRT=1 go test ./internal/sandbox -run
TestLiveCodingSRT` is the host-level proof: a declared worktree write succeeds
while an attempted Git-hook write returns `Operation not permitted`.
`TestCodingProfileAllowsTmpButNotClaudeTmp` asserts the `/tmp` grant and the
`/tmp/claude` and `/tmp/claude-<uid>` carve-outs, and under
`SHUTTLE_LIVE_SRT=1` has real srt take a `/private/tmp` write and refuse one
under each carve-out in both spellings.

## Shuttle's own suite inside the coding profile

A hosted segment's work and review agents run `go test ./...` on shuttle inside
this profile. Its fixtures land where the profile lets them write: every temp
root comes from `t.TempDir()` or `os.TempDir()`, which follow the segment's
`TMPDIR`, except in four named sets of fixtures. The socket-bound fixture
helpers (`newObserverEnv`, `preflightFixture`, `newLiveFixture`,
`newQueueHost`, `newSupervisorFixture`, `shortRunDir`, `shortCodingRoot`) are
the first. They keep a `/tmp` root, which the `/private/tmp` grant covers:
srt's mux socket under their run directory must fit the 103-byte limit above,
which the sandbox, runner and supervisor checks enforce even against a
stand-in srt, and macOS's `os.TempDir()` is 49–57 bytes before the run
directory starts. Their worktrees do not go there, since the preflight refuses
a worktree under `/tmp` or the per-user temp dir. The worktree roots are the
second set: `internal/sandbox`'s `worktreeRoot`, under its coding fixtures'
worktrees, and `internal/weftremote`'s `fixtureWeftRoot`, the `weft.root` of
`newSupervisorFixture` and `newQueueHost`, each make a dot-prefixed temp
directory under the package directory rather than following `TMPDIR`, which a
host session may set under `/tmp` and which on the host is the real per-user
temp dir the supervisor grants. The checkout is writable both on the host and,
as the segment's worktree, inside a hosted segment; `.gitignore` covers the
roots a killed run leaves behind. The opt-in `SHUTTLE_LIVE_SRT` fixtures that run the real srt
are the third: `liveCodingRoot`
(`internal/sandbox/coding_test.go`) keeps a
literal `/var/tmp` root, because their run directory must sit outside every
coding-profile grant — `/tmp` and the per-user `TMPDIR` — or the coding check
refuses it as writable. `internal/reap`'s `hostTmpRoot` is the fourth: it keeps
a `/tmp` root for a run directory that must sit outside the per-user `TMPDIR` a
containment decoy is granted. `TestNoTestHardcodesTmp` (`internal/repocheck`) fails
on a literal `/tmp` fixture root outside the socket-bound helpers and
`hostTmpRoot`.

Some tests still cannot pass inside srt, and run only in the anchors, outside
it. A failure among them in a segment is the sandbox, not the change. Measured
2026-10-31 by running every package's test binary, one top-level test per srt
invocation, under a profile with the coding profile's network keys and its
`/private/tmp`, per-user temp and tk-state write grants:

- **A nested sandbox.** A process already under a seatbelt profile cannot apply
  another: `/usr/bin/sandbox-exec` exits 71. The containment tests that stand in
  for a model process with a holder under its own profile start one — the nine
  `internal/reap`'s package doc names, failing with `start detached child: exit
  status 71`. `internal/sandbox`'s
  `TestCheckRefusesAMalformedProfile` runs the real srt, which inside srt fails
  one step earlier: with `allowUnixSockets` empty it cannot bind its own mux
  socket (`Error: listen EPERM: operation not permitted …/srt-mux-<pid>-0.sock`).
- **Local TCP binding.** The profile sets `allowLocalBinding: false`, so an
  `httptest` server panics with `httptest: failed to listen on a port: listen
  tcp6 [::1]:0: bind: operation not permitted`. The panic ends the package's test
  binary, hiding every test after it. The packages whose tests bind one are
  `cmd/shuttle/cmd`, `internal/deliver`, `internal/runner`,
  `internal/runtime/claude` and `internal/weftremote`.
- **Listing a user's processes.** srt's base policy lets a sandboxed process
  read `kern.proc.all`, `kern.proc.pgrp` and `kern.proc.pid` and inspect
  processes in its own sandbox, so process listing is not in itself denied:
  every `internal/reap` test but the nine above passes inside it.
  `kern.proc.uid` is outside that policy and refused, and it is the listing
  `reap.Contain`'s sweep starts from. `kern.proc.tty`, which the terminal host
  lists a PTY's jobs by, is outside the policy too; the terminal tests fail on
  the local bind before reaching it.

## Running unsandboxed

`shuttle run <agent> --unsafe-no-sandbox` is the only way to run a model outside
the sandbox. There is no `"none"` sandbox kind in the manifest schema (§4.1), no
key for it in `config.json` and no environment variable the daemon reads; the
flag is the only input that will select an empty `claude.Options.Launcher`.
Hosted weft segments have no equivalent escape hatch. The manual report flag
prints one line to stderr naming the run:

```
warning: --unsafe-no-sandbox: run daily-20260906-043000-3795 is exempt from the srt sandbox
```
