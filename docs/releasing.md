# Releasing shuttle

Releases are built by [GoReleaser](https://goreleaser.com). Cutting one is a single
action: **push a `vX.Y.Z` tag**.

```sh
git tag v1.2.0
git push origin v1.2.0
```

The `Release` workflow (`.github/workflows/release.yml`) fires on any `v*` tag and runs
`goreleaser release --clean`, which:

1. Builds `shuttle` for darwin/arm64 with `CGO_ENABLED=1`. The tag drives `shuttle
   version`, and `host_build` on `/v2/service`, through `-X
   shuttle/internal/version.Current={{ .Tag }}`.
2. Checks the built binary with `scripts/verify-release-binary.sh`, before anything is
   archived.
3. Builds `shuttle_<version>_darwin_arm64.tar.gz`, and a `checksums.txt` covering it and
   `release/manifest.json`.
4. Publishes a GitHub Release with those assets and a GitHub-native changelog.

Config lives in [`.goreleaser.yml`](../.goreleaser.yml). Validate a change locally before
pushing a tag:

```sh
goreleaser check
goreleaser release --snapshot --clean --skip=publish   # full dry-run into ./dist
```

## cgo is required

`internal/reap/seatbelt_darwin.go` is `//go:build cgo`: it reaches `sandbox_check`, which is
how containment shows a process holds no grant of a segment's. `CGO_ENABLED=0` builds
cleanly and silently selects `seatbelt_other.go`, whose checks return `errNoSeatbelt`, so
containment fails closed and reservations stay held forever. loom's recipe, cross-compiled
from ubuntu with cgo off, would ship exactly that. So the release is built on a macOS runner,
for one target, and two things fail rather than ship a no-cgo binary:

- The build sets the `release` tag, and `internal/reap/release_nocgo.go` (`release && !cgo`)
  does not compile.
- `scripts/verify-release-binary.sh` reads `CGO_ENABLED=1` out of the built binary's build
  info, whatever the config said, and aborts the release before anything is published.

## Version numbering

Versioning follows [SemVer](https://semver.org). The updater moves a host forward only: a
lower tag is never installed, because a ledger a newer release migrated is one the older
binary refuses. The workflow needs no secrets beyond the built-in `GITHUB_TOKEN`.

## The release's compatibility rule

`release/manifest.json` is what a release says it needs of the host. Set it in the commit you
tag.

- **`min_worker_version`** is the oldest `weft-worker` the release runs against. The updater
  reads it before it pauses anything, holds it to what `weft.worker_bin` probes as (the
  version in `worker_version`, `weft-worker 0.2.0 (weft@dev)`), and refuses a release asking
  for a newer worker, at every poll, until the worker is updated. A worker it cannot probe is
  refused the same way.
- **The desktop has no such check yet.** Weft clients do not report their versions, so the
  oldest desktop a release supports is a rule held here, by whoever tags: **never serve a v2
  value the current desktop release does not decode.** The desktop decodes v2 strictly, so a
  host serving a value the installed desktop does not know puts every connected desktop in its
  refused state. A new value ships in a desktop release first, and only then in a host release
  (weft/decode-acceptance-criteria-1c0c was that ordering fix).

### Precondition for the first release that writes the `host` event

The `host` event (weft-service/v2 §10) is the first such value. Strict decoders refuse an event
name they do not know, which ends a client's connection on the refused state, and this host
writes one the first time dispatch pauses or resumes, which an update always does.

**Do not tag the first release carrying it until every client in use reads it:** the desktop on
both Macs and the iPhone app. A build that reads it works against a host with or without it, so
that step can finish well ahead of the tag. weft's `docs/upgrade-runbook.md`, "Writing the `host`
event", is the rollout, and the same order holds for a `make install` of this code. Do not roll a
client back past that build afterwards.

## Releases are the deploy channel

A tagged release is the deploy unit: an unreleased commit on `main` reaches a host only in a
release. `shuttle updater daemon`, the `com.shuttle.updater` job, polls the latest GitHub
Release anonymously every `SHUTTLE_UPDATER_INTERVAL_MINUTES` (5 by default, on the plist).

loom's updater is the shape, and its doc excludes apps that cannot lose in-memory state on a
restart, which is this one. A restart here kills what the daemon supervises, leaves a
forward-only ledger migrated, and reaches desktops that decode strictly. So a cycle runs:

1. **Download and verify.** The tarball and manifest against `checksums.txt`, the worker against
   the manifest; extract only the `shuttle` binary, refusing an archive with an entry that climbs
   out of it; stage it beside the target and run it, which must report the tag. Read the running
   build from `/v2/service`. Anything refused here retries at the next poll with dispatch
   untouched.
2. **Pause `weft-worker`,** with reason `update`. Clients see it on `dashboard.host`, as the
   `host` event, and on waiting entries' `host_paused`, in words that name the update. A pause
   that already stands is left alone and the poll skipped: the updater never takes over or lifts
   an operator's, auth or streak pause.
3. **Drain.** Wait for the ledger to show nothing a restart would kill, never signalling anything.
   Then raise the [restart barrier](#the-restart-barrier), which makes that final check and holds
   it true: from here the update runs to its end.
4. **Back up** `runs.db` with `VACUUM INTO`, which reads through the WAL, to
   `~/.shuttle/backups/runs.db.<stamp>.pre-<tag>`. The newest three are kept.
5. **Install.** Rename the staged binary over `~/.local/bin/shuttle`, keeping the old one as
   `shuttle.previous`. A rename, never a write in place: macOS kills a process whose signed
   executable's pages change.
6. **Restart** the daemon and watchdog: `<new binary> updater rebootstrap`, which is bootout and
   bootstrap. `launchctl kickstart` leaves a running job alone, and `-k` has launchd refuse a
   binary signed differently from the one it bootstrapped; see loom's "macOS caveat".
7. **Confirm** `/v2/service` reports `shuttle <tag>`, exactly.
8. **Resume,** lifting the pause and the barrier together, then restart the updater itself, last,
   through a detached helper.

### What counts as drained

Every row of `runs` still claimed or running, of any agent and any segment kind: scheduled agents
such as `daily`, manual `shuttle run`s, hosted segments, reconcile, cleanup and archive workers.
The restart pass reaps them all, and neither a cron slot nor a manual run stops for a pause on
`weft-worker`. A run whose email is still going out counts too, for ten minutes after it ends:
its row completes before the send.

So does every open terminal: `starting`, `live` or `detached`, repair sessions and others alike.
§9.4 says sessions do not survive a daemon restart, and one left detached is as live as an
attached one. A terminal ends only when someone ends it, so an open one does not start a drain:
**the update waits, polling, with dispatch open, until the terminal is closed**, and one opened
during a drain ends that attempt at once. A forgotten terminal holds updates back indefinitely,
and `updater.log` names it at every poll.

The bound is two hours. Past it the updater lifts the pause, leaves the old binary running, and
stays away for six hours: dispatch is worth more than a prompt update, and what ran out the bound
may be a row that never closes.

### The restart barrier

Neither a cron slot, a manual run nor a terminal waits on `weft-worker`'s pause, and an operator
can lift the pause, so a drain that reads clear proves nothing about the moment of the restart. The
barrier closes that gap. One ledger transaction checks that the update pause stands and nothing a
restart would kill remains, and writes the `restart_barrier_until` row of `weft_settings`. Every
ledger transaction opens with `BEGIN IMMEDIATE`, which takes SQLite's write lock across processes,
so a claim either committed first, and the check sees its row and ends the attempt, or runs after
and is refused.

Until the update ends, fall-back included:

- **A scheduled slot** writes no row and logs `action=hold reason=restarting` to `daemon.log`. The
  cursor stays behind it, so the first tick after the update claims that same slot.
- **`shuttle run`** exits with an error naming the update and records nothing.
- **A new terminal** is refused 409 `repository_reserved`, with a message naming the update. Clients
  decode error codes strictly, and that is the code `POST /v2/terminals` already answers for work
  that holds a session off, whatever the target.
- **Hosted dispatch** stays shut on the update pause, which the barrier pins. Waiting entries keep
  their place as `host_paused`. What a settlement claims instead of dispatch, a reconcile for a lost
  landing or the new run of an archived one, is claimed as a segment closes, and with nothing in
  flight nothing closes.

`shuttle resume weft-worker` under the barrier cannot lift the pause, since that pause is what keeps
dispatch shut. It prints `weft-worker stays paused while an update restarts shuttle, and resumes when
the update ends`, clears the failure streak at once, and leaves the pause as the update's, so
dispatch opens when the update ends, however it ends. `shuttle pause` under the barrier does not
abandon the update either, and its pause stands afterwards. A resume after that pause hands it back
to the update.

The barrier writes nothing a client reads: `dashboard.host` and the `host` event follow the pause as
they did. The one event it causes is the change of words when a resume hands an operator's pause
back to the update.

The fall-back's restart is covered. A new build that came up but was never confirmed can otherwise
claim work, and that restart would kill it, so the barrier stays up until the fall-back ends. It
goes up before the backup, so a restored ledger brings it back too.

It cannot wedge the host. Every way out of a cycle takes it down with the pause, in one transaction,
whichever pause stands, and the first poll after a crash does the same. An updater that never comes
back leaves it to lapse by itself after 30 minutes, when held work starts again and `shuttle resume
weft-worker` lifts the pause as usual.

Two windows are left:

- **The lapse.** The bounded steps reach the last restart under five minutes after the backup, so
  only a backup that hangs or an updater suspended past 30 minutes reaches it: work admitted after
  the lapse is killed by the restart that follows.
- **A daemon in its own restart pass.** If the daemon crashes during a drain, launchd restarts it,
  and its restart pass closes the rows the drain was waiting on, then claims what follows them in a
  later transaction: a reconcile for a lost landing, the new run of an archived one. A barrier that
  rises between the two sees an empty host, the claim does not read it, and the update's restart
  interrupts that one segment. The next restart pass accounts for it like any other.

### When it fails

Every failure lifts the pause and the barrier, and writes the reason to `~/.shuttle/updater.log`, which records
each step, decision and refusal. Dispatch picks up within one catalog poll (5s): `watchCatalog`
dispatches on every pass, which is how a pause lifted from another process reaches the queue,
with or without a daemon restart.

- **Before the install,** nothing has changed.
- **From the install on,** a restart or confirmation that fails renames `shuttle.previous` back,
  bootstraps it, and confirms the old build returned. If the new build moved the ledger's schema,
  the old binary would refuse it, so the jobs are stopped and the backup restored first. What the
  ledger took in since the backup is lost from the live record and kept beside the backups as
  `runs.db.<stamp>.failed-<tag>`.
- **A release that fell back** is not tried again until a newer one ships or the updater restarts.
- **A fall-back that fails** still lifts the pause and logs `the host needs an operator`, with one
  exception. If the ledger restore fails after a migration, this binary cannot open the ledger
  that remains, so neither the fall-back nor any later poll can reach the pause. The daemon is
  stopped at that point, `updater.log` says the ledger must be repaired, and the pause stands until
  an operator repairs it or installs a newer binary; the next poll then lifts it.
- **A crash** cannot leave dispatch paused: every poll, the first one after a restart included,
  starts by lifting an update pause no running cycle holds, and the barrier with it.

`shuttle resume weft-worker` during a drain abandons the update at the next check, as does `shuttle
pause`, whose pause then stands. Once the barrier is up neither does: see
[The restart barrier](#the-restart-barrier).

If the updater stops polling after an update (no new lines in `updater.log`), its detached helper
died between bootout and bootstrap. `shuttle install updater` brings it back.

### Dev builds

As in loom, a `dev` build always updates to the latest release, whatever that is, so a host never
stays on an unreleased binary. `make install` on a host running the updater therefore lasts until
the next poll; to run an unreleased build for longer, `launchctl bootout` the updater first.

### A host's first update

A host without the updater needs it installed once by hand, at a point you would otherwise have
installed by hand: pause `weft-worker`, wait until nothing runs, back up `runs.db`, `make install`,
`shuttle install daemon`, check `host_build`, resume. That build is `dev`, so its updater installs
the latest release at its first poll. The repository must be public by then: the updater fetches
with no credential, and until there is a public release it logs a 404 at every poll and does
nothing else.

`weft.listen` must be set, to an address the daemon itself accepts: `127.0.0.0/8` or
`100.64.0.0/10`. With no `/v2/service` to confirm a build against, the updater refuses every release,
and it reads the service token only after that check, since the token travels over plain HTTP.
