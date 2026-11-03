# Agent users

Status: 2026-11-01, Mac Studio, macOS, sudo 1.9.17p2, git 2.55.0
(`/opt/homebrew/bin/git`). The scripts in `scripts/agent-users/` productise what
`shuttle/spike-run-hosted-170e` proved by hand. `check.sh` and `protect-checkout.sh` were
measured against the spike's `weftagent1` in a test root laid out as `setup.sh` lays out
`/Users/Shared/weft` (root without an ACL, both trees carrying the shared ACL), with checkouts on
git's default files ref backend; every denial below was shown to refuse the agent with the agent
still working in the worktree.
`setup.sh` and `teardown.sh` need sudo and were not run here, so the idempotence and rebuild
behaviour below is what they are written to do, not yet measured.

Each hosted slot runs its agent as its own unprivileged macOS user. The uid is the
containment domain: a step down in privilege from the operator, not a sandbox. The operator
is the human whose account the worker runs under; it launches the agent's commands through
`sudo -u weftagentN`.

## The pieces

**Users: `weftagent1`..`weftagentN`.** Standard, non-admin accounts with no password: nothing
logs in as an agent, and the operator reaches one only through `sudo`. `setup.sh` builds each
record with `dscl` rather than `sysadminctl`, which takes a password in argv, where `ps` shows it
to every user including agents already running, and puts the user in `staff`. The record has
`Password: *` and no `AuthenticationAuthority`, as the system's own `_` accounts do, its primary
group is `weftagents` from the start, and its `UniqueID` is set last, so no process can run as it
before all of that holds. A user that already exists with password authentication, such as the
spike's `sysadminctl`-made `weftagent1`, has its `AuthenticationAuthority` and shadow hash
removed and `Password` set to `*`. `setup.sh` reads each of the three keys on its own: `dscl
-read` of a key the record lacks exits 0 with nothing on stdout and `No such key` on stderr
(measured as the operator against `_www`, which reads as already correct; not measured as root,
which alone can read `ShadowHashData`), so a key counts as absent only when its read succeeds
and prints nothing. Primary group `weftagents`, also listed as a member of it,
and not in `staff` or `admin`. The operator's home is 0750 `<operator>:staff`, so a `staff`
member could read `~/.claude`, `~/.gitconfig` and `~/code`; keeping the agents out of
`staff` is what refuses them those. A process keeps the groups it started with, and a login
outlives the password that opened it, so before repairing an existing user's password
authentication, primary group or `staff`/`admin` membership, `setup.sh` kills every process
running as that user (printing a `changed:` line saying so), and kills again once the repairs
are in, failing unless none is left. Each kill repeats `pkill -KILL -U` until `pgrep -U` finds
nothing, since a process forked between pkill's listing and its signal escapes one pass, and
gives up after five seconds, listing on stderr the pid and command of each survivor. A repair
therefore kills any hosted run in that slot: do not repair a user under a live run. Adding a
user to `weftagents` kills nothing, and a run that repairs nothing kills nothing. Homes are
`/Users/weftagentN`, 0700 `weftagentN:weftagents`, so one agent cannot read another's.

One process comes back on its own. launchd runs `distnoted` in each user's background session as
a `KeepAlive` agent (`com.apple.distnoted.xpc.agent`), and the spike saw it reappear after
`pkill -U`. Measured on the spike's `weftagent1`, killing it by pid as the agent through the
operator's sudo: an instance that had run longer than 10 s was replaced within 60 ms; killing
that replacement, under 10 s old, brought the next one only 10 s later (launchd's respawn
throttle). So the kill loop most likely ends with nothing left, and `distnoted` returns about
10 s after it has passed. Not measured: that sequence when root runs the loop, and whether
launchd still respawns it once `sysadminctl -deleteUser` has removed the user. The first
`setup.sh` run on the Studio settles the former: if the loop gives up, it prints what survived.

As root, `setup.sh` changes an agent's home directory itself and nothing in it. An agent can
hard-link into its home any file it can reach on the data volume, its owner unchanged (measured:
`weftagent1` linked the operator's `.git/config` in a checkout and the operator's Homebrew `git`
binary), so a recursive `chown` there would hand it those files. And a root write through a
symlink the agent planted lands where the agent chose: BSD `install` given a symlink to a
directory as its target writes into that directory (measured). So the agent's `~/.gitconfig` is
compared and written as the agent, which only redirects the agent's own privileges.

**Group: `weftagents`.** The agents' primary group. The operator is not a member: the
checkout denies below are written against this group, and the operator reaches files an
agent creates through its own inherited `user:<operator>` ACE instead.

**sudoers: `/etc/sudoers.d/weft-agents`**, 0440 root:wheel. No dot in the name: sudo skips
files in `sudoers.d` whose names contain one.

```
Runas_Alias WEFT_AGENTS = weftagent1, …, weftagentN
Defaults>WEFT_AGENTS env_keep += "CLAUDE_CODE_OAUTH_TOKEN"
<operator> ALL = (WEFT_AGENTS) NOPASSWD: ALL
```

The runas list names the agent users explicitly and never root, so the operator can only
step down. `env_keep` is how the Claude OAuth token reaches the agent: through the
environment, never through argv, because argv is visible in `ps` to every user on the
machine, agents included. It relies on sudo's default `env_reset` being the allowlist, so a
launch must not put `env -i` after `sudo`; that strips the kept token with everything else.
macOS's default sudoers also keeps the operator's `HOME` (measured: `sudo -u weftagent1
printenv HOME` prints the operator's), so a launch passes `-H`, or the agent's git and claude
look for their configuration in a home they cannot read. `setup.sh` validates the candidate
with `visudo -cf`, installs it only when it differs, then runs `visudo -c` on the whole
configuration and restores the previous file if that fails.

**Shared root: `/Users/Shared/weft`.** `<operator>:weftagents` 0750 with no ACL: agents
traverse and list it, and cannot write it. Exactly two trees under it are agent-writable,
each `<operator>:weftagents` 0770 carrying two inheritable allow ACEs, one for
`user:<operator>` and one for `group:weftagents`:

- `repos/`: the hosted checkouts.
- `worktrees/`: the per-run worktrees.

Everything created in them inherits both ACEs, so the operator can open and continue
anything an agent made and an agent can work in anything the operator made. New files take
their parent directory's group (BSD semantics), so ownership never needs fixing up. The trees
themselves also carry a non-inherited `group:weftagents deny delete`: without it the allow ACE
on the tree lets an agent move the whole of `repos/` out of the shared root into its own home
(measured), where the operator cannot follow. Setting or repairing a tree's ACL clears it and
adds that deny before either allow, so there is no moment the allow holds without it. Run logs,
summaries, findings and base records stay in the operator-only weft root, not here: an agent
that could rewrite a base record or `runs.jsonl` could hide commits from review or forge
resume and landing state.

**Checkout denies: `protect-checkout.sh <checkout>`.** Run as the operator, who owns the
checkout, without sudo. It adds `group:weftagents deny` ACEs, evaluated before the inherited
group allow, to the paths that decide which config and hooks the operator's next git command in
the checkout or one of its worktrees uses. Without them an agent could set `core.hooksPath` or
drop a hook, or swap in a config and hooks of its own, and have that command run them as the
operator. Each was measured as exploitable before the deny and refused after:

- `.git/config`: no write, append or delete, so nothing can be renamed over it or it away.
- `.git/hooks` and every file in it: no hook added, edited, deleted or renamed, and no rename of
  `.git/hooks` itself. A hook the operator later creates there inherits the file deny.
- The checkout directory and `.git`: no rename, so neither can be swapped for another.
- `.git/worktrees`: no entry added or removed, so agents add no worktrees. For each linked
  worktree: its admin directory (no rename), the `commondir` and `gitdir` in it, the worktree
  directory (no rename) and its `.git` file. Once those are denied it checks that `commondir`
  says `../..` and the `.git` file names that admin directory, and refuses otherwise.

Agents can add, replace and delete every other file at the top of `.git`: the files ref backend
takes `.git/packed-refs.lock` for every ref deletion and renames it over `packed-refs` (and `gc`
takes `.git/gc.pid` the same way), so a deny on adding files to `.git`, or on `packed-refs`,
breaks an agent's `branch -D`, `tag -d` and cherry-pick (measured). What that leaves open is
listed under **What it does not protect**. With these denies, an agent in a linked worktree
committed, stashed, amended, cherry-picked, reset, merged, rebased, created and deleted branches
and tags (loose and packed), and ran `gc --auto` and `gc` (measured). Branch commands that also
edit the branch's section of `.git/config` are refused that write: `branch -D` prints
`could not write config file` and deletes the branch, exiting 0; `branch -m` renames the branch
and exits 128; `push -u` and anything else that writes config as the agent fails.

The owner of a path can rewrite its ACL, so it refuses a path the operator does not own, as well
as a symlink (`chmod +a` follows symlinks) and a checkout outside `/Users/Shared/weft`. It
refuses `extensions.worktreeConfig`, under which git reads a `config.worktree` from each admin
directory, where agents must be able to add files. It leaves an ACE that is already present
alone, so re-running it adds nothing.

Three orderings bind whatever creates and operates production checkouts and worktrees, which
this ticket does not build:

- **Protect before exposure.** Anything created in `repos/` or `worktrees/` inherits the agent
  allow at once, so an agent watching the tree could plant a hook or config in a new checkout
  before it is protected, and the operator's first commit would run it. Build and protect the
  checkout where no agent can reach it, then move it in: `check.sh` does this with a 0700 stage
  in the shared root whose trees carry the shared ACL, `git worktree add --relative-paths` so
  the worktree's paths survive the move, and a `rename(2)` onto a name reserved with `mkdir`.
- **Re-protect after each `git worktree add`, `move` or `repair`.** A new admin directory and
  worktree, or rewritten `commondir`, `gitdir` and `.git` files, carry no deny until it runs.
  Between the two an agent can tamper with them; the re-run then refuses what it does not
  recognise rather than protecting it.
- **Re-protect after each config write by the operator's git in the checkout.** `git config`,
  and anything that writes config such as `git remote add`, `git branch --set-upstream-to` or
  `git push -u`, replaces `.git/config` by renaming a new file over it. The new file carries only
  the inherited allows, so the agent can write it until `protect-checkout.sh` runs again
  (measured). The re-run refuses a config the agent replaced, which it no longer owns, but it
  cannot tell one the agent edited in place, and that config is the one the operator's git in
  every worktree of the checkout reads. Keep operator config writes out of hosted checkouts, or
  re-run immediately after one; a deny inherited by every new file at the top of `.git` would
  close this, and is what breaks the files ref backend above.

**Git identity.** Each agent's `~/.gitconfig` carries the operator's own `user.name` and
`user.email`, read by `setup.sh` from the operator's global git config. Weft lands agent
commits by fast-forward, so this identity goes straight into history, and agents commit as
the operator by decision. `setup.sh` fails if the operator has none, and re-running it after
the operator changes identity rewrites every agent's copy.

**safe.directory.** The checkouts and worktrees are operator-owned, and git refuses a
repository its user does not own. The same `~/.gitconfig` lists
`safe.directory = /Users/Shared/weft/repos/*` and `/Users/Shared/weft/worktrees/*`; a trailing
`/*` matches every repository under that directory (`git help config`, 2.55). Measured: with
only those two entries, scoped to a test root, the agent committed in a worktree under it.
`check.sh` injects no `safe.directory` of its own, so its git checks test what setup
installed.

## What it does not protect

This is a uid boundary on a shared kernel. It makes the operator's files cost more to reach;
it does not make them unreachable.

- **Egress is unfiltered.** Phase-1 decision: an agent has the same network access as any
  local user.
- **Anything world-readable elsewhere on the machine is readable by the agents**: `/tmp`,
  the rest of `/Users/Shared`, anything mode o+r outside the operator's home. The boundary
  covers the operator's home and the denied parts of a checkout, and nothing already open to
  every user.
- **Files already in `repos/` or `worktrees/` before setup are not re-ACL'd.** `setup.sh`
  sets the two trees' own ACLs and does not recurse; contents created before then keep
  whatever they had.
- **The operator's git in a worktree, or in a checkout's working tree, is not safe.** The
  checkout denies keep the checkout's own config and hooks in place; they do not stop an agent
  making git read a different repository's. Two routes, both measured:
  - A forged submodule. In a worktree the agent turned an operator-made directory into a git
    directory whose config sets `core.fsmonitor`, an operator-made file into a gitfile pointing
    at it, and recorded a submodule there in the worktree's index; the operator's next plain
    `git status` in the worktree ran the agent's command as the operator. git's ownership check
    (`safe.directory`) passes, because the agent only renamed and rewrote files the operator
    owns, which the shared ACL lets it do anywhere in the trees.
  - `.git/commondir`. Agents can add files at the top of `.git` (the files ref backend needs
    it, above), so one can create `.git/commondir` naming a directory of its own whose config
    sets `core.fsmonitor`; the operator's next plain `git status` in the checkout ran it as the
    operator. It does not reach a linked worktree, whose own `commondir` and `.git` file are
    denied: there git still resolved the checkout's `.git`. An agent can likewise overwrite
    `.git/HEAD`, after which the operator's git does not recognise the checkout and walks up
    into `repos/`; an agent-made `repos/.git` was then refused for its ownership.

  Nothing here closes either; the remedy is a follow-up in weft: run no operator git in an
  agent-writable tree, for example land by fetching the agent's branch into an operator-only
  clone. Likewise anything git keeps per worktree in the admin directory, which agents must be
  able to write: rebase and cherry-pick state, for one, which the operator's
  `git rebase --continue` there would follow.
- **The operator's own processes are not bounded by any of this.** The worker, git push,
  review and landing run with the operator's full environment.

Not provisioned here: a copy of warp's `~/.claude` scripts in each agent's home. The spike
found weft's test suite needs it when run as an agent user.

## check.sh

```
scripts/agent-users/check.sh
```

Run as the operator, never root, without a password. It checks every member of `weftagents`
(each must be runnable with `sudo -n -u`; no members is a failure):

- (a) not in `staff` or `admin`; refused the operator's home listing, `~/.ssh`, `~/.claude`,
  `~/.gitconfig`, `~/code`, `~/Library/Keychains` and SSH agent; refused `sudo -n`; refused
  creating a file directly in `/Users/Shared/weft` and moving `repos/` or `worktrees/` out of
  it (into the other tree, so a failure can be undone); and `CLAUDE_CODE_OAUTH_TOKEN` set on
  the operator's `sudo` reaches the agent.
- (b) the agent commits in a worktree under `worktrees/`, the operator commits after it, and
  the agent commits again.
- (c) in a checkout under `repos/` protected by `protect-checkout.sh`, and its worktree, the
  agent is refused entering the stage; appending to `.git/config`, `git config`, deleting it,
  replacing it by rename, adding a hook, editing a sample hook and renaming `.git/hooks`;
  renaming `.git` or the checkout; adding an entry to `.git/worktrees`, renaming the worktree or
  its admin directory, and editing the worktree's `.git` file or the admin directory's
  `commondir` or `gitdir`. Afterwards the config is unchanged, the operator's git still runs in
  the checkout, and in both the checkout and the worktree it still resolves the checkout's own
  `.git` and `.git/hooks`.
- (d) a double-forked, `setsid`'d sleeper started as the agent is gone after
  `pkill -KILL -U` as the agent.

(b) and (c) use a scratch repository `repos/.check-<pid>` and worktree
`worktrees/.check-<pid>`, built, committed to and protected in the operator-only stage
`/Users/Shared/weft/.check-stage-<pid>` before they are published, as described under
**Protect before exposure**; a name that already exists fails the check rather than being
reused. All of it is removed on exit, including on failure. A denial passes only when the
target exists for the operator and the agent's command fails with a permission error; a
missing target is a failure, not a pass. The SSH-agent check needs a socket from
`SSH_AUTH_SOCK` or `launchctl getenv SSH_AUTH_SOCK` that the operator can reach, and a
connection error for the agent. (d) kills everything the agent user is running, so do not run
the check while a segment is running.

It prints one PASS or FAIL line per check, appends every command and its output to a log
under the operator's `TMPDIR` whose path it prints first and last, and exits 1 if anything
failed.

## Adding a slot

```
sudo scripts/agent-users/setup.sh N
scripts/agent-users/check.sh
```

with N the new total. `setup.sh` compares each item with its desired state and changes only
what differs, printing a `changed: …` line per change and saying so when nothing changed. A
larger N adds the new users and rewrites the sudoers alias; nothing else changes. Users
numbered above N are noted and left alone, but drop out of the sudoers alias, so `check.sh`
fails on them until teardown removes them.

## Rebuilding

```
scripts/agent-users/teardown.sh             # prints what it would remove
sudo scripts/agent-users/teardown.sh --yes
sudo scripts/agent-users/setup.sh N
```

`teardown.sh` removes the sudoers file first, so nothing can launch as an agent between the
kill and the delete; then enumerates every `weftagent<digits>` user, kills its processes with
the same loop as `setup.sh` and deletes it and its home; then the `weftagents` group and
`/Users/Shared/weft`. That last includes every hosted checkout and worktree, which may hold work
not yet landed, so without `--yes` it removes nothing and lists what it would. A user whose
processes survive the loop is not deleted: a process left running as a deleted uid would pass to
the rebuilt agent, which `setup.sh`'s numbering (one above the highest uid) usually gives the
same uid. `teardown.sh` prints what survived, reports the user under `REMAINS` and exits 1.
After a reinstall, `setup.sh` rebuilds
everything from nothing, and each checkout re-cloned under `repos/` needs `protect-checkout.sh`
again, before any agent can reach it.

## Spike leftovers

The spike left `weftagent1`, the `weftagents` group, `/etc/sudoers.d/weft-spike` and
`/Users/Shared/weft-spike` on the Studio. `setup.sh` repairs `weftagent1` and `weftagents` in
place, disabling `weftagent1`'s password and killing its running processes, and touches nothing
else of the spike's.

- The spike's own `teardown.sh` deletes `weftagent1` and the `weftagents` group. Run it
  before `setup.sh`, never after, or it removes what setup provisioned.
- `/etc/sudoers.d/weft-spike` grants runas to `weftagent1` without `env_keep`. Remove it
  (`sudo rm /etc/sudoers.d/weft-spike`); the managed file supersedes it.
