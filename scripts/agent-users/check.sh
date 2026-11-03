#!/bin/bash
# Check that the agent users setup.sh provisions hold the boundary docs/agent-users.md
# describes. Run as the operator, never root; it needs no password, only the sudoers rule
# setup.sh installs:
#
#   scripts/agent-users/check.sh
#
# Every member of the weftagents group is checked:
#   (a) outside staff and admin; refused the operator's home, ~/.ssh, ~/.claude, ~/.gitconfig,
#       ~/code, ~/Library/Keychains and SSH agent; refused sudo, writing in the shared root
#       itself and moving repos/ or worktrees/ out of it; handed CLAUDE_CODE_OAUTH_TOKEN through
#       sudo's env_keep;
#   (b) the agent and the operator both commit in a worktree under worktrees/;
#   (c) in a checkout under repos/ that protect-checkout.sh has protected, and its worktree, the
#       agent is refused .git/config, .git/hooks, and what would let it swap them: renaming the
#       checkout, .git, the worktree or its admin directory, adding an entry to .git/worktrees, and
#       editing the worktree's .git file or its admin directory's commondir or gitdir; afterwards
#       the operator's git in both still resolves the checkout's own .git and hooks;
#   (d) pkill -KILL -U as the agent leaves no setsid'd descendant.
# (b) and (c) use a scratch repository and worktree this builds and protects in an operator-only
# stage, publishes, and removes on exit. (d) kills every process the agent user runs, so do not
# run this while a segment is running.
#
# One PASS or FAIL line per check on stdout; every command and its output is appended to a log
# whose path is printed first and last. Exits 1 if anything FAILs.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

if [ "$(id -u)" -eq 0 ]; then
  echo "run as the operator, not root" >&2
  exit 1
fi
if [ $# -ne 0 ]; then
  echo "usage: $0" >&2
  exit 64
fi
OPERATOR=$(id -un)
OP_HOME=$(dscl . -read "/Users/$OPERATOR" NFSHomeDirectory | awk '{print $2}')
LOG=$(mktemp "${TMPDIR:-$(getconf DARWIN_USER_TEMP_DIR)}/agent-users-check.XXXXXX") || exit 1
echo "log: $LOG"

# The operator's cwd may be somewhere the agents cannot traverse, and git run as an agent dies
# on getcwd there.
cd /

PASSED=0
FAILED=0
record() {
  local line
  line=$(printf '%s  %-12s  %-24s  %s' "$1" "$2" "$3" "$4")
  echo "$line"
  echo "$line" >> "$LOG"
  if [ "$1" = PASS ]; then PASSED=$((PASSED + 1)); else FAILED=$((FAILED + 1)); fi
}
oneline() { printf '%s' "$1" | tr '\n' ' ' | head -c 200; }

# run <cmd...>: the command and its output go to the log; the output is left in OUT.
OUT=""
run() {
  local rc
  printf '$ %s\n' "$*" >> "$LOG"
  OUT=$("$@" 2>&1)
  rc=$?
  printf '%s\n[exit %d]\n' "$OUT" "$rc" >> "$LOG"
  return "$rc"
}

# As a hosted launch runs it: sudo's own environment reset, HOME set to the agent's. Nothing of
# the operator's git configuration is injected, so git sees only what setup.sh installed.
as_agent() { sudo -n -u "$AGENT" -H "$@"; }

# expect_denied <check> <target> <cmd...>: PASS only when the filesystem refused the agent. A
# target the operator cannot see is untestable, and a failure for any other reason is not a
# refusal; both FAIL.
expect_denied() {
  local name="$1" target="$2"
  shift 2
  if [ ! -e "$target" ] && [ ! -L "$target" ]; then
    record FAIL "$AGENT" "$name" "cannot test: $target does not exist"
  elif run as_agent "$@"; then
    record FAIL "$AGENT" "$name" "agent was allowed: $(oneline "$OUT")"
  elif printf '%s' "$OUT" | grep -qiE 'permission denied|operation not permitted'; then
    record PASS "$AGENT" "$name" "refused: $(oneline "$(printf '%s' "$OUT" | tail -n 1)")"
  else
    record FAIL "$AGENT" "$name" "failed, but not refused: $(oneline "$OUT")"
  fi
}

check_access() {
  local g sock probe
  if ! run as_agent /usr/bin/id -Gn; then
    for g in staff admin; do
      record FAIL "$AGENT" "access.not-$g" "cannot test: id -Gn failed: $(oneline "$OUT")"
    done
  else
    for g in staff admin; do
      if printf ' %s ' "$OUT" | grep -q " $g "; then
        record FAIL "$AGENT" "access.not-$g" "groups: $(oneline "$OUT")"
      else
        record PASS "$AGENT" "access.not-$g" "groups: $(oneline "$OUT")"
      fi
    done
  fi
  expect_denied access.home "$OP_HOME" /bin/ls "$OP_HOME"
  expect_denied access.ssh "$OP_HOME/.ssh" /bin/ls "$OP_HOME/.ssh"
  expect_denied access.claude "$OP_HOME/.claude" /bin/ls "$OP_HOME/.claude"
  expect_denied access.gitconfig "$OP_HOME/.gitconfig" /bin/cat "$OP_HOME/.gitconfig"
  expect_denied access.code "$OP_HOME/code" /bin/ls "$OP_HOME/code"
  expect_denied access.keychains "$OP_HOME/Library/Keychains" /bin/ls "$OP_HOME/Library/Keychains"

  # ssh-add -l exits non-zero for a reachable agent holding no keys too, so the agent's exit
  # code alone would score a reachable SSH agent as refused: require a connection error, and
  # first that the operator reaches it (exit 0 keys listed, 1 no keys, 2 unreachable).
  sock="${SSH_AUTH_SOCK:-$(launchctl getenv SSH_AUTH_SOCK 2>/dev/null)}"
  if [ -z "$sock" ]; then
    record FAIL "$AGENT" access.ssh-agent "cannot test: no SSH_AUTH_SOCK in the environment or launchd"
  elif run /usr/bin/env SSH_AUTH_SOCK="$sock" /usr/bin/ssh-add -l || [ $? -eq 1 ]; then
    run as_agent /usr/bin/env SSH_AUTH_SOCK="$sock" /usr/bin/ssh-add -l
    if printf '%s' "$OUT" | grep -qi 'error connecting to agent'; then
      record PASS "$AGENT" access.ssh-agent "refused: $(oneline "$(printf '%s' "$OUT" | tail -n 1)")"
    else
      record FAIL "$AGENT" access.ssh-agent "agent reached the SSH agent: $(oneline "$OUT")"
    fi
  else
    record FAIL "$AGENT" access.ssh-agent "cannot test: the operator cannot reach $sock either"
  fi

  if run as_agent /usr/bin/sudo -n /usr/bin/true; then
    record FAIL "$AGENT" access.no-sudo "agent ran sudo"
  elif printf '%s' "$OUT" | grep -q '^sudo:'; then
    record PASS "$AGENT" access.no-sudo "refused: $(oneline "$OUT")"
  else
    record FAIL "$AGENT" access.no-sudo "failed, but not refused by sudo: $(oneline "$OUT")"
  fi

  SCRATCH+=("$SHARED_ROOT/.check-$$")
  expect_denied access.shared-root-write "$SHARED_ROOT" /usr/bin/touch "$SHARED_ROOT/.check-$$"
  tree_move access.repos-move repos worktrees
  tree_move access.worktrees-move worktrees repos

  probe="check-$$-$RANDOM$RANDOM"
  run /usr/bin/env CLAUDE_CODE_OAUTH_TOKEN="$probe" sudo -n -u "$AGENT" -H /usr/bin/printenv CLAUDE_CODE_OAUTH_TOKEN
  if [ "$OUT" = "$probe" ]; then
    record PASS "$AGENT" access.env-keep "CLAUDE_CODE_OAUTH_TOKEN reached the agent through sudo"
  else
    record FAIL "$AGENT" access.env-keep "CLAUDE_CODE_OAUTH_TOKEN did not reach the agent: $(oneline "$OUT")"
  fi
}

# tree_move <check> <tree> <other tree>: the agent moving a whole tree out of the shared root.
# The destination is in the other tree, from which the operator can move it back; the agent's
# home, where an attack would put it, is out of the operator's reach.
tree_move() {
  local dest="$SHARED_ROOT/$3/.check-$$-$2"
  expect_denied "$1" "$SHARED_ROOT/$2" /bin/mv "$SHARED_ROOT/$2" "$dest"
  if [ -d "$dest" ] && [ ! -e "$SHARED_ROOT/$2" ] && ! run /bin/mv "$dest" "$SHARED_ROOT/$2"; then
    echo "could not move $dest back to $SHARED_ROOT/$2: $(oneline "$OUT")" >&2
  fi
}

check_git() {
  local commit gd="$REPO/.git" before after admin gd_real p
  if [ "$SCRATCH_OK" -ne 1 ]; then
    record FAIL "$AGENT" git "cannot test: the scratch checkout and worktree were not created"
    return
  fi

  # (b) commits from both sides, interleaved, in the same worktree.
  commit='cd "$1" && echo "$2" >> "$2.txt" && git add "$2.txt" && git commit -q -m "check: $2"'
  if run as_agent /bin/sh -c "$commit" sh "$WT" "$AGENT"; then
    record PASS "$AGENT" git.agent-commit "agent committed in $WT"
  else
    record FAIL "$AGENT" git.agent-commit "$(oneline "$OUT")"
  fi
  if run /bin/sh -c "$commit" sh "$WT" "$OPERATOR"; then
    record PASS "$AGENT" git.operator-commit "operator committed after the agent"
  else
    record FAIL "$AGENT" git.operator-commit "$(oneline "$OUT")"
  fi
  if run as_agent /bin/sh -c "$commit" sh "$WT" "$AGENT"; then
    record PASS "$AGENT" git.agent-after-operator "agent committed again after the operator"
  else
    record FAIL "$AGENT" git.agent-after-operator "$(oneline "$OUT")"
  fi

  # (c) what decides the repository, config and hooks the operator's next git command in the
  # checkout or the worktree uses.
  expect_denied scratch.stage-closed "$STAGE" /bin/ls "$STAGE"
  admin=$(cd "$WT" && cd "$(sed -n 's/^gitdir: //p' .git)" && pwd -P)
  before=$(shasum "$gd/config" | cut -d' ' -f1)
  expect_denied checkout.config-append "$gd/config" \
    /bin/sh -c 'echo "[core] hooksPath = hooks-check" >> "$1"' sh "$gd/config"
  # Through sh, so a git the agent cannot resolve reads as "not found", never as a refusal.
  expect_denied checkout.config-command "$gd/config" \
    /bin/sh -c 'git -C "$1" config core.hooksPath hooks-check' sh "$WT"
  expect_denied checkout.config-delete "$gd/config" /bin/rm -f "$gd/config"
  expect_denied checkout.config-replace "$gd/config" \
    /bin/sh -c 'cp "$1" "$HOME/.check-config" && mv -f "$HOME/.check-config" "$1"; s=$?; rm -f "$HOME/.check-config"; exit $s' sh "$gd/config"
  expect_denied checkout.hook-add "$gd/hooks" /usr/bin/touch "$gd/hooks/pre-commit"
  expect_denied checkout.hook-edit "$gd/hooks/pre-commit.sample" \
    /bin/sh -c 'echo x >> "$1"' sh "$gd/hooks/pre-commit.sample"
  expect_denied checkout.hooks-rename "$gd/hooks" /bin/mv "$gd/hooks" "$gd/hooks-moved"
  expect_denied checkout.git-rename "$gd" /bin/mv "$gd" "$gd-moved"
  expect_denied checkout.rename "$REPO" /bin/mv "$REPO" "$REPO-moved"
  expect_denied worktree.add "$gd/worktrees" /bin/mkdir "$gd/worktrees/.check-added"
  expect_denied worktree.rename "$WT" /bin/mv "$WT" "$WT-moved"
  expect_denied worktree.gitfile-edit "$WT/.git" \
    /bin/sh -c 'echo "gitdir: $HOME" > "$1"' sh "$WT/.git"
  expect_denied worktree.admin-rename "$admin" /bin/mv "$admin" "$admin-moved"
  expect_denied worktree.commondir-edit "$admin/commondir" \
    /bin/sh -c 'echo "$HOME" > "$1"' sh "$admin/commondir"
  expect_denied worktree.gitdir-edit "$admin/gitdir" \
    /bin/sh -c 'echo "$HOME/.git" > "$1"' sh "$admin/gitdir"
  after=$(shasum "$gd/config" 2>/dev/null | cut -d' ' -f1)
  if [ "$before" = "$after" ]; then
    record PASS "$AGENT" checkout.config-intact "config unchanged after every attempt"
  else
    record FAIL "$AGENT" checkout.config-intact "config changed or missing"
  fi
  if run git -C "$REPO" config --get core.bare && run git -C "$REPO" status --short; then
    record PASS "$AGENT" checkout.operator-git "operator's git still reads and runs in the checkout"
  else
    record FAIL "$AGENT" checkout.operator-git "$(oneline "$OUT")"
  fi
  gd_real=$(cd "$gd" 2>/dev/null && pwd -P)
  for p in checkout:"$REPO" worktree:"$WT"; do
    if run git -C "${p#*:}" rev-parse --path-format=absolute --git-common-dir --git-path hooks \
      && [ "$OUT" = "$gd_real"$'\n'"$gd_real/hooks" ]; then
      record PASS "$AGENT" "${p%%:*}.operator-resolves" "operator's git still uses the checkout's own .git and hooks"
    else
      record FAIL "$AGENT" "${p%%:*}.operator-resolves" "expected $gd_real and its hooks: $(oneline "$OUT")"
    fi
  done
}

check_contain() {
  local tag="agent-users-check-sleeper-$$" sleeper
  # Double fork and setsid: the sleeper is in no session or process group its launcher owns,
  # so only a uid-wide kill reaches it.
  run as_agent /usr/bin/perl -MPOSIX -e 'exit if fork; POSIX::setsid(); exit if fork; close STDIN; close STDOUT; close STDERR; $0 = $ARGV[0]; sleep 600' "$tag"
  sleep 1
  sleeper=$(pgrep -U "$AGENT" -f "$tag" | head -1)
  if [ -z "$sleeper" ]; then
    record FAIL "$AGENT" contain.pkill-uid "cannot test: the detached sleeper did not start"
    return
  fi
  run ps -o pid=,sess=,pgid=,comm= -p "$sleeper"
  run as_agent /usr/bin/pkill -KILL -U "$AGENT"
  sleep 1
  if ps -p "$sleeper" >/dev/null 2>&1; then
    record FAIL "$AGENT" contain.pkill-uid "setsid'd sleeper $sleeper survived pkill -KILL -U"
  else
    run ps -U "$AGENT" -o pid=,comm=
    record PASS "$AGENT" contain.pkill-uid "setsid'd sleeper $sleeper killed"
  fi
}

# Scratch paths, removed on any exit.
SCRATCH=()
cleanup() {
  local p
  for p in "${SCRATCH[@]+"${SCRATCH[@]}"}"; do
    if { [ -e "$p" ] || [ -L "$p" ]; } && ! run rm -rf "$p"; then
      echo "could not remove $p: $(oneline "$OUT")" >&2
    fi
  done
  echo "$PASSED passed, $FAILED failed"
  echo "log: $LOG"
}
trap cleanup EXIT
trap 'exit 1' INT TERM HUP

AGENTS=()
gid=$(dscl . -read "/Groups/$AGENT_GROUP" PrimaryGroupID 2>/dev/null | awk '{print $2}')
if [ -n "$gid" ]; then
  for user in $({
    dscl . -read "/Groups/$AGENT_GROUP" GroupMembership 2>/dev/null | sed 's/^GroupMembership://' | tr ' ' '\n'
    dscl . -list /Users PrimaryGroupID | awk -v g="$gid" '$2 == g {print $1}'
  } | grep -v '^$' | sort -u); do
    AGENTS+=("$user")
  done
fi
if [ ${#AGENTS[@]} -eq 0 ]; then
  record FAIL - setup "no members of group $AGENT_GROUP; run setup.sh"
  exit 1
fi

# One scratch checkout under repos/, protected as a hosted checkout is, and a worktree of it
# under worktrees/. The leading dot keeps the names off any real checkout's. Where a check's
# attempt to move one of them succeeded, the moved copy is removed too.
SCRATCH_OK=0
REPO="$SHARED_ROOT/repos/.check-$$"
WT="$SHARED_ROOT/worktrees/.check-$$"
STAGE="$SHARED_ROOT/.check-stage-$$"
SCRATCH+=("$REPO-moved" "$WT-moved")

# publish <from> <to>: rename(2), which unlike mv never moves into a directory at <to>: it
# replaces an empty one and fails on anything else.
publish() {
  /usr/bin/perl -e 'rename($ARGV[0], $ARGV[1]) or die "rename $ARGV[0] to $ARGV[1]: $!\n"' "$1" "$2"
}

# The checkout and worktree are built, committed to and protected where no agent can reach them,
# and only then published: an agent that could write in the checkout before protect-checkout.sh
# ran could plant a hook or config that the operator's first commit would run. The stage is 0700
# in the shared root, which carries no ACL; its two trees carry the shared ACL, so what is made
# in them inherits exactly what it would have in place, and a rename keeps an ACL as it is.
# Relative worktree paths survive the move. The published names are reserved first with mkdir,
# which fails on a name that already exists.
stage_scratch() {
  local s_repo="$STAGE/repos/.check-$$" s_wt="$STAGE/worktrees/.check-$$"
  run mkdir -m 700 "$STAGE" || return 1
  SCRATCH+=("$STAGE")
  run mkdir "$STAGE/repos" "$STAGE/worktrees" \
    && run apply_shared_acl "$STAGE/repos" "$OPERATOR" \
    && run apply_shared_acl "$STAGE/worktrees" "$OPERATOR" \
    && run git init -q -b main "$s_repo" \
    && run /bin/sh -c 'echo check > "$1/README" && git -C "$1" add README && git -C "$1" commit -q -m "check: base"' sh "$s_repo" \
    && run git -C "$s_repo" worktree add -q --relative-paths -b "check-$$" "$s_wt" \
    && run "$SCRIPT_DIR/protect-checkout.sh" "$s_repo" \
    || return 1
  run mkdir "$REPO" || return 1
  SCRATCH+=("$REPO")
  run mkdir "$WT" || return 1
  SCRATCH+=("$WT")
  run publish "$s_repo" "$REPO" && run publish "$s_wt" "$WT"
}

# git init and git worktree add create missing parents; the trees must already be setup's.
if [ ! -d "$SHARED_ROOT/repos" ] || [ ! -d "$SHARED_ROOT/worktrees" ]; then
  record FAIL - git.scratch "$SHARED_ROOT/repos or $SHARED_ROOT/worktrees is missing; run setup.sh"
elif stage_scratch; then
  SCRATCH_OK=1
else
  record FAIL - git.scratch "could not create $REPO and $WT: $(oneline "$OUT")"
fi

for AGENT in "${AGENTS[@]}"; do
  if ! run as_agent /usr/bin/true; then
    record FAIL "$AGENT" setup "cannot run as $AGENT with sudo -n: $(oneline "$OUT")"
    continue
  fi
  check_access
  check_git
  check_contain
done

[ "$FAILED" -eq 0 ]
