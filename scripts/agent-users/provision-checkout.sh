#!/bin/bash
# Create a hosted checkout: clone <origin-url> to /Users/Shared/weft/repos/<name>, protected by
# protect-checkout.sh before any agent can reach it. Run as the operator, who owns the checkout,
# without sudo:
#
#   scripts/agent-users/provision-checkout.sh <origin-url>
#
# A local origin must be an absolute path. <name> is the project tk derives from the clone's
# remote.origin.url, so tk resolves the checkout, and every worktree of it, to the project its
# directory is named for. An existing <name> is refused: re-protecting a checkout is
# protect-checkout.sh's job. See docs/agent-users.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

if [ "$(id -u)" -eq 0 ]; then
  echo "run as the operator, not root" >&2
  exit 1
fi
if [ $# -ne 1 ]; then
  echo "usage: $0 <origin-url>" >&2
  exit 64
fi
origin="$1"
OPERATOR=$(id -un)

# The operator's cwd may be somewhere the agents cannot traverse; nothing here depends on it.
cd /

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  printf '%s' "${s%"${s##*[![:space:]]}"}"
}

# tk's projectFromGitRemote (ticket internal/project/resolve.go): trim, drop one trailing .git
# and then one trailing /, take what follows the last / or else the last :, trim again.
remote=$(trim "$origin")
remote="${remote%.git}"
remote="${remote%/}"
case "$remote" in
  */*) name="${remote##*/}" ;;
  *:*) name="${remote##*:}" ;;
  *) name="$remote" ;;
esac
name=$(trim "$name")
# tk's ValidName.
case "$name" in
  "" | . | .. | */*)
    echo "refusing $origin: it names no project tk can resolve (derived name: \"$name\")" >&2
    exit 1
    ;;
esac

repos="$SHARED_ROOT/repos"
dest="$repos/$name"
if [ ! -d "$repos" ]; then
  echo "$repos is not a directory; run setup.sh" >&2
  exit 1
fi
if [ -e "$dest" ] || [ -L "$dest" ]; then
  echo "refusing $dest: it already exists; protect-checkout.sh re-protects an existing checkout" >&2
  exit 1
fi

# Built and protected in a 0700 stage in the shared root, which carries no ACL, and only then
# published, as check.sh's stage_scratch does: an agent that could write in the checkout before
# protect-checkout.sh ran could plant a hook or config the operator's first git command would run.
# The stage's repos/ carries the shared ACL, so the clone inherits exactly what it would in place,
# and a rename keeps an ACL as it is. --no-hardlinks: a local origin's object files would
# otherwise be hard links whose inodes never inherit it.
stage="$SHARED_ROOT/.provision-$name-$$"
staged=0
reserved=0
published=0
cleanup() {
  if [ "$staged" -eq 1 ]; then
    rm -rf "$stage"
  fi
  # rmdir, never rm -r: the reservation is removed only while still empty.
  if [ "$reserved" -eq 1 ] && [ "$published" -eq 0 ]; then
    rmdir "$dest" || echo "could not remove the reserved $dest" >&2
  fi
}
trap cleanup EXIT
trap 'exit 1' INT TERM HUP

mkdir -m 700 "$stage"
staged=1
mkdir "$stage/repos"
apply_shared_acl "$stage/repos" "$OPERATOR"
GIT_TERMINAL_PROMPT=0 git clone -q --no-hardlinks -- "$origin" "$stage/repos/$name"
"$SCRIPT_DIR/protect-checkout.sh" "$stage/repos/$name"

# mkdir fails on a name that already exists, so the publish never lands on another checkout.
mkdir "$dest"
reserved=1
publish "$stage/repos/$name" "$dest"
published=1

echo "published: $dest"
echo "bind it in shuttle's config.json; the daemon reads config.json at start, so restart it after"
echo "(shuttle weft bind <origin-url> runs this script, then binds, checks and restarts):"
echo "  \"weft\": {\"repositories\": {\"$name\": \"$dest\"}}"
