#!/bin/bash
# Remove everything setup.sh creates: the sudoers file, every weftagent<N> user with its
# processes and home, the weftagents group and the shared root. The shared root holds the hosted
# checkouts and worktrees, which may carry work not yet landed, so without --yes this only
# prints what it would remove:
#
#   scripts/agent-users/teardown.sh             # print what it would remove
#   sudo scripts/agent-users/teardown.sh --yes  # remove it
#
# setup.sh rebuilds all of it from nothing. See docs/agent-users.md.
set -uo pipefail

source "$(dirname "$0")/common.sh"
PATH=/usr/bin:/bin:/usr/sbin:/sbin
export PATH

if [ $# -gt 1 ] || { [ $# -eq 1 ] && [ "$1" != --yes ]; }; then
  echo "usage: $0 [--yes]" >&2
  exit 64
fi

users=()
for user in $(dscl . -list /Users | grep -E "^$AGENT_PREFIX[0-9]+\$"); do
  users+=("$user")
done

echo "would remove:"
for user in "${users[@]+"${users[@]}"}"; do
  echo "  user $user, its processes and /Users/$user"
done
[ -e "$SUDOERS_FILE" ] && echo "  $SUDOERS_FILE"
dscl . -read "/Groups/$AGENT_GROUP" >/dev/null 2>&1 && echo "  group $AGENT_GROUP"
if [ -e "$SHARED_ROOT" ]; then
  echo "  $SHARED_ROOT, including:"
  for tree in "${SHARED_TREES[@]}"; do
    for entry in "$SHARED_ROOT/$tree"/* "$SHARED_ROOT/$tree"/.[!.]*; do
      [ -e "$entry" ] && echo "    $entry"
    done
  done
fi

if [ $# -eq 0 ]; then
  echo "refusing: nothing removed; re-run with sudo and --yes" >&2
  exit 1
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "run with sudo: sudo $0 --yes" >&2
  exit 1
fi

# The rule goes first, so nothing launches as an agent between the kill and the delete.
rm -f "$SUDOERS_FILE"
# A process that outlived its user would run as a deleted uid, and setup.sh numbers new users
# above the highest uid, so the rebuilt agent would usually get that uid back with the process.
# A user whose processes survive is kept, and reported below.
for user in "${users[@]+"${users[@]}"}"; do
  if ! kill_agent "$user"; then
    echo "teardown: processes running as $user survived SIGKILL; $user and /Users/$user not deleted" >&2
    continue
  fi
  sysadminctl -deleteUser "$user"
  rm -rf "/Users/$user"
done
if dscl . -read "/Groups/$AGENT_GROUP" >/dev/null 2>&1; then
  dseditgroup -o delete "$AGENT_GROUP"
fi
rm -rf "$SHARED_ROOT"

remaining=0
for user in "${users[@]+"${users[@]}"}"; do
  if id "$user" >/dev/null 2>&1 || [ -e "/Users/$user" ]; then
    echo "REMAINS: $user"
    remaining=1
  fi
done
if dscl . -read "/Groups/$AGENT_GROUP" >/dev/null 2>&1; then
  echo "REMAINS: group $AGENT_GROUP"
  remaining=1
fi
for path in "$SUDOERS_FILE" "$SHARED_ROOT"; do
  if [ -e "$path" ]; then
    echo "REMAINS: $path"
    remaining=1
  fi
done
[ "$remaining" -eq 0 ] && echo "removed: ${#users[@]} agent users and their homes, $SUDOERS_FILE, group $AGENT_GROUP, $SHARED_ROOT"
exit "$remaining"
