#!/bin/bash
# Deny the agent users the paths that decide which config and hooks git uses as whoever runs it
# next in a hosted checkout or one of its linked worktrees:
#   - .git/config: no write, append or delete, so none renamed over it or away;
#   - .git/hooks and every file in it, and any hook later created in it;
#   - the checkout and .git, so neither can be swapped for another by rename;
#   - .git/worktrees and, for each linked worktree, its admin directory, the commondir and gitdir
#     in it, the worktree directory and its .git file.
# Agents can still add files at the top of .git, which the files ref backend needs for
# packed-refs.lock; one of them can be a .git/commondir that redirects the operator's git in the
# checkout itself (docs/agent-users.md, What it does not protect). Nothing is inherited by name,
# so run this as the operator, who owns the checkout, without sudo: before any agent can reach the
# checkout, again after every git worktree add, move or repair, and again after every config write
# by the operator's git in the checkout, which renames a new .git/config over the denied one:
#
#   scripts/agent-users/protect-checkout.sh /Users/Shared/weft/repos/<name>
#
# Idempotent: an ACE already present is left alone, so a re-run adds nothing. Prints one
# "changed: ..." line per ACE it adds. See docs/agent-users.md.
set -euo pipefail

source "$(dirname "$0")/common.sh"

if [ "$(id -u)" -eq 0 ]; then
  echo "run as the operator, not root" >&2
  exit 1
fi
if [ $# -ne 1 ]; then
  echo "usage: $0 <checkout under $SHARED_ROOT>" >&2
  exit 64
fi
root=$(cd "$SHARED_ROOT" && pwd -P)
checkout=$(cd "$1" && pwd -P)
case "$checkout" in
  "$root"/?*) ;;
  *) echo "refusing $checkout: not under $root" >&2; exit 1 ;;
esac
refuse() {
  echo "refusing ${1#"$root"/}: $2" >&2
  exit 1
}
git_dir="$checkout/.git"
if [ -L "$git_dir" ] || [ ! -d "$git_dir" ]; then
  refuse "$git_dir" "not a directory"
fi
# chmod +a follows a symlink, which would move the deny onto the link's target.
if [ -L "$git_dir/config" ] || [ ! -f "$git_dir/config" ]; then
  refuse "$git_dir/config" "missing or not a regular file"
fi
if [ -L "$git_dir/hooks" ]; then
  refuse "$git_dir/hooks" "a symlink"
fi
# With worktreeConfig, git also reads config.worktree from each linked worktree's admin
# directory, where agents must be able to add files.
if [ "$(git config --file "$git_dir/config" --type=bool --get extensions.worktreeConfig || true)" = true ]; then
  refuse "$git_dir/config" "extensions.worktreeConfig is set"
fi
CHANGES=0
# A missing hooks/ is one an agent could create; it must exist to carry the deny.
if [ ! -d "$git_dir/hooks" ]; then
  mkdir "$git_dir/hooks"
  echo "changed: created .git/hooks"
  CHANGES=$((CHANGES + 1))
fi

ref=$(mktemp -d)
trap 'rm -rf "$ref"' EXIT
touch "$ref/file"
mkdir "$ref/dir" "$ref/keep" "$ref/new"
chmod +a "group:$AGENT_GROUP deny $FILE_DENY" "$ref/file"
chmod +a "group:$AGENT_GROUP deny $DIR_DENY" "$ref/dir"
chmod +a "group:$AGENT_GROUP deny $KEEP_DENY" "$ref/keep"
chmod +a "group:$AGENT_GROUP deny $NEW_FILE_DENY" "$ref/new"
file_ace=$(ace_lines "$ref/file")
dir_ace=$(ace_lines "$ref/dir")
keep_ace=$(ace_lines "$ref/keep")
new_ace=$(ace_lines "$ref/new")

# deny <path> <perms> <ace as ls renders it>. The owner of a path can rewrite its ACL, so a
# deny binds only on what the operator owns: anything an agent made is refused instead.
uid=$(id -u)
deny() {
  if [ -L "$1" ] || [ ! -e "$1" ]; then
    refuse "$1" "missing or a symlink"
  fi
  if [ "$(stat -f %u "$1")" != "$uid" ]; then
    refuse "$1" "not owned by $(id -un)"
  fi
  if ! grep -qxF "$3" <<<"$(ace_lines "$1")"; then
    chmod +a "group:$AGENT_GROUP deny $2" "$1"
    echo "changed: deny ${1#"$root"/}"
    CHANGES=$((CHANGES + 1))
  fi
}
# realdir <dir>: its physical path, or nothing.
realdir() { (cd "$1" 2>/dev/null && pwd -P) || true; }

deny "$checkout" "$KEEP_DENY" "$keep_ace"
deny "$git_dir" "$KEEP_DENY" "$keep_ace"
deny "$git_dir/config" "$FILE_DENY" "$file_ace"
deny "$git_dir/hooks" "$DIR_DENY" "$dir_ace"
deny "$git_dir/hooks" "$NEW_FILE_DENY" "$new_ace"
for f in "$git_dir/hooks"/* "$git_dir/hooks"/.[!.]*; do
  [ -e "$f" ] || [ -L "$f" ] || continue
  if [ -L "$f" ] || [ ! -f "$f" ]; then
    refuse "$f" "not a regular file"
  fi
  deny "$f" "$FILE_DENY" "$file_ace"
done

# Linked worktrees. git in a worktree follows its .git file to the admin directory, and the
# admin directory's commondir to the config and hooks; each is checked against what git worktree
# add writes only once denied, when an agent can no longer change it. Paths come from the files,
# not from names: git renames an admin directory whose worktree name it rejects.
if [ -e "$git_dir/worktrees" ] || [ -L "$git_dir/worktrees" ]; then
  deny "$git_dir/worktrees" "$DIR_DENY" "$dir_ace"
  for admin in "$git_dir/worktrees"/* "$git_dir/worktrees"/.[!.]*; do
    [ -e "$admin" ] || [ -L "$admin" ] || continue
    [ -d "$admin" ] || refuse "$admin" "not a directory"
    deny "$admin" "$KEEP_DENY" "$keep_ace"
    deny "$admin/commondir" "$FILE_DENY" "$file_ace"
    deny "$admin/gitdir" "$FILE_DENY" "$file_ace"
    [ "$(cat "$admin/commondir")" = "../.." ] || refuse "$admin/commondir" "does not name ../.."
    gitfile=$(cat "$admin/gitdir")
    case "$gitfile" in
      /*) ;;
      *) gitfile="$admin/$gitfile" ;;
    esac
    [ "$(basename "$gitfile")" = .git ] || refuse "$admin/gitdir" "does not name a .git file"
    worktree=$(realdir "$(dirname "$gitfile")")
    case "$worktree" in
      "$root"/?*) ;;
      *) refuse "$admin/gitdir" "names a worktree that is missing or not under $root" ;;
    esac
    deny "$worktree" "$KEEP_DENY" "$keep_ace"
    deny "$worktree/.git" "$FILE_DENY" "$file_ace"
    [ -f "$worktree/.git" ] || refuse "$worktree/.git" "not a regular file"
    target=$(sed -n 's/^gitdir: //p' "$worktree/.git")
    case "$target" in
      /*) ;;
      *) target="$worktree/$target" ;;
    esac
    [ "$(realdir "$target")" = "$admin" ] || refuse "$worktree/.git" "does not name $admin"
  done
fi
[ "$CHANGES" -gt 0 ] || echo "nothing changed: $checkout already protected"
