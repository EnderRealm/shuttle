#!/bin/sh
# Holds a built release binary to what the updater and containment rely on,
# after the build and before anything is archived or published: cgo on, or
# containment fails closed on the host (docs/releasing.md), and the tag
# stamped in, or the updater cannot confirm the build it installed.
set -eu
binary=$1
tag=$2
if ! go version -m "$binary" | grep -q 'CGO_ENABLED=1'; then
  echo "$binary was built without cgo: containment would fail closed on the host" >&2
  exit 1
fi
reported=$("$binary" version)
if [ "$reported" != "$tag" ]; then
  echo "$binary reports version $reported, not $tag" >&2
  exit 1
fi
