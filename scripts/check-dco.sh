#!/usr/bin/env bash
# Checks that every commit in the given range carries a DCO sign-off
# (a "Signed-off-by:" trailer). Usage: check-dco.sh <base-sha> <head-sha>
set -euo pipefail

base="${1:?usage: check-dco.sh <base-sha> <head-sha>}"
head="${2:?usage: check-dco.sh <base-sha> <head-sha>}"

missing=0
for sha in $(git rev-list --no-merges "${base}..${head}"); do
  if ! git log -1 --format='%(trailers:key=Signed-off-by,valueonly)' "$sha" | grep -q .; then
    echo "Missing Signed-off-by: $(git log -1 --format='%h %s' "$sha")" >&2
    missing=1
  fi
done

if [ "$missing" -ne 0 ]; then
  echo >&2
  echo "Sign off your commits with 'git commit -s' (see CONTRIBUTING.md)." >&2
  echo "To fix the last commit: git commit --amend -s --no-edit" >&2
  exit 1
fi

echo "OK: all commits are signed off."
