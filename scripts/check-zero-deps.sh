#!/usr/bin/env bash
# Fails if the library module (repository root) has any dependency.
# The library must use only the Go standard library, including in tests.
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
  echo "$1" >&2
  echo "Move code that needs dependencies to examples/ or bench/ (separate modules)." >&2
  exit 1
}

# Text check first: works offline and gives a clear message.
if grep -Eq '^[[:space:]]*(require|replace)\b' go.mod; then
  fail "go.mod contains a require or replace directive; the library must have zero dependencies."
fi

# Module graph check: catches anything the text check could miss.
deps="$(GOWORK=off go list -m all | tail -n +2)"
if [ -n "$deps" ]; then
  fail "The library module must have zero dependencies, but it requires: ${deps}"
fi

echo "OK: library module has zero dependencies."
