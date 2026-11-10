#!/usr/bin/env bash
# Compares the library's core benchmarks between a base revision (default:
# the latest release tag) and the working tree, on this machine, with
# benchstat. Run before a release to catch performance regressions:
#
#   scripts/bench-compare.sh            # against the latest vX.Y.Z tag
#   scripts/bench-compare.sh main       # against another revision
#
# BENCH (regexp) and COUNT override which benchmarks run and how often.
set -euo pipefail

BENCHSTAT_VERSION=v0.0.0-20261009192801-be2c69fb417e
BENCH="${BENCH:-BenchmarkGetParallelIdle|BenchmarkApplyDeltas/entries=100000/|BenchmarkOpen}"
COUNT="${COUNT:-6}"
export GOWORK=off

root="$(git rev-parse --show-toplevel)"
base="${1:-$(git -C "$root" describe --tags --abbrev=0 --match 'v[0-9]*')}"
out="$(mktemp -d)"
trap 'git -C "$root" worktree remove --force "$out/base" >/dev/null 2>&1 || true; rm -rf "$out"' EXIT

echo "Base: $base ($(git -C "$root" rev-parse --short "$base"))  Head: working tree"
git -C "$root" worktree add --detach --quiet "$out/base" "$base"

run() { # dir, file to append results to
	(cd "$1" && go test -run '^$' -bench "$BENCH" -benchmem -count 1 . >>"$2")
}
# One run of each side per round, alternating, so drift on the machine
# (heat, background work) affects both sides alike.
for i in $(seq 1 "$COUNT"); do
	echo "== round $i of $COUNT"
	run "$out/base" "$out/base.txt"
	run "$root" "$out/head.txt"
done

go run "golang.org/x/perf/cmd/benchstat@${BENCHSTAT_VERSION}" base="$out/base.txt" head="$out/head.txt"
