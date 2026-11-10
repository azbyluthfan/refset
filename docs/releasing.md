# Releasing

refset is a Go module, so a release is a tag. Pushing a `vX.Y.Z` tag runs [`release.yml`](../.github/workflows/release.yml), which:

1. checks that the tag is `vX.Y.Z`, is on `main`, has a dated section in `CHANGELOG.md`, and that the library module still has no dependencies;
2. runs the library tests with the race detector on Go 1.26 and 1.27, govulncheck, and the examples against Postgres 15 and 17;
3. creates the GitHub release with that CHANGELOG section as its notes (`v0.x` and versions with a suffix such as `-rc.1` are marked pre-release);
4. fetches the version through `proxy.golang.org`, so `go get` and pkg.go.dev see it right away.

If any step fails, nothing is published. Fix the problem on `main`, delete the tag (`git push origin :refs/tags/vX.Y.Z`) and tag again.

## Versions

The project follows [Semantic Versioning](https://semver.org/). Until v1.0.0, a minor version (`v0.2.0`) may break the API and a patch version (`v0.1.1`) may not. Only the root module is tagged; `examples/` and `bench/` are not released.

## Before tagging

1. **Weekly run is green.** Run `weekly` from the Actions tab ("Run workflow") on the commit you will tag, or check the latest scheduled run. It covers what the release itself doesn't: the soak, long fuzzing and the newest Go and Postgres.
2. **Benchmarks, if the core changed.** Run `make bench-compare` (core benchmarks of the last release against the working tree) and `make perfreport` (latency, GC and memory under load), and update [benchmarks.md](benchmarks.md) if anything moved. Before a minor release, also run a 1-hour soak locally (`make soak SOAK_DURATION=1h`) and update the soak section of [benchmarks.md](benchmarks.md).
3. **CHANGELOG.md.** Rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and add an empty `## [Unreleased]` above it. Breaking changes go under a `### Changed` heading that says what to change in calling code.
4. **Docs.** Check that the README, [roadmap.md](roadmap.md) and godoc examples match the release.
5. Merge that through a pull request, like any other change.

## Tagging

```bash
git switch main && git pull --ff-only
git tag -s vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

`-s` signs the tag so GitHub shows it as Verified. With an SSH key registered on GitHub as a signing key, point Git at it once (no GPG needed):

```bash
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
```

Use `-a` instead of `-s` for an unsigned tag. Then watch the `Release` run in the Actions tab.

## After the release

- Check the release page and `https://pkg.go.dev/github.com/azbyluthfan/refset@vX.Y.Z`.
- Set the refset `require` line in `examples/go.mod` and `bench/go.mod` to the new version (they keep their `replace`, so this only records which release they were last checked with), and check the examples still build without the `replace`.
- Move finished items in [roadmap.md](roadmap.md).
- If a published version is broken, don't delete it: the module proxy keeps it forever. Release a fix, and add a `retract` directive for the bad version to `go.mod` in that release.
