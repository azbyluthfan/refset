## What this changes

<!-- One change per pull request. Link the issue if there is one. -->

## Guarantees affected

<!-- Which of G1–G6 (docs/design.md §8) does this touch, and which test covers it? Write "none" for docs or tooling. -->

- [ ] G1 Consistency
- [ ] G2 Atomic batches
- [ ] G3 No lost changes
- [ ] G4 Read-your-writes
- [ ] G5 Bounded staleness
- [ ] G6 Convergence
- [ ] None

Covering test(s):

## Checklist

- [ ] Commits are signed off (`git commit -s`)
- [ ] `make ci` passes locally
- [ ] The library module still has zero dependencies
- [ ] Docs updated if behavior or API changed (`docs/design.md`, `docs/source-contract.md`, godoc)
- [ ] CHANGELOG.md updated under "Unreleased"
