---
name: New Source backend
about: Propose an example or community Source for another store
labels: source
---

## Store

<!-- Which system (MySQL, NATS KV, S3, HTTP service, ...)? -->

## How it meets the Source contract

<!-- docs/source-contract.md. For each requirement, explain how the store provides it. -->

- C1 consistent snapshot at a version:
- C2 gap-free versions in commit order:
- C3 changes up to head, values at head:
- C4 ErrSnapshotRequired when history is gone:
- C5 best-effort Watch:
- C6 KeyLoader (optional):
- C7 stable Name:

## Where it would live

- [ ] examples/ in this repository
- [ ] A separate community module (link it from the README)
