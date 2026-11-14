# Security Policy

## Supported versions

refset is pre-1.0. Only the latest release receives fixes.

## Reporting a vulnerability

Please report vulnerabilities privately using GitHub's [private vulnerability reporting](https://github.com/azbyluthfan/refset/security/advisories/new) for this repository. Don't open a public issue.

Include what you found, how to reproduce it, and the impact you expect. You should get an acknowledgement within a week. Once a fix is released, the advisory is published with credit to the reporter unless you prefer otherwise.

## Scope

The security model (what refset trusts, the threats it handles and how) is in [docs/security.md](docs/security.md).

Relevant areas include anything that could make a replica serve wrong or stale data while reporting it as fresh, ways a client could force excessive source or fallback load (for example through version headers), and weaknesses in the optional header signing in `refsethttp`.
