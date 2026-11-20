# MDS documentation

Open-source `v0.1.0` docs. Few pages on purpose.

## Use

- [Getting started](getting-started.md) - Podman + `mds-all`
- [What works in 0.1.0](what-works-in-0.1.0.md) - APIs, groups, gaps, Kafka diffs
- [Architecture](architecture.md) - runtime picture, modules, terms
- [Operations](operations.md) - run, metrics, containers

## Contribute

- [Build](contribute/build.md) - mise, gates, risk score
- [Determinism](contribute/determinism.md) - dst, allow-list, audits, campaigns

Determinism CI reads audit records under `docs/broker/dep-audits/` and
`docs/controlplane/dep-audits/`. Do not move those paths without updating the
scripts.
