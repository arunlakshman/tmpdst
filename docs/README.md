# MDS documentation

Docs for the open-source `v0.1.0` launch. Start with getting started if you want
to run something. Use what-works and kafka-compatibility before you assume a
Kafka feature is present.

## Users

- [Getting started](getting-started.md)  -  Podman + `mds-all` quickstart
- [What works in 0.1.0](what-works-in-0.1.0.md)  -  supported Kafka APIs and gaps
- [Architecture](architecture.md)  -  broker, control plane, stores, modules
- [Kafka compatibility](kafka-compatibility.md)  -  API ranges and intentional diffs

## Ops

- [Run](operations/run.md)  -  binaries, flags, stores, objects
- [Observability](operations/observability.md)  -  metrics, logs, `run_id`
- [Deploy](operations/deploy.md)  -  separate images; kind + helm pointer

## Contribute

- [Build](contribute/build.md)  -  mise, `make build`, Podman, hooks
- [Determinism](contribute/determinism.md)  -  why, allow-list, audits, sim
- [Risk gate](contribute/risk-gate.md)  -  change-risk threshold

## Reference

- [Glossary](reference/glossary.md)
- [DST](reference/dst.md)  -  deterministic simulation library

## Engineering

- [Engineering index](engineering/README.md)  -  not required for users
- [Decisions](engineering/decisions.md)  -  still-true ADR index
- [Archive](engineering/archive/)  -  frozen pre-launch design notes

Ops and contribute pages may land in parallel with this spine. If a link 404s,
check the module READMEs or the paths still under `docs/broker/` and
`docs/controlplane/` until the migration finishes.

Determinism CI still reads audit records under `docs/broker/dep-audits/` and
`docs/controlplane/dep-audits/`. Do not move those paths.
