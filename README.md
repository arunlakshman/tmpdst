# MDS

MDS is a Kafka-protocol message broker and control plane, written in Go. Clients speak Kafka wire protocol to `mdsbroker`; the broker talks to `mdscontrolplane` for metadata and classic consumer groups. The same repo includes `dst`, a library for deterministic simulation testing.

## Quickstart

With [Podman](https://podman.io/) installed (on macOS: `podman machine start`), from the repo root:

```sh
make image-all
podman run --rm --name mds-all \
  -p 9092:9092 -p 8080:8080 -p 9100:9100 -p 9101:9101 \
  mds-all:dev
```

Point a Kafka client at `127.0.0.1:9092`. Full steps, smoke checks, and caveats (in-memory demo only): [docs/getting-started.md](docs/getting-started.md).

## Kafka surface

Supported APIs, classic groups, and intentional gaps:
[docs/architecture.md](docs/architecture.md).

## Modules and build

Four Go modules (`dst` ← `shared` ← `controlplane` ← `broker`). A module may import only modules to its left; `make check-deps` enforces that.

```sh
mise install       # go, golangci-lint, buf, …; installs the pre-commit hook
make generate      # required before standalone broker/controlplane builds
make build         # full gate for all modules, then bin/mdscontrolplane and bin/mdsbroker
```

`make build` also runs `make check-modules-standalone`. Module builds (and the commit hook) run Podman integration tests where applicable. See [CONTRIBUTING.md](CONTRIBUTING.md) for `SKIP_BUILD_HOOK` and contributor docs.

## Links

- [docs/](docs/) — getting started, architecture, operations, contribute
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CHANGELOG.md](CHANGELOG.md)
- [LICENSE](LICENSE) (Apache-2.0)
