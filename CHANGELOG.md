# Changelog

## 0.1.0

### Added

- Kafka-protocol broker (`mdsbroker`) and control plane (`mdscontrolplane`).
- Classic consumer groups via the control plane.
- Demo all-in-one image `mds-all` (in-memory stores; not for production).
- Deterministic simulation library (`dst`).

### Known limitations

See [docs/architecture.md](docs/architecture.md) for the supported Kafka API surface and intentional gaps.
