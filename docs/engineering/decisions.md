# Decisions (0.1.0 index)

Still-true, load-bearing decisions for OSS 0.1.0. Numbers are historical labels,
not a sequence you need to read in order.

| # | Decision | Where enforced |
|---|----------|----------------|
| I | Deterministic packages use an exact-path import allow-list (fail closed); third-party modules need an audit record under `docs/*/dep-audits/`. | `dst/cmd/determinismcheck`; `ops/*/determinism-allow.txt`; `ops/*/scripts/ci-determinism.sh`; [`contribute/determinism.md`](../contribute/determinism.md) |
| 36 | Metrics are facts counted at the edge; Prometheus stays out of the core / determinism closure. | `internal/system/production.Metrics`; CP `controlplanemetrics`; [`operations/observability.md`](../operations/observability.md) |
| 45 | Compression is opaque bytes: validate header/CRC only; never decompress in the broker. | `broker.checkBatch`; `kafkawire`; [`kafka-compatibility.md`](../kafka-compatibility.md) |
| 48 / 63 | Sticky appender: commits go to one preferred CP writer; one writer at a time via epoch claim + fence entry + log floor. | CP replica / log append path |
| 60 | Classic consumer groups only (no KIP-848); group state lives in the control plane; brokers stay stateless. | Group RPC forwarding; [`what-works-in-0.1.0.md`](../what-works-in-0.1.0.md); [`kafka-compatibility.md`](../kafka-compatibility.md) |
| 64 | Log backends: memory, DynamoDB, and PostgreSQL (`-store mem` / `dynamolog` / `postgres`). | `executor.Log` backends; [`operations/run.md`](../operations/run.md) |
