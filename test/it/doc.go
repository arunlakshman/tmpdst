// Package it holds the integration tests that drive real brokers (sys/prod
// wiring, the same as cmd/manibroker) with the real franz-go client (kgo).
// Hermetic tests run in-process over loopback with the memstore control plane
// and an in-memory object store. The full-stack test needs the docker
// backends (scripts/it-up.sh) and MANIFESTER_DYNAMO_ENDPOINT +
// MANIFESTER_S3_ENDPOINT. kgo is imported only here: never from a
// deterministic package (scripts/ci-determinism.sh).
package it
