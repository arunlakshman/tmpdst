// Package integrationtest holds the integration tests that drive real brokers (sys/prod
// wiring, the same as cmd/mdsbroker) with the real franz-go client (kgo).
// Hermetic tests that needed an in-process control plane skip until reconnect
// against external mds-controlplane (docs/cp-extraction.md). The full-stack
// test needs the docker backends (scripts/it-up.sh) and MDS_DYNAMO_ENDPOINT +
// MDS_S3_ENDPOINT. kgo is imported only here: never from a
// deterministic package (ops/tools/determinismcheck).
package integrationtest
