//go:build integration

// Package integrationtest holds the integration tests that drive real brokers (sys/prod
// wiring, the same as cmd/mdsbroker) with the real franz-go client (kgo).
//
// All tests (make it, part of make build) start mdscontrolplane against DynamoDB
// Local and SeaweedFS from shared/testinfra (Podman only) and put segment objects
// in that S3. They never skip for a missing backend.
//
// kgo is imported only here: never from a deterministic package
// (ops/tools/determinismcheck).
package integrationtest
