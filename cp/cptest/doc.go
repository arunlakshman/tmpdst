// Package cptest holds the control-plane tests only: handler and client round
// trips, conformance C3 (stores and the full HTTP stack against
// veritas.ControlPlane) and the DynamoDB concurrency tests. DynamoDB tests run
// when MANIFESTER_DYNAMO_ENDPOINT is set (scripts/it-up.sh).
package cptest
