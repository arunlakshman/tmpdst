// Package executor executes control-plane IOCalls outside Step: log slots and
// object-store checkpoints. It owns the ports (Log and ObjectStore), the
// in-memory test versions of them, and the Executor that runs the calls.
//
// The real log implementations are separate packages, so that a package that
// needs only the Log interface does not pull in a cloud SDK or a SQL driver:
//   - executor/dynamolog: the DynamoDB log (AWS SDK).
//   - executor/sqllog: the Postgres log (pgx).
//   - executor/logtest: the conformance suite of Log. Every implementation runs it
//     from its own package.
//
// The AWS SDK stays out of controlplane/replica (decision 56). The rules are
// checked by architecture_test.go in the parent folder.
package executor
