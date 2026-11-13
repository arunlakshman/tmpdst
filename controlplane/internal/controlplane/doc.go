// Package controlplane is the control-plane service tree. The binary
// cmd/mdscontrolplane serves gRPC through server, which runs the replica and the
// rebalance machine on dst/live. Brokers call the gRPC API
// (api/controlplane/v1) with their own client.
//
// Tests sit next to the code they test. Two exceptions: the model conformance
// tests are in test/conformance, and the end-to-end tests of the real binary are
// in test/integration (build tag integration, run by make it).
//
// # Generated files
//
// The .proto files in api/ are the source. make generate (buf, with the plugin
// ops/tools/protoc-gen-mdswire) writes the *.pb.go files, ../../contract/wire_types_gen.go,
// domain/rebalance/wire_types_gen.go and wire/wire_gen.go. None of them is in
// git, and the module does not compile without them. make build runs it first.
//
// # Map of the packages
//
//   - api/controlplane/v1: the gRPC service and its messages (controlplane.proto).
//   - api/controlplane/shared/v1: messages that the service and the log both use.
//   - api/controlplane/log/v1: the log entry.
//   - api/controlplane/options/v1: custom options that say how a message becomes
//     a contract value type (read by the plugin).
//   - ../../contract (outside this tree, public because the broker imports it): the
//     plain value types of every command, read and answer (mostly generated), the
//     sealed sums Operation and Response with their Tag and Fold, and ErrCode.
//   - wire: the only place that converts contract types to and from the api
//     messages. Nearly all generated (wire_gen.go). By hand: the error code table
//     (errcode.go), the gRPC error of a failed call (rpcerror.go) and the group
//     call mapping for the rebalance machine (rebalance.go).
//   - domain/topic: topics and segments. Commands CreateTopic and CommitSegment,
//     reads FindSegments and DescribeTopics.
//   - domain/group: the logged state of consumer groups (groups, members,
//     offsets). Commands GroupInstall, GroupAssign, GroupRemove, OffsetCommit,
//     and the group reads.
//   - domain/rebalance: the consumer group protocol (join, sync, heartbeat,
//     leave and so on) as a pure state machine. See docs/rebalance.md.
//   - domain/broker: which brokers are alive (leases). Memory only, never logged.
//   - metadb: the database. It runs commands on schemas, owns the log entry
//     format, the checkpoint and the state hash.
//   - metadb/engine: the ordered key-value interface under metadb.
//     metadb/engine/memory and metadb/engine/pebble implement it.
//   - replica: one node's copy of the replicated log: writer or standby,
//     catch-up, checkpoint and restore. A dst machine.Machine.
//   - executor: runs the log and object-store calls that replica asks for. It
//     owns the Log and ObjectStore ports. executor/dynamolog and executor/sqllog
//     are the DynamoDB and Postgres logs; executor/logtest is their shared tests.
//   - server: the gRPC adapter and the node's life (node.go). One handler file
//     per area: topics_handler.go, groups_handler.go, brokers_handler.go,
//     status_handler.go.
//   - controlplanemetrics: Prometheus instrumentation.
//   - outside this tree: internal/runtime (event and value types, Clock),
//     dst/live (the production loop of a dst machine; it is in the dst module),
//     spec (the independent model), internal/verify/simulator (the
//     deterministic simulation, run by cmd/cpsim).
//
// # Mental model
//
// engine is like InnoDB, and metadb is like the MySQL server on top of it. A
// schema (topic, group) owns its tables, which are key prefixes in the engine.
// metadb sends each command or read to its schema. One difference from a normal
// database: durability is in the replicated log, not in the engine. The engine
// can be thrown away and rebuilt from the log and a checkpoint.
//
// Each schema has the same two steps:
//
//	Decide(view, command) -> events, error code   pure rules, writes nothing
//	Apply(txn, event)                             writes the event to the tables
//
// There are no aggregates, repositories or per-command services. The log puts
// every command in one order, so nothing else is needed for concurrency.
//
// Commands are in the log, events are in memory. The log holds only commands.
// What a command does (for example the offsets of a CommitSegment) is decided
// when it is applied, on every replica. The events are made at that time and
// are never stored. A writer that decides outcomes before it appends works from
// stale state: that is the planted bug writer-offsets.
//
// The rebalance machine is a pure dst machine.Machine state machine (Step takes
// one input and returns outputs; time and chance come from the step context).
// server.Node hosts it in a second live.Run loop beside the replica. Its log
// writes and reads go to the replica as inputs, and the answers come back as
// inputs. The simulator hosts it the same way (a second dst process per node).
//
// # How they fit together, from the imports
//
//   - server imports live, replica, executor, metadb, domain/rebalance,
//     domain/broker, wire and contract.
//   - replica imports metadb. metadb imports domain/topic and domain/group.
//     domain/topic and domain/group import engine, and not each other or metadb.
//   - executor imports only runtime. dynamolog and sqllog import executor.
//   - domain/rebalance imports contract and runtime, and the dst packages. It
//     does not import replica or metadb.
//   - spec (a public package) shares types with the code (contract) and never
//     logic: it imports none of domain, metadb or engine.
//
// Time: all time comes from one interface, machine.Clock, or from the step
// context of a dst process. shared/clock has the production
// clock (Real) and the test clock (Fake). The domain and server packages may not call
// time.Now, time.AfterFunc and the like.
// The import rules are checked by architecture_test.go in this folder. The
// deterministic packages are checked by ops/tools/determinismcheck
// (make determinism); server, executor and metadb are not in that gate.
package controlplane
