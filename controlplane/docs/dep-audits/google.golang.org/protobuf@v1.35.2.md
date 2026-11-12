# google.golang.org/protobuf@v1.35.2

- Date / auditor: 2026-11-05, T4.4 of docs/greenfield-refactor-plan.md (log entries in protobuf).
- Requires Go: 1.21 (go.mod of the module). The repo is on 1.26, so nothing moves.
- Verdict: **ACCEPTED WITH RULES**, for the log entry codec in `internal/controlplane/metadb`
  (`entry.go`, `entry_codec.go`) only.
- Import paths allowed (`ops/determinism-allow.txt`): `google.golang.org/protobuf/proto` and
  `google.golang.org/protobuf/reflect/protoreflect`. `internal/controlplane/architecture_test.go` lets
  `metadb` import these two and no other third-party package.

Why a record now: before T4.4 only the generated API (`controlplane/api`) used protobuf, and the
gate does not follow imports into the API (the input/output edge). Now `metadb.Encode` and `metadb.Decode`
call `proto` on every log entry, on every replica, inside the simulator. `metadb` is reached from the
simulator root, so `ops/scripts/ci-determinism.sh` asks for this record.

What we use: `proto.MarshalOptions{Deterministic: true}.Marshal`, `proto.UnmarshalOptions{}.Unmarshal`,
and `protoreflect.Message.GetUnknown` / `Range` (to refuse unknown fields). The messages are
`controlplane.log.v1.Entry` and the messages it holds (log.proto, shared.proto). None of them has a map
field, an extension range, or a `lazy` field.

## Findings

File:line is in the module source, `google.golang.org/protobuf@v1.35.2/`. `determinismcheck` on `proto`,
`internal/impl`, `encoding/protowire`, `internal/detrand`, `internal/order`, `internal/strs`,
`internal/errors`, `runtime/protoiface`, `reflect/protoreflect`, `internal/flags`, `internal/filedesc`,
`internal/filetype`, `reflect/protoregistry`: 0 `go` statements, 0 channels or `select`, 19 `range` over a
map, 2 reflect map iterations, 225 package-level variables, and the imports `os`, `sync`,
`sync/atomic`, `unsafe`, `reflect`, `io`, `hash/fnv`, `compress/gzip`, `encoding/json`, `go/token`,
`unicode`.

| Finding (file:line) | Matters? | Why |
|---|---|---|
| `internal/detrand/rand.go:38` `randSeed = binaryHash()`: reads the program binary (`os.Executable`) at init. The seed is fixed in one binary and different between builds. | Yes, handled | It is the one source of output that differs between builds. Its users are below. None of them is on the binary marshal path with `Deterministic: true`. Proved by the probe (Evidence): a build with another seed wrote the same entry bytes. |
| `internal/impl/message_reflect.go:110`: detrand swaps two neighbours in the order of `protoreflect.Message.Range`. | No | Marshal does not use `Range` (next rows). Our code calls `Range` only in `metadb.hasUnknown`, whose answer is a yes or no and does not depend on the order. |
| `proto/encode.go:176-177`: generated messages take the fast path, `impl.MessageInfo.marshal`. | No | Fast path below. |
| `internal/impl/codec_message.go:148-150` sorts the coder fields by number, `:168-174` then puts oneof fields last (`order.LegacyFieldOrder`, `internal/order/order.go:21-41`). `internal/impl/encode.go:117-140` writes in that order. | No | A fixed order, from the descriptor only. No detrand, no map. For `Entry` it means `id` first, then the command. |
| `proto/encode.go:246-252`: the slow (reflection) path uses `order.AnyFieldOrder`, which is `Range` order (detrand), unless `Deterministic` is set; then it uses `LegacyFieldOrder`. | No, with rule 1 | Generated messages never take this path, and with `Deterministic: true` it is fixed anyway. This is the reason for rule 1. |
| `internal/impl/codec_map.go:297`, `codec_map_go112.go:12`: map fields are written in map order, sorted only with `Deterministic`. | No, with rule 2 | The log messages have no map field. |
| `internal/impl/encode.go:188-235`: extensions; more than one is sorted, `:170` sizes them in map order. | No | The log messages have no extensions. Size is a sum, so order does not matter. |
| `internal/impl/encode.go:66,94,99`: the size cache is stored in the message with `atomic.StoreInt32`. | No | Encode builds a new message for each call, and nothing else holds it. |
| `internal/impl/message.go:67-82`: `MessageInfo.init` runs once, under a mutex. Also `filedesc` lazy init (`sync.Once`), `checkinit.go:87` (`sync.Map` memo), `legacy_*.go` (`sync.Map` caches). | No | Each one computes a value from the descriptor or the Go type only. The first caller (from any goroutine of `cpsim`) gets the same value as any other. The legacy caches are for message types that are not generated with protoc-gen-go; ours are. |
| `internal/order/range.go:20,71`: `sync.Pool` of scratch slices for sorting fields (slow path, maps). | No | Scratch space only; the result is sorted. |
| `internal/impl/pointer_unsafe.go:13`: `UnsafeEnabled = true`, field access by pointer offset. | No | Memory layout, not order or time. The generated code checks the runtime version (`protoimpl.EnforceVersion`). |
| Aliasing: `internal/impl/codec_gen.go:5064` (string: `string(v)`), `:5497` (bytes: `append(nil, v...)`). No alias option in `proto.UnmarshalOptions` (`proto/decode.go:24-50`). | No | Decode copies every string and `[]byte` out of the input, so the replica may reuse its buffer. |
| `internal/impl/codec_gen.go:5042` (`appendStringValidateUTF8`): proto3 `string` fields must be valid UTF-8, on Marshal and on Unmarshal. | Yes, rule 3 | Marshal fails on a string that is not UTF-8. `metadb.Encode` panics then (it has no error result). Decode returns ErrCorrupt. The old hand-written codec took any bytes. |
| Unknown fields: Unmarshal keeps them (`DiscardUnknown: false`), and Marshal writes them back as they came. | Yes, handled | `metadb.Decode` refuses an entry with an unknown field at any depth (`hasUnknown`). A node must not skip a part of a command that a newer node wrote. |
| `reflect/protoregistry/registry.go:42-63`: a name conflict at registration reads `GOLANG_PROTOBUF_REGISTRATION_CONFLICT` and may panic or warn. | No | Only at package init, when two files register one name. Ours do not: every test binary would panic at init. |
| `internal/errors/errors.go:29`: detrand picks a normal or a non-breaking space after `proto:` in error texts. | No | The texts of protobuf errors never reach state or the trace: Decode returns our own `ErrCorrupt`. |
| `encoding/prototext`, `internal/encoding/json`, `internal/descfmt`: detrand changes spaces in text output. | No | Linked in by `internal/impl`, but our code never formats a message as text. |
| `reflect/protoregistry/registry.go:358,743,774,805,837`, `internal/filedesc/desc_list.go:55,426`, `internal/impl/equal.go`, `merge.go`, `message_reflect.go:238`, `codec_messageset.go`: `range` over maps. | No | Registry listing, `proto.Equal`, `proto.Merge`, extension `Range` and MessageSet. Not called by Marshal or Unmarshal of our messages (no extensions, no MessageSet). |
| Deterministic marshaling is "not guaranteed to remain stable over time" (`proto/encode.go:34-55`). | Yes, rule 4 | Fine inside one build. Across versions the bytes may change, so: state records stay hand-written (the state hash reads them), and Decode accepts any valid encoding of an Entry, not only the bytes Encode writes. `TestEntryBytesPinned` fails when a version bump changes the bytes. |
| Hidden defaults (timestamps, random ids, checksums) | No | None in the binary wire format. |

## Rules for our code

1. Marshal log entries only with `proto.MarshalOptions{Deterministic: true}` (it is in `metadb.Encode`, the
   one place that marshals an entry).
2. No `map` field, extension or `lazy` field in a message of `controlplane.log.v1.Entry`, and no other way of
   writing log bytes. The field numbers of `Entry` stay below 16, so its first byte is never the replica's
   group marker (0xC5) or envelope marker (0xC8).
3. Every string in a logged command is valid UTF-8. Commands come in through gRPC, which checks this; the
   simulator and the tests use ASCII names. `metadb.Encode` panics on a string that is not UTF-8.
4. Never compare or hash log entry bytes between builds, and never feed them into the state hash. State
   records (what `Snapshot` writes and `StateHash` reads) keep their hand-written codecs.
5. `metadb.Decode` refuses unknown fields; it never panics on bad bytes (`FuzzDecode`).

## Evidence

- `go run github.com/manifesterio/mds/dst/cmd/determinismcheck` over the packages above: the findings counted at the top, each one
  read and listed in the table. No goroutine, channel, time or random number on the path.
- Probe (a temporary `main` in the repo, not committed): six builds with a different `-ldflags -X` string,
  one `-trimpath` build and one `-race` build, each run twice (16 processes). Each process encoded one entry
  of each kind 1000 times, compared plain `proto.Marshal` with the deterministic bytes, and decoded them back
  (`reflect.DeepEqual`). All 16 printed the same SHA-256 of the entry bytes (`09174ce6e2e224a2...`) and
  `stable=true`. The `-race` build had a different detrand seed (its prototext and error text had other
  spaces), and its entry bytes were the same.
- `TestEntryBytesAreDeterministic` (metadb): every kind, 200 encodes, plus a message read back from the
  bytes and a message built with its fields set in reverse order, with both deterministic and plain
  Marshal: the same bytes.
- `TestEntryBytesPinned` pins the bytes of one entry, so a protobuf version bump that changes them fails.
- Simulator: `go run ./cmd/cpsim -seeds 300 -scenario all` gives the same five hashes as before the change
  (`da8028bc bc9f0863 23603524 67c48ce5 ba61ad2c`), and the golden trace hashes do not move: the trace
  folds the state hash and the step counts, not the entry bytes.
