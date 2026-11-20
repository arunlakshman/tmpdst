# github.com/twmb/franz-go/pkg/kmsg@v1.14.0

- Date: 2026-10-05. Audited by a Claude subagent, then reproduced with `ops/scripts/audit-dep.sh`.
- Requires Go: 1.24.0 (the reason this repo moved to Go 1.24).
- Verdict: **ACCEPTED WITH RULES**. Pure encode/decode: no clock, randomness,
  goroutines, channels, sync, os or net.
- Import paths allowed: `github.com/twmb/franz-go/pkg/kmsg` (exact). Its internal
  `kmsg/internal/kbin` is reviewed here but is not importable by us.

## Findings

| Finding (file:line) | Matters? | Why |
|---|---|---|
| `internal/kbin/primitives.go:647` `Reader.Span` returns `b.Src[:l:l]` | **Yes** | Decoded `[]byte` fields alias the input buffer in both `ReadFrom` and `UnsafeReadFrom`: `Record.Key/Value`, `Header.Value`, `ProduceRequest…Records`, `FetchResponse…RecordBatches`, tag values. Capacity is capped, so appends don't clobber the input, but writing into the input or the slice changes both. Verified by mutating the buffer after decoding. |
| `internal/kbin/primitives.go:850-856` `UnsafeString` (`unsafe` + `reflect.StringHeader`) | Yes, avoidable | Used only by `UnsafeReadFrom`; decoded strings then alias the input. `ReadFrom` copies strings. |
| `api.go:405` range over `Tags.keyvals` map | No | Keys are collected and passed to `sort.Slice` before encoding; map order never reaches the output. |
| `generated.go` `reflect.DeepEqual` (for example :3854, :4719, :5573) | No | Pure comparison of tagged-field structs against defaults to decide whether to emit them. |
| `api.go:30`, `generated.go:4` import `context` | No | Appears only in the `Requestor` interface signature. |
| `api.go:462` package-level var `ListClientMetricsResources` | No | A `Key` constant declared as `var`; never written at runtime. No pools or caches. |
| `RecordBatch.AppendTo` | Note | Writes `FirstTimestamp`, `CRC` and `Length` exactly as given. Nothing is filled in implicitly. |

## Rules for our code

1. **Framer owns fresh buffers.** Each frame handed to the codec is a new slice
   that is never reused or mutated afterwards (it is aliased by the decoded
   message for as long as that message lives).
2. **`ReadFrom` only, never `UnsafeReadFrom`.**
3. **We compute everything kmsg writes verbatim:** record-batch CRC (crc32c,
   `hash/crc32` Castagnoli), lengths, timestamps (from `Event.Wall`).
4. Don't set unknown tags on tag keys the protocol already defines (that
   produces undecodable bytes; it's caller error, not nondeterminism).

## Evidence

- determinismcheck findings are exactly the rows above (reflect/unsafe/context imports,
  the sorted map range, one package-level var).
- ProduceRequest v9/v11 and FetchResponse v11/v12/v16 encode byte-identically
  twice; decode twice gives `reflect.DeepEqual` results; re-encode round-trips;
  output identical across separate processes (15/15 checks).
