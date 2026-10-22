# Chapter 5. The Kafka wire format and the segment format

Manifester uses two byte formats. `kwire/` handles the Kafka protocol.
Clients send length-prefixed frames that contain record batches. The broker
answers with length-prefixed frames. `format/` handles the segment format.
Manifester packs sealed batches from many partitions into one segment object.
The broker stores the segment object under a `seg/<random>` key. Readers
fetch parts of the segment object by byte range.

Both packages are pure and deterministic. The packages use no clock. The
packages use no randomness. The packages do no input or output. Callers pass
in timestamps and IDs. The same code runs in the broker state machine and in
the simulated clients. A byte that a test produces matches a byte that a real
client accepts. The sources are
`kwire/doc.go`, `kwire/codec.go`, `kwire/framer.go`, `kwire/batch.go`,
`kwire/supported.go`, `format/format.go`, and `format/key.go`. The call sites
are in `broker/produce.go`, `broker/fetch.go`, and `broker/segment.go`.

## 5.1 Each file in `kwire` and `format` owns one task

| Package | File | Responsibility |
|---|---|---|
| `kwire` | `kwire/framer.go` | Reassemble TCP streams into Kafka frames |
| `kwire` | `kwire/codec.go` | Decode requests and encode responses for the broker. Decode responses and encode requests for sim clients |
| `kwire` | `kwire/supported.go` | API keys and versions the broker advertises and serves |
| `kwire` | `kwire/batch.go` | Record batch v2 build, parse, verify, split, and patch |
| `format` | `format/format.go` | Segment object layout with header, index, and payloads |
| `format` | `format/key.go` | Segment keys in the form `seg/<32 lowercase hex>` |

`kwire` wraps franz-go's `kmsg` (`github.com/twmb/franz-go/pkg/kmsg`). The
`kmsg` package supplies per-API message types. Manifester computes the batch
CRC, the batch lengths, and the batch timestamps. `kmsg` writes the values
that Manifester computes. `kwire/doc.go` and
`docs/dep-audits/github.com/twmb/franz-go/pkg/kmsg@v1.14.0.md` describe the
split.

## 5.2 `kwire.Framer` reassembles a TCP stream into frames

Kafka frames have a 4-byte big-endian length. The payload follows the length.
The payload has the number of bytes that the length names. TCP does not keep
the frame boundaries. `Framer` reassembles the frames from the TCP stream.

```
stream bytes (arbitrary Push chunking)
+----------+-------------+----------+-------------+---
| len = N  | payload N B | len = M  | payload M B | ...
|  u32 BE  |             |  u32 BE  |             |
+----------+-------------+----------+-------------+---
      \________ frame 1 ______/ \_____ frame 2 ___/
```

`Framer.Push(data)` appends to an internal buffer. It returns every complete
frame in order. The returned frames do not include the length prefix.
Incomplete tails stay in the buffer. `Buffered()` reports the count of held
bytes.

A length `<= 0` yields `ErrBadFrame`. A length above the `max` given to
`NewFramer(max)` yields `ErrBadFrame`. `Push` still returns frames that it
completed before the bad length. It returns those frames with the error.
`ErrBadFrame` does not recover. Every later `Push` returns the same error.
The framer discards the buffer. The connection ends after `ErrBadFrame`.

Each frame is a copy (`append([]byte(nil), ...)`). A frame is not a view into
the framer buffer. A later `Push` cannot mutate a frame that a caller still
decodes. Section 5.5 states the aliasing rules.

One `Framer` serves one connection. The `core.Framer` interface is in
`core/kafka.go`.

## 5.3 `kwire.Codec` decodes requests and encodes responses

`kwire.Codec` is a stateless value. The value implements `core.Codec`.
`kwire/doc.go` states the rule that pairs each encode function with a decode
function.

> `Encode*` functions return a complete frame **with** the 4-byte length
> prefix, ready to write. `Framer` strips the prefix, so `Decode*`
> functions take a frame **without** it.

A request on the wire has this layout.

```
+----------+-------------------+------------------+-----------------+
|  length  | api key | version | correlation id | client id ...   |
|  u32 BE  |  i16 BE |  i16 BE |     i32 BE     | i16 len + bytes |
+----------+-------------------+------------------+-----------------+
 \_ EncodeRequest adds this /   \_______ DecodeRequest input ______/
   Framer strips it again
```

`DecodeRequest` parses the fixed header without `kmsg`. The header has a
minimum of 10 bytes. The function parses the header before the function
parses the body with `kmsg`. The fields are the
api key `i16`, version `i16`, and correlation id `i32`. A nullable client-id
follows. The client-id has an `i16` length. A length of `-1` means the
client-id is absent. A length below `-1` is malformed.

`DecodeRequest` then takes one path from the list below.

- An unsupported key or version gives a parsed header and an
  `*UnsupportedError` (see §5.4). The code does not parse the body.
- For flexible APIs, `skipTags` walks the header tag section and discards the
  header tags. `kmsg` handles trailing body tags.
- For a supported key and version, the code parses the body with `kmsg`'s
  `ReadFrom`. The code does not use `UnsafeReadFrom`.

`EncodeResponse` writes the correlation id. The function writes a single
`0x00` header-tag byte for flexible APIs. The function then writes the body.
The `prefix` helper reserves 4 bytes for the length. The `seal` helper fills
the 4 bytes and seals the frame. `EncodeRequest` does the same work for sim
clients. The function encodes the client id as a non-null string.
`DecodeResponse` checks the correlation id against the request header. The
function returns `ErrMalformed` on a mismatch. All malformed input in this
file wraps `ErrMalformed`. Callers match the error with `errors.Is`.

ApiVersions has one special case. Kafka answers an ApiVersions request that
Kafka cannot serve at version 0 with error 35. `respVersion` encodes a
response for an unserved version as v0. ApiVersions uses response header v0.
Response header v0 has no tag byte. A flexible v3 request also gets response
header v0 in this case.

## 5.4 `kwire/supported.go` serves five API keys

The broker serves five keys. The table below lists the ranges from
`Supported()` in `kwire/supported.go`. `ApiVersionsResponse` advertises the
ranges.

| API key | Name | Min | Max | Notes |
|---:|---|---:|---:|---|
| 0 | Produce | 3 | 9 | `>= 3` so batches are record batch v2 |
| 1 | Fetch | 4 | 12 | below v13, which switches to topic IDs |
| 2 | ListOffsets | 1 | 7 | |
| 3 | Metadata | 1 | 12 | |
| 18 | ApiVersions | 0 | 3 | answered at v0 when request version unsupported |

`DecodeRequest` rejects any other key or version with an
`*UnsupportedError`. The type has this shape.

```go
type UnsupportedError struct {
    Key, Version int16
    UnknownKey   bool // key is not served at all (versus version out of range)
}
```

`UnknownKey: true` means the key is absent from `Supported()`. InitProducerID
(key 22) is one example. The broker does not serve InitProducerID because the
broker does not support idempotent producers.
`TestInitProducerIDNotSupported` in `kwire/codec_test.go` checks that result.
The test checks that the broker does not advertise the key. The test checks
that `DecodeRequest` does not decode the key. A client that needs
InitProducerID fails at startup.
`UnknownKey: false` means the broker serves the key. The version is out of
range in this case. Produce v2, Fetch v13, ApiVersions v99, and a negative
version are examples.

`DecodeRequest` returns the parsed `core.RequestHeader` with the error. The
broker uses the header to send a v0 ApiVersions response.
`ApiVersionsResponse` builds the response. The response carries
`ErrCodeUnsupportedVersion` (35). This response path does not require the
broker to close the connection during the handshake.

## 5.5 Decoded data aliases the input frame

Decoded `kmsg` messages alias the input frame. Record keys and values point
into the bytes that the caller passes to `ReadFrom`. Produce partition
`Records` point into the frame bytes. Fetch record batches point into the
frame bytes. The code makes no defensive copy. The rules follow.

- A frame from `Framer.Push` is a fresh slice. The framer does not reuse it.
  The framer does not mutate it. Aliased decoded data stays valid while the
  caller keeps the frame alive and does not mutate it.
- Callers use `kmsg` through `ReadFrom` only. They do not use
  `UnsafeReadFrom`.
- `kwire` computes every value that `kmsg` writes verbatim. Those values are
  the batch CRC-32C, lengths, and caller-supplied timestamps. The code uses
  no clock. The same inputs give the same frame. The codec tests assert
  byte-identical re-encoding.

`ParseBatch` and `format.Decode` follow the same aliasing rule.
`Batch.records` aliases the input to `ParseBatch`. `Batch.Payload()` aliases
the input buffer. Decoded record keys and values alias the input buffer.
`SplitBatches` returns capacity-capped subslices (`b[:n:n]`). An `append` to
a subslice cannot write over the next batch.
`format.Segment.Batches[i].Data` aliases the object that the caller passes to
`format.Decode`. `PatchBatch` copies the batch before the patch writes to the
batch (see §5.7). The caller copies bytes that must outlive the source frame.
The caller copies bytes that must outlive the source segment object.

## 5.6 `kwire/batch.go` uses record batch format version 2 only

Manifester uses batch format version 2 only (magic `2`). Produce v3 is the
first Produce version with record batch version 2. A version 2 batch has a
61-byte header and a records section. The layout follows.

```
offset  size  field
------  ----  --------------------------------------------------
 0       8    baseOffset            } rewritten by PatchBatch
 8       4    length (= total - 12) }
12       4    partitionLeaderEpoch  } rewritten by PatchBatch
16       1    magic (= 2)
17       4    CRC-32C  -----------------------------+
21       2    attributes            |  CRC covers  |
23       4    lastOffsetDelta       |  bytes 21    |
27       8    firstTimestamp        |  to the end  |
35       8    maxTimestamp          |              |
43       8    producerId            |              |
51       2    producerEpoch         |              |
53       4    baseSequence          |              |
57       4    numRecords            |              |
61       ...  records (or compressed records payload) ------+
```

`ParseBatch` verifies one batch. The function does not decompress the batch.
The function runs the checks in the order that the list below gives.

- A batch with fewer than 17 bytes gives `ErrCorrupt`. The code cannot read
  the magic byte from a batch with fewer than 17 bytes.
- A batch with a byte 16 other than `2` gives `ErrUnsupportedBatch`. The code
  rejects magic 0 and magic 1 from the message-set era. The code rejects a
  newer magic. The code does not convert a batch with another magic.
- A batch with fewer than 61 bytes gives `ErrCorrupt`. A batch with
  `length != len(b) - 12` gives `ErrCorrupt`. A batch with a length below the
  49-byte minimum for an empty records section gives `ErrCorrupt`.
- The CRC-32C (Castagnoli, `crc32.MakeTable(crc32.Castagnoli)`) over `b[21:]`
  must equal `b[17:21]`. A mismatch gives `ErrCorrupt`.
  `ParseBatchUnverified` skips the CRC-32C check. A broker mutant uses
  `ParseBatchUnverified` to model a missing CRC-32C check. Real code uses
  `ParseBatch`.
- A codec id (attributes bits 0-2) above 4 with a valid CRC gives
  `ErrUnknownCodec`. The check order matters. A batch with invalid codec bits
  and a bad CRC gives `ErrCorrupt`. Tests recompute the CRC with `refix` to
  reach the codec check.
- `kmsg.RecordBatch.ReadFrom` parses the batch structure again. `countFits`
  checks the record count. An uncompressed payload of `n` bytes holds at most
  `n` records. A compressed payload can expand to any size. `countFits`
  checks that the count is not negative. `countFits` checks that the payload
  is not empty for a positive count.

| Error | Meaning |
|---|---|
| `ErrCorrupt` | structure or CRC validation failed |
| `ErrUnsupportedBatch` | magic != 2, or `Records()` on a compressed batch |
| `ErrUnknownCodec` | valid CRC, but compression codec id > 4 |
| `ErrTruncated` | `SplitBatches` hit a partial trailing batch |
| `ErrNotSingle` | a Produce payload did not hold exactly one batch |

`assembleBatch` builds each batch. The function takes the header from
caller-supplied `BatchParams`. The params hold the leader epoch, the
attributes, the producer id, the producer epoch, the base sequence, and the
timestamps. The code uses no clock. The code sets
`magic = 2`. The code sets `lastOffsetDelta = numRecords - 1`. The code
computes the length. The code backpatches the CRC. `BuildBatch` builds an
uncompressed batch with base offset 0. `BuildBatch` numbers records by index
(`OffsetDelta = i`). `BuildBatch` rejects a non-zero compression attribute.
`BuildOpaqueBatch` wraps a payload that a caller encodes. The payload may be
compressed. `BuildOpaqueBatch` does not read the payload content. The
CRC must be valid. The payload does not need to decompress. Tests and the
simulator use `BuildOpaqueBatch` for a payload from a real client.
`SplitBatches` reads concatenated batches by length. `SplitBatches` checks
structure only. `SplitBatches` does not check the CRC. `SplitBatches` returns
complete batches. `SplitBatches` returns `ErrTruncated` for a partial tail.
`OneBatch` requires one batch. `broker/produce.go` requires one batch per
Produce partition.

### The broker stores compressed batches as opaque bytes

Codec ids are the low three attribute bits. The table below lists each codec
id, the codec name, and the constant.

| Codec id | Name | Constant |
|---:|---|---|
| 0 | none | `kwire.CodecNone` |
| 1 | gzip | `kwire.CodecGzip` |
| 2 | snappy | `kwire.CodecSnappy` |
| 3 | lz4 | `kwire.CodecLZ4` |
| 4 | zstd | `kwire.CodecZstd` |

The broker stores compressed payloads as opaque bytes. The broker serves the
payloads as opaque bytes. The broker does not decompress the payloads
(`docs/decisions.md`, decision 45).
The CRC-32C covers the compressed bytes. A reader checks integrity from the
CRC-32C without a decode of the payload. Offsets come from the header
(`baseOffset + lastOffsetDelta`).
Offset assignment does not need the records. `Batch.Records()` refuses
compressed batches with `ErrUnsupportedBatch`. `Batch.Payload()` returns the
stored records section for the storage path. `DecodeRecords` serves callers
that hold a decompressed payload. The broker rejects a codec id > 4 at
Produce time. The broker maps a codec id > 4 to Kafka's
UNSUPPORTED_COMPRESSION_TYPE. The deterministic core uses no codec library.
Clients compress a batch before the clients send the batch. Clients
decompress a batch after the clients receive the batch.

The code exposes other attribute bits. The code does not interpret the other
attribute bits.
`IsTransactional()` reads bit `0x10`. `IsControl()` reads bit `0x20`.
`broker/checkBatch` rejects both bits. The function rejects
`ProducerID >= 0` for idempotent producers. The function rejects a
non-positive record count. The function rejects
`LastOffsetDelta != NumRecords - 1`. The function reads each value from the
batch header.

## 5.7 Fetch patches `baseOffset` and `partitionLeaderEpoch` outside the CRC range

The broker stores a batch in the form that the producer sends. The stored
`baseOffset` holds the value that the client writes. Clients write 0 by
convention. The log assigns a different offset to the batch.

The CRC-32C covers bytes `21..end`. `baseOffset` at bytes 0-7 sits outside
the covered range. The length at bytes 8-11 sits outside the covered range.
`partitionLeaderEpoch` at bytes 12-15 sits outside the covered range.
`PatchBatch(b, baseOffset, leaderEpoch)` copies the batch
(`append([]byte(nil), b...)`). The copy does not mutate the stored bytes. The
function returns an input shorter than 16 bytes as an unchanged copy. The
function rewrites bytes 0-7 and bytes 12-15. The rewrite leaves the CRC-32C
valid. `ParseBatch` on the patched batch succeeds. The patch also succeeds on
a compressed batch. The patch leaves the compressed payload untouched.

In `broker/fetch.go` (`onFetchGet`), the code verifies each ranged
object-store read with `kwire.ParseBatch`. The code checks the read against
the committed record count. The code appends the read to the response as
`kwire.PatchBatch(e.Data, base, 0)`. The `base` value is the committed base
offset for the batch from the lookup. The consumer receives the producer
bytes with two header fields changed. Kafka rewrites the two header fields on
append.

## 5.8 `format/format.go` puts the index before the payloads and has no footer

One segment object holds batches for many topic-partitions.
`broker/segment.go` (`sealBatches`) seals the segment object.
`core.CommitSegment` commits the segment object. The segment object has a
header and an index. The segment object has no footer. The index sits at the
front of the body. Payloads follow the index with no gaps. The segment object
ends at the last payload byte. All integers use big-endian byte order.

```
object = header || body

header (20 bytes)
+--------+---------+--------+------------+-------------------+
| "MSEG" | version | flags  | body len   | body CRC-32C      |
|  0..3  | 4..5 u16| 6..7   | 8..15 u64  | 16..19 u32        |
|        |   = 1   | u16 =0 |            | over whole body   |
+--------+---------+--------+------------+-------------------+

body
+----------+---------------------------+----------------------+
| count    | index entries (x count)   | payloads, back to    |
| u32      | variable, see below       | back, in index order |
+----------+---------------------------+----------------------+

index entry (30 + len(topic) bytes)
+---------------+---------+-----------+---------+--------+--------+-----------+
| topic len u16 | topic   | partition | records | off    | len    | batch     |
|               | bytes   | i32       | u32     | u64 *  | u64    | CRC-32C   |
+---------------+---------+-----------+---------+--------+--------+-----------+
  * off is relative to body start on the wire; Entry values returned by
    Encode/Decode are absolute in the object (HeaderSize + off).
```

`Encode(batches)` returns one segment object and one `Entry` per batch. The
function returns the `Entry` values in batch order. The encoding is
canonical. One batch list has one encoding. The function rejects each case
below with `ErrInvalid`.

- an empty list
- an empty topic
- a topic over 65535 bytes
- a negative partition
- an empty payload

The version is 1. The flags are 0. Payload offsets have no gaps. `Encode`
writes the body CRC-32C into header bytes 16-19.

`Decode(obj)` validates the whole segment object. The function does not
panic. Each failure below matches `ErrCorrupt`.

- truncation
- bad magic
- version != 1
- non-zero flags
- a body-length mismatch
- a body CRC mismatch
- a malformed index
- a non-contiguous payload range
- an out-of-range payload range
- trailing body bytes
- a per-batch CRC mismatch

`format.Code` maps these cases to `core` codes (`ErrCorrupt`, `ErrInvalid`,
`OK`).

### Three CRC-32C values cover the batch, the entry, and the body

The batch carries Kafka's CRC-32C over the batch records section. The segment
body carries one CRC-32C over the index and the payloads. Each index entry
carries a CRC-32C of the entry payload. A range read needs the per-entry
CRC-32C. A reader fetches one batch with `Entry.Ref(key)`. The ref is a
`core.SegmentRef` with a key, an offset, and a length. The reader does not
receive the whole body. The reader cannot check the body CRC-32C from a range
read. `VerifyBatch(entry, bytes)` checks the fetched length. The function
checks the per-entry CRC-32C. The function returns the bytes on success. The
broker Fetch path runs `kwire.ParseBatch` on ranged bytes too (see §5.7).
`VerifyBatch` is the format-level check. Readers such as `sim/echo/broker.go`
use `VerifyBatch`. The readers do not need Kafka-level parsing.

## 5.9 `format.SegmentKey` names each segment with 32 lowercase hex digits

`SegmentKey(id)` names segment objects. The form follows.

```
seg/<128-bit id as 32 lowercase hex digits>
seg/ab000000000000000000000000000000     <- SegmentKey(core.ID{0xAB})
```

The id is a random 128-bit value. The broker draws the id from `Env.NewID()`.
The broker does not draw the id from a request id. The broker draws the key
once at seal time in `broker/segment.go`. A retry of one commit reuses the
same key. The key stays unique across nodes and boots. The namespace is flat.
The spelling is canonical. A key has one `/`. A key has 32 lowercase hex
digits. A key has no node component. A key has no incarnation component.
`ParseSegmentKey` inverts `SegmentKey`. `ParseSegmentKey` rejects upper-case
hex. The function rejects wrong lengths. The function rejects extra slashes.
The function rejects the old nested `seg/<node>/<inc>/<id>` spelling. A
case-insensitive store cannot alias two segment keys to one name.

## 5.10 One batch moves from Produce to Fetch through one segment

```
producer --Produce frame--> Framer.Push -> Codec.DecodeRequest
  -> kwire.OneBatch + ParseBatch (magic, CRC, codec, header checks)
  -> batches buffered, sealed: format.Encode(batches)
  -> object PUT under format.SegmentKey(env.NewID())
  -> CommitSegment(entries[i].Ref(key)) records each batch's range

consumer --Fetch frame--> Lookup returns committed ranges
  -> one ranged GET per batch -> kwire.ParseBatch verifies
  -> kwire.PatchBatch(data, committedBaseOffset, 0)
  -> Codec.EncodeResponse -> length-prefixed frame -> consumer
```

The broker does not decompress records. The broker does not re-encode
records. The broker does not renumber records. The stored bytes are the
producer bytes. Two header fields outside the CRC range change in the Fetch
response.

## Key takeaways

- `Encode*` returns frames with the length prefix. `Framer` strips the
  prefix. `Decode*` consumes frames without the prefix. `ErrBadFrame` does
  not recover. The connection ends after `ErrBadFrame`.
- The broker serves five APIs. The ranges are Produce 3-9, Fetch 4-12,
  ListOffsets 1-7, Metadata 1-12, and ApiVersions 0-3. An
  `*UnsupportedError` marks an unknown key or an out-of-range version.
  `DecodeRequest` returns the header with the error. The broker answers with
  the header.
- Batches use magic v2 only. CRC-32C covers bytes 21..end. `baseOffset` and
  `partitionLeaderEpoch` sit outside the covered range. Fetch patches offsets
  with `PatchBatch` on a copy. The patch leaves the CRC-32C valid for the
  consumer check.
- The broker stores compressed batches as opaque bytes. The broker serves
  compressed batches as opaque bytes. The codecs are gzip, snappy, lz4, and
  zstd with ids 1-4. The broker reads the codec id from the batch header. A
  codec id above 4 with a valid CRC gives `ErrUnknownCodec`. The broker does
  not decompress a batch.
- Decoded `kmsg` data aliases the input frame. Framer frames are fresh
  slices. `ReadFrom` is the one parse entry point. The caller copies data
  that must outlive the source buffer.
- A segment has a 20-byte `MSEG` header. The body holds an index and then
  payloads. The body has no footer. The segment has three CRC-32C values.
  The three values cover the batch, each entry, and the whole body. A
  whole-object decode checks the body CRC-32C. A single-batch range read
  checks the per-entry CRC-32C. Segment keys are flat and canonical. The form
  is `seg/<32 lowercase hex>`.

## Checkpoint

<details>
<summary>1. A client sends Produce v2. What does <code>DecodeRequest</code> return, and why not simply fail?</summary>

The parsed `core.RequestHeader` plus an `*UnsupportedError` with `UnknownKey: false`. The broker serves Produce. It does not serve Produce at v2. The range is 3-9. The header lets the broker answer or close with the right correlation id. The code does not parse the body.
</details>

<details>
<summary>2. Why can Fetch rewrite a stored batch's base offset without invalidating its CRC?</summary>

The v2 CRC-32C covers bytes 21 to the end. `baseOffset` (0-7) and `partitionLeaderEpoch` (12-15) sit before byte 21. They sit outside the covered range. `PatchBatch` copies the batch. It rewrites those fields. `ParseBatch` on the result verifies.
</details>

<details>
<summary>3. <code>Batch.Records()</code> errors on a gzip batch, yet the broker stores and serves it. Why is that consistent?</summary>

The broker does not need the records. The batch CRC-32C covers the compressed bytes. The CRC-32C gives the integrity check. Offsets come from the header (`baseOffset + lastOffsetDelta`). The broker stores compressed batches as opaque bytes. The broker serves compressed batches as opaque bytes (decision 45). Clients decompress the batches. `Records()` serves uncompressed batches. `DecodeRecords` serves decompressed payloads.
</details>

<details>
<summary>4. The object has a body CRC and each batch a Kafka CRC. Why does every index entry carry a third CRC?</summary>

A ranged read of one batch (`Entry.Ref`) does not fetch the whole body. A reader cannot check the body CRC from that read. Format-layer readers do not need Kafka parsing. The per-entry CRC-32C lets `VerifyBatch` check the length from a range GET. The per-entry CRC-32C lets `VerifyBatch` check the CRC from that GET. `VerifyBatch` returns the bytes after the length check and the CRC check pass.
</details>

<details>
<summary>5. You decode a Produce request, keep a record value, then push more bytes through the same <code>Framer</code>. Is the value still valid?</summary>

Yes. `Framer.Push` returns fresh slices. The framer does not reuse a returned slice. The framer does not mutate a returned slice. Parsed `kmsg` data aliases the frame. The value stays valid while the caller holds the frame. A mutation of the frame changes the value. The caller copies data that must outlive the source buffer. `PatchBatch` copies the batch before the patch.
</details>

## Try it

```sh
go test ./kwire/ ./format/
go test -run 'TestPatchBatch|TestOpaqueCompressedBatch' ./kwire/ -v
go test -run 'TestUnsupported|TestInitProducerIDNotSupported' ./kwire/ -v
go test -run 'TestFlipEveryByte|TestParseRejects' ./format/ -v
```

`TestFlipEveryByte` flips every bit of a segment object. The test requires
`ErrCorrupt` for each flipped object. `TestPatchBatch` shows that a patched
batch passes `ParseBatch`. Run `go test -run TestCompression ./it/ -v` for
the compression test. The compression test is in `it/compression_test.go`.

## Further reading

- `kwire/doc.go` states the aliasing rules and the framing rules in canonical
  form.
- The tests in `kwire/batch_test.go`, `kwire/codec_test.go`, and
  `kwire/framer_test.go` encode each wire layout as executable checks. The
  tests in `format/format_test.go` and `format/key_test.go` encode each
  segment layout and each rejection rule as executable checks.
- `broker/produce.go` (`checkBatch`), `broker/fetch.go` (`onFetchGet`), and
  `broker/segment.go` (`sealBatches`) are the call sites that consume the two
  formats.
- `docs/dep-audits/github.com/twmb/franz-go/pkg/kmsg@v1.14.0.md` describes the
  `kmsg` wrapper.
- `docs/decisions.md` records decision 30 (Fetch patches committed offsets),
  decision 43 (`seg/<random>` keys), decision 45 (compression as opaque
  bytes), and decision 41 (no idempotent producers).
