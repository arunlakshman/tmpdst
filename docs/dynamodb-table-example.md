# What the DynamoDB control-plane table looks like after 10 commits

This page shows a real `Scan` of the control-plane table (`cp/dynamo`) after a broker (node `broker-1`) committed ten segments across two topics. Nothing here is invented. A throwaway Go program drove the real `dynamo.Store` API against ExtendDB (`scripts/it-up.sh`), then scanned the whole table with the DynamoDB API and decoded every item. The segment layout and keys come from the real `format.Encode` / `format.SegmentKey`, so the byte offsets and lengths are what the broker would build for batches of those sizes. Batch payload bytes are filler (`x`); the control plane never reads them.

Sections: [1 Key schema](#1-key-schema-legend) · [2 Scan summary](#2-scan-summary) · [3 Every item](#3-the-scan-item-by-item) · [4 Timeline](#4-timeline-how-10-commits-build-the-table) · [5 Dup / out-of-order / replay](#5-the-special-cases) · [6 Inspect it yourself](#6-inspect-it-yourself)

## 1. Key schema (legend)

One table, `demo_cp` (prefix `demo_` + `cp`), string keys `pk` (hash) and `sk` (range), on-demand billing. Design source: `cp/doc.go`.

| pk | sk | attributes | what it is |
|---|---|---|---|
| `N#<node>` | `-` | `epoch` N | fencing epoch; `Register` does an atomic `ADD epoch 1`. Every commit carries the epoch and a `ConditionCheck` pins it. |
| `PID` | `-` | `n` N | producer-ID counter; `InitProducer` does `ADD n 1`, ID = 999 + n (first is 1000). |
| `TOPICS` | `<topic>` | `parts` N | partition count (`CreateTopic` and growth only ever raise it). |
| `G#<segkey>` | `-` | `blob` B | one item per committed segment: JSON of the request batches plus the full answer. Written with `attribute_not_exists`, so it is the idempotency key: a replay reads it and returns the stored answer. |
| `R#<topic>#<p>` | `!` | `hwm` N | high watermark = next offset to assign. `!` sorts before every digit, so the range query never sees it. |
| `R#<topic>#<p>` | 20-digit base offset | `base` N, `cnt` N, `runs` B | a **run**: all batches one commit assigned to one partition. Sort key is the first offset, `cnt` the total records, `runs` the packed batch list. |
| `S#<topic>#<p>` | 20-digit producer ID | `pepoch` N, `nxt` N, `win` B, `v` N | idempotent-producer state for one (partition, producer): epoch, next expected sequence, the last 5 batches (JSON), and a version used for optimistic concurrency. |

Decoding the packed `runs` blob (all integers are varints): `0x01` format byte; `nkeys`, then `nkeys` x (length, bytes) distinct segment keys; `nbatches`, then per batch `keyIdx, count, off, len` (`off` and `len` zigzag). Batch *i* starts at `base + sum(count[:i])`, so offsets are never stored. `off`/`len` are the byte range of the batch inside the segment object (the object starts with a 20-byte header and an index, so the first batch never sits at 0).

The `win` blob is a JSON array, oldest first, at most 5 entries: `{"s": seq, "n": records, "b": base offset, "k": segment key, "o": byte off, "l": byte len}`. `seq` is the producer's base sequence, and `nxt` advances by the record count, so sequences count records, not batches.

A `G#` blob (S1 here), decoded and abbreviated (the real JSON repeats the full segment key in every batch and range; field names `Batches`/`Ranges` are capitalised, the inner ones are lower-case):

```json
{
 "Batches": [
  {
   "topic": "orders",
   "partition": 0,
   "records": 3,
   "seg_key": "…646b7279",
   "seg_off": 276,
   "seg_len": 205,
   "producer": 1000
  },
  {
   "topic": "orders",
   "partition": 0,
   "records": 2,
   "seg_key": "…646b7279",
   "seg_off": 481,
   "seg_len": 157,
   "producer": 1000,
   "base_seq": 3
  },
  {
   "topic": "orders",
   "partition": 1,
   "records": 4,
   "seg_key": "…646b7279",
   "seg_off": 638,
   "seg_len": 253,
   "producer": 1000
  },
  "... 4 more"
 ],
 "Ranges": [
  {
   "topic": "orders",
   "partition": 0,
   "base": 0,
   "count": 3,
   "seg_key": "…646b7279",
   "seg_off": 276,
   "seg_len": 205
  },
  {
   "topic": "orders",
   "partition": 0,
   "base": 3,
   "count": 2,
   "seg_key": "…646b7279",
   "seg_off": 481,
   "seg_len": 157
  },
  {
   "topic": "orders",
   "partition": 1,
   "base": 0,
   "count": 4,
   "seg_key": "…646b7279",
   "seg_off": 638,
   "seg_len": 253
  },
  "... 4 more"
 ]
}
```

Segment keys are `seg/<node>/<incarnation hex>/<id hex>`. In this run the node is `broker-1`, the incarnation is `a0a1…aeaf`, and the ten ids were generated deterministically; below a key is shortened to its last 8 hex digits (`…646b7279`) or to its label `S1`..`S10` (commit order).

## 2. Scan summary

| type | items | notes |
|---|---|---|
| N# | 1 | epoch |
| PID | 1 | counter |
| TOPICS | 2 | orders, clicks |
| G# | 10 | one per committed segment (10) |
| R# hwm | 5 | one per partition touched (5) |
| R# run | 30 | one per (commit, partition) with new batches |
| S# | 6 | one per (partition, idempotent producer) |
| **total** | **55** | about 28714 bytes of attribute names plus values (about 40.1 KB as DynamoDB JSON) |

The Scan was paged (`Limit` 25, so 3 pages, `LastEvaluatedKey` followed) and sorted client-side by (`pk`, `sk`); a Scan itself returns items in no useful order. The two topics: `orders` (3 partitions) and `clicks` (2). Two idempotent producers were allocated: **A = 1000** and **B = 1001**; some batches were non-idempotent (`producer` absent in the request, no `S#` item).

Hot spots worth noticing: 30 run items versus 5 high watermarks and 6 producer states. The table grows by about one run per (commit, partition), not per batch: S7 put 6 batches into one run item.

## 3. The scan, item by item

Sorted by (`pk`, `sk`) within each group. `B` values are base64 of the packed or JSON bytes described above.

### 3.1 Node epoch (`N#`)

```json
{ "pk": {"S": "N#broker-1"}, "sk": {"S": "-"}, "epoch": {"N": "1"} }
```

- `N#broker-1`: broker-1 registered once; epoch = 1.

### 3.2 Producer counter (`PID`)

```json
{ "pk": {"S": "PID"}, "sk": {"S": "-"}, "n": {"N": "2"} }
```

- `PID`: two `InitProducer` calls so far, so producers 1000 and 1001 exist.

### 3.3 Topics (`TOPICS`)

```json
{ "pk": {"S": "TOPICS"}, "sk": {"S": "clicks"}, "parts": {"N": "2"} }
{ "pk": {"S": "TOPICS"}, "sk": {"S": "orders"}, "parts": {"N": "3"} }
```

- `clicks`: 2 partitions.
- `orders`: 3 partitions.

### 3.4 High watermarks (`R#<topic>#<p>` / `!`)

```json
{ "pk": {"S": "R#clicks#0"}, "sk": {"S": "!"}, "hwm": {"N": "16"} }
{ "pk": {"S": "R#clicks#1"}, "sk": {"S": "!"}, "hwm": {"N": "23"} }
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "!"}, "hwm": {"N": "34"} }
{ "pk": {"S": "R#orders#1"}, "sk": {"S": "!"}, "hwm": {"N": "18"} }
{ "pk": {"S": "R#orders#2"}, "sk": {"S": "!"}, "hwm": {"N": "20"} }
```

- `R#clicks#0`: next offset to hand out is 16, i.e. offsets 0..15 are committed.
- `R#clicks#1`: next offset to hand out is 23, i.e. offsets 0..22 are committed.
- `R#orders#0`: next offset to hand out is 34, i.e. offsets 0..33 are committed.
- `R#orders#1`: next offset to hand out is 18, i.e. offsets 0..17 are committed.
- `R#orders#2`: next offset to hand out is 20, i.e. offsets 0..19 are committed.

### 3.5 Range runs (`R#<topic>#<p>` / base offset)

One item per commit per partition. Two shown in full, with the packed `runs` blob decoded; the other 28 follow as one line each (JSON compact, same attribute order) and a decoded index table.

```json
{
  "pk": {"S": "R#clicks#0"},
  "sk": {"S": "00000000000000000000"},
  "base": {"N": "0"},
  "cnt": {"N": "4"},
  "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzEwMTcxZTI1MmMzMzNhNDE0ODRmNTY1ZDY0NmI3Mjc5AwAB9g3aAQAB0A/aAQACqhG6Ag=="}
}
```

Decoded `runs` (100 bytes, hex `01014e7365672f62726f6b65722d312f61306131...`):

| # | segment | offsets | records | byte offset | byte length |
|---|---|---|---|---|---|
| 0 | …646b7279 (S1) | [0,1) | 1 | 891 | 109 |
| 1 | …646b7279 (S1) | [1,2) | 1 | 1000 | 109 |
| 2 | …646b7279 (S1) | [2,4) | 2 | 1109 | 157 |

`R#clicks#0` / `00000000000000000000`: base 0, cnt 4, so this run covers offsets [0, 4).

```json
{
  "pk": {"S": "R#clicks#0"},
  "sk": {"S": "00000000000000000004"},
  "base": {"N": "4"},
  "cnt": {"N": "1"},
  "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzIwMjcyZTM1M2M0MzRhNTE1ODVmNjY2ZDc0N2I4Mjg5AQABhgzaAQ=="}
}
```

Decoded `runs` (88 bytes, hex `01014e7365672f62726f6b65722d312f61306131...`):

| # | segment | offsets | records | byte offset | byte length |
|---|---|---|---|---|---|
| 0 | …747b8289 (S2) | [4,5) | 1 | 771 | 109 |

`R#clicks#0` / `00000000000000000004`: base 4, cnt 1, so this run covers offsets [4, 5).

The largest run, `R#orders#0` / `00000000000000000020` (S7, six single-record batches in one item, 118 bytes):

```json
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "00000000000000000020"}, "base": {"N": "20"}, "cnt": {"N": "6"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzcwNzc3ZTg1OGM5MzlhYTFhOGFmYjZiZGM0Y2JkMmQ5BgABqATaAQABggbaAQAB3AfaAQABtgnaAQABkAvaAQAB6gzaAQ=="} }
```

| # | segment | offsets | records | byte offset | byte length |
|---|---|---|---|---|---|
| 0 | …c4cbd2d9 (S7) | [20,21) | 1 | 276 | 109 |
| 1 | …c4cbd2d9 (S7) | [21,22) | 1 | 385 | 109 |
| 2 | …c4cbd2d9 (S7) | [22,23) | 1 | 494 | 109 |
| 3 | …c4cbd2d9 (S7) | [23,24) | 1 | 603 | 109 |
| 4 | …c4cbd2d9 (S7) | [24,25) | 1 | 712 | 109 |
| 5 | …c4cbd2d9 (S7) | [25,26) | 1 | 821 | 109 |

All 30 runs, compact:

```json
{ "pk": {"S": "R#clicks#0"}, "sk": {"S": "00000000000000000000"}, "base": {"N": "0"}, "cnt": {"N": "4"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzEwMTcxZTI1MmMzMzNhNDE0ODRmNTY1ZDY0NmI3Mjc5AwAB9g3aAQAB0A/aAQACqhG6Ag=="} }
{ "pk": {"S": "R#clicks#0"}, "sk": {"S": "00000000000000000004"}, "base": {"N": "4"}, "cnt": {"N": "1"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzIwMjcyZTM1M2M0MzRhNTE1ODVmNjY2ZDc0N2I4Mjg5AQABhgzaAQ=="} }
{ "pk": {"S": "R#clicks#0"}, "sk": {"S": "00000000000000000005"}, "base": {"N": "5"}, "cnt": {"N": "4"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzMwMzczZTQ1NGM1MzVhNjE2ODZmNzY3ZDg0OGI5Mjk5AgAD6A+aAwABghPaAQ=="} }
{ "pk": {"S": "R#clicks#0"}, "sk": {"S": "00000000000000000009"}, "base": {"N": "9"}, "cnt": {"N": "2"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzUwNTc1ZTY1NmM3MzdhODE4ODhmOTY5ZGE0YWJiMmI5AQACgga6Ag=="} }
{ "pk": {"S": "R#clicks#0"}, "sk": {"S": "00000000000000000011"}, "base": {"N": "11"}, "cnt": {"N": "4"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzgwODc4ZTk1OWNhM2FhYjFiOGJmYzZjZGQ0ZGJlMmU5AgACjgu6AgACyA26Ag=="} }
{ "pk": {"S": "R#clicks#0"}, "sk": {"S": "00000000000000000015"}, "base": {"N": "15"}, "cnt": {"N": "1"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmL2EwYTdhZWI1YmNjM2NhZDFkOGRmZTZlZGY0ZmIwMjA5AQABlg3aAQ=="} }
{ "pk": {"S": "R#clicks#1"}, "sk": {"S": "00000000000000000000"}, "base": {"N": "0"}, "cnt": {"N": "5"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzEwMTcxZTI1MmMzMzNhNDE0ODRmNTY1ZDY0NmI3Mjc5AQAF5BPaBA=="} }
{ "pk": {"S": "R#clicks#1"}, "sk": {"S": "00000000000000000005"}, "base": {"N": "5"}, "cnt": {"N": "2"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzIwMjcyZTM1M2M0MzRhNTE1ODVmNjY2ZDc0N2I4Mjg5AQAC4A26Ag=="} }
{ "pk": {"S": "R#clicks#1"}, "sk": {"S": "00000000000000000007"}, "base": {"N": "7"}, "cnt": {"N": "4"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzQwNDc0ZTU1NWM2MzZhNzE3ODdmODY4ZDk0OWJhMmE5BAABtgzaAQABkA7aAQAB6g/aAQABxBHaAQ=="} }
{ "pk": {"S": "R#clicks#1"}, "sk": {"S": "00000000000000000011"}, "base": {"N": "11"}, "cnt": {"N": "1"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzYwNjc2ZTc1N2M4MzhhOTE5ODlmYTZhZGI0YmJjMmM5AQAB/AbaAQ=="} }
{ "pk": {"S": "R#clicks#1"}, "sk": {"S": "00000000000000000012"}, "base": {"N": "12"}, "cnt": {"N": "3"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzgwODc4ZTk1OWNhM2FhYjFiOGJmYzZjZGQ0ZGJlMmU5AQADghCaAw=="} }
{ "pk": {"S": "R#clicks#1"}, "sk": {"S": "00000000000000000015"}, "base": {"N": "15"}, "cnt": {"N": "4"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzkwOTc5ZWE1YWNiM2JhYzFjOGNmZDZkZGU0ZWJmMmY5AgABjAjaAQAD5gmaAw=="} }
{ "pk": {"S": "R#clicks#1"}, "sk": {"S": "00000000000000000019"}, "base": {"N": "19"}, "cnt": {"N": "4"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmL2EwYTdhZWI1YmNjM2NhZDFkOGRmZTZlZGY0ZmIwMjA5AgAC8A66AgACqhG6Ag=="} }
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "00000000000000000000"}, "base": {"N": "0"}, "cnt": {"N": "5"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzEwMTcxZTI1MmMzMzNhNDE0ODRmNTY1ZDY0NmI3Mjc5AgADqASaAwACwge6Ag=="} }
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "00000000000000000005"}, "base": {"N": "5"}, "cnt": {"N": "2"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzIwMjcyZTM1M2M0MzRhNTE1ODVmNjY2ZDc0N2I4Mjg5AQACmAO6Ag=="} }
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "00000000000000000007"}, "base": {"N": "7"}, "cnt": {"N": "6"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzMwMzczZTQ1NGM1MzVhNjE2ODZmNzY3ZDg0OGI5Mjk5AQAGrgq6BQ=="} }
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "00000000000000000013"}, "base": {"N": "13"}, "cnt": {"N": "5"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzQwNDc0ZTU1NWM2MzZhNzE3ODdmODY4ZDk0OWJhMmE5AgABqATaAQAEggb6Aw=="} }
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "00000000000000000018"}, "base": {"N": "18"}, "cnt": {"N": "2"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzYwNjc2ZTc1N2M4MzhhOTE5ODlmYTZhZGI0YmJjMmM5AQACwgS6Ag=="} }
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "00000000000000000020"}, "base": {"N": "20"}, "cnt": {"N": "6"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzcwNzc3ZTg1OGM5MzlhYTFhOGFmYjZiZGM0Y2JkMmQ5BgABqATaAQABggbaAQAB3AfaAQABtgnaAQABkAvaAQAB6gzaAQ=="} }
{ "pk": {"S": "R#orders#0"}, "sk": {"S": "00000000000000000026"}, "base": {"N": "26"}, "cnt": {"N": "8"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmL2EwYTdhZWI1YmNjM2NhZDFkOGRmZTZlZGY0ZmIwMjA5AwACqAS6AgAC4ga6AgAEnAn6Aw=="} }
{ "pk": {"S": "R#orders#1"}, "sk": {"S": "00000000000000000000"}, "base": {"N": "0"}, "cnt": {"N": "4"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzEwMTcxZTI1MmMzMzNhNDE0ODRmNTY1ZDY0NmI3Mjc5AQAE/An6Aw=="} }
{ "pk": {"S": "R#orders#1"}, "sk": {"S": "00000000000000000004"}, "base": {"N": "4"}, "cnt": {"N": "5"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzMwMzczZTQ1NGM1MzVhNjE2ODZmNzY3ZDg0OGI5Mjk5AwAB4APaAQACugW6AgAC9Ae6Ag=="} }
{ "pk": {"S": "R#orders#1"}, "sk": {"S": "00000000000000000009"}, "base": {"N": "9"}, "cnt": {"N": "3"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzUwNTc1ZTY1NmM3MzdhODE4ODhmOTY5ZGE0YWJiMmI5AQADvAiaAw=="} }
{ "pk": {"S": "R#orders#1"}, "sk": {"S": "00000000000000000012"}, "base": {"N": "12"}, "cnt": {"N": "2"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzcwNzc3ZTg1OGM5MzlhYTFhOGFmYjZiZGM0Y2JkMmQ5AQACxA66Ag=="} }
{ "pk": {"S": "R#orders#1"}, "sk": {"S": "00000000000000000014"}, "base": {"N": "14"}, "cnt": {"N": "4"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzkwOTc5ZWE1YWNiM2JhYzFjOGNmZDZkZGU0ZWJmMmY5AgACmAO6AgAC0gW6Ag=="} }
{ "pk": {"S": "R#orders#2"}, "sk": {"S": "00000000000000000000"}, "base": {"N": "0"}, "cnt": {"N": "6"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzIwMjcyZTM1M2M0MzRhNTE1ODVmNjY2ZDc0N2I4Mjg5AgAD0gWaAwAD7AiaAw=="} }
{ "pk": {"S": "R#orders#2"}, "sk": {"S": "00000000000000000006"}, "base": {"N": "6"}, "cnt": {"N": "2"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzQwNDc0ZTU1NWM2MzZhNzE3ODdmODY4ZDk0OWJhMmE5AQAC/Am6Ag=="} }
{ "pk": {"S": "R#orders#2"}, "sk": {"S": "00000000000000000008"}, "base": {"N": "8"}, "cnt": {"N": "6"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzgwODc4ZTk1OWNhM2FhYjFiOGJmYzZjZGQ0ZGJlMmU5AwAB4APaAQACugW6AgAD9AeaAw=="} }
{ "pk": {"S": "R#orders#2"}, "sk": {"S": "00000000000000000014"}, "base": {"N": "14"}, "cnt": {"N": "5"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzkwOTc5ZWE1YWNiM2JhYzFjOGNmZDZkZGU0ZWJmMmY5AQAFgA3aBA=="} }
{ "pk": {"S": "R#orders#2"}, "sk": {"S": "00000000000000000019"}, "base": {"N": "19"}, "cnt": {"N": "1"}, "runs": {"B": "AQFOc2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmL2EwYTdhZWI1YmNjM2NhZDFkOGRmZTZlZGY0ZmIwMjA5AQAB5BPaAQ=="} }
```

Decoded index of all runs (segment label, batches, offsets covered; each run references exactly one segment here):

| pk | sk (base) | cnt | batches | covers | segment |
|---|---|---|---|---|---|
| `R#clicks#0` | `00000000000000000000` | 4 | 3 | [0,4) | S1 |
| `R#clicks#0` | `00000000000000000004` | 1 | 1 | [4,5) | S2 |
| `R#clicks#0` | `00000000000000000005` | 4 | 2 | [5,9) | S3 |
| `R#clicks#0` | `00000000000000000009` | 2 | 1 | [9,11) | S5 |
| `R#clicks#0` | `00000000000000000011` | 4 | 2 | [11,15) | S8 |
| `R#clicks#0` | `00000000000000000015` | 1 | 1 | [15,16) | S10 |
| `R#clicks#1` | `00000000000000000000` | 5 | 1 | [0,5) | S1 |
| `R#clicks#1` | `00000000000000000005` | 2 | 1 | [5,7) | S2 |
| `R#clicks#1` | `00000000000000000007` | 4 | 4 | [7,11) | S4 |
| `R#clicks#1` | `00000000000000000011` | 1 | 1 | [11,12) | S6 |
| `R#clicks#1` | `00000000000000000012` | 3 | 1 | [12,15) | S8 |
| `R#clicks#1` | `00000000000000000015` | 4 | 2 | [15,19) | S9 |
| `R#clicks#1` | `00000000000000000019` | 4 | 2 | [19,23) | S10 |
| `R#orders#0` | `00000000000000000000` | 5 | 2 | [0,5) | S1 |
| `R#orders#0` | `00000000000000000005` | 2 | 1 | [5,7) | S2 |
| `R#orders#0` | `00000000000000000007` | 6 | 1 | [7,13) | S3 |
| `R#orders#0` | `00000000000000000013` | 5 | 2 | [13,18) | S4 |
| `R#orders#0` | `00000000000000000018` | 2 | 1 | [18,20) | S6 |
| `R#orders#0` | `00000000000000000020` | 6 | 6 | [20,26) | S7 |
| `R#orders#0` | `00000000000000000026` | 8 | 3 | [26,34) | S10 |
| `R#orders#1` | `00000000000000000000` | 4 | 1 | [0,4) | S1 |
| `R#orders#1` | `00000000000000000004` | 5 | 3 | [4,9) | S3 |
| `R#orders#1` | `00000000000000000009` | 3 | 1 | [9,12) | S5 |
| `R#orders#1` | `00000000000000000012` | 2 | 1 | [12,14) | S7 |
| `R#orders#1` | `00000000000000000014` | 4 | 2 | [14,18) | S9 |
| `R#orders#2` | `00000000000000000000` | 6 | 2 | [0,6) | S2 |
| `R#orders#2` | `00000000000000000006` | 2 | 1 | [6,8) | S4 |
| `R#orders#2` | `00000000000000000008` | 6 | 3 | [8,14) | S8 |
| `R#orders#2` | `00000000000000000014` | 5 | 1 | [14,19) | S9 |
| `R#orders#2` | `00000000000000000019` | 1 | 1 | [19,20) | S10 |

### 3.6 Producer state (`S#<topic>#<p>` / producer ID)

```json
{
  "pk": {"S": "S#clicks#0"},
  "sk": {"S": "00000000000000001000"},
  "nxt": {"N": "5"},
  "pepoch": {"N": "0"},
  "v": {"N": "2"},
  "win": {"B": "W3sicyI6MCwibiI6MiwiYiI6MTEsImsiOiJzZWcvYnJva2VyLTEvYTBhMWEyYTNhNGE1YTZhN2E4YTlhYWFiYWNhZGFlYWYvODA4NzhlOTU5Y2EzYWFiMWI4YmZjNmNkZDRkYmUyZTkiLCJvIjo3MTEsImwiOjE1N30seyJzIjoyLCJuIjoyLCJiIjoxMywiayI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi84MDg3OGU5NTljYTNhYWIxYjhiZmM2Y2RkNGRiZTJlOSIsIm8iOjg2OCwibCI6MTU3fSx7InMiOjQsIm4iOjEsImIiOjE1LCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmL2EwYTdhZWI1YmNjM2NhZDFkOGRmZTZlZGY0ZmIwMjA5IiwibyI6ODQzLCJsIjoxMDl9XQ=="}
}
```

`S#clicks#0` producer 1000: epoch 0, next expected sequence 5, version 2, window (oldest first): seq 0 x2 @11 (S8); seq 2 x2 @13 (S8); seq 4 x1 @15 (S10)

```json
{
  "pk": {"S": "S#clicks#0"},
  "sk": {"S": "00000000000000001001"},
  "nxt": {"N": "1"},
  "pepoch": {"N": "0"},
  "v": {"N": "1"},
  "win": {"B": "W3sicyI6MCwibiI6MSwiYiI6NCwiayI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi8yMDI3MmUzNTNjNDM0YTUxNTg1ZjY2NmQ3NDdiODI4OSIsIm8iOjc3MSwibCI6MTA5fV0="}
}
```

`S#clicks#0` producer 1001: epoch 0, next expected sequence 1, version 1, window (oldest first): seq 0 x1 @4 (S2)

```json
{
  "pk": {"S": "S#clicks#1"},
  "sk": {"S": "00000000000000001001"},
  "nxt": {"N": "16"},
  "pepoch": {"N": "0"},
  "v": {"N": "4"},
  "win": {"B": "W3sicyI6NywibiI6MSwiYiI6OSwiayI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi80MDQ3NGU1NTVjNjM2YTcxNzg3Zjg2OGQ5NDliYTJhOSIsIm8iOjEwMTMsImwiOjEwOX0seyJzIjo4LCJuIjoxLCJiIjoxMCwiayI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi80MDQ3NGU1NTVjNjM2YTcxNzg3Zjg2OGQ5NDliYTJhOSIsIm8iOjExMjIsImwiOjEwOX0seyJzIjo5LCJuIjozLCJiIjoxMiwiayI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi84MDg3OGU5NTljYTNhYWIxYjhiZmM2Y2RkNGRiZTJlOSIsIm8iOjEwMjUsImwiOjIwNX0seyJzIjoxMiwibiI6MiwiYiI6MTksImsiOiJzZWcvYnJva2VyLTEvYTBhMWEyYTNhNGE1YTZhN2E4YTlhYWFiYWNhZGFlYWYvYTBhN2FlYjViY2MzY2FkMWQ4ZGZlNmVkZjRmYjAyMDkiLCJvIjo5NTIsImwiOjE1N30seyJzIjoxNCwibiI6MiwiYiI6MjEsImsiOiJzZWcvYnJva2VyLTEvYTBhMWEyYTNhNGE1YTZhN2E4YTlhYWFiYWNhZGFlYWYvYTBhN2FlYjViY2MzY2FkMWQ4ZGZlNmVkZjRmYjAyMDkiLCJvIjoxMTA5LCJsIjoxNTd9XQ=="}
}
```

`S#clicks#1` producer 1001: epoch 0, next expected sequence 16, version 4, window (oldest first): seq 7 x1 @9 (S4); seq 8 x1 @10 (S4); seq 9 x3 @12 (S8); seq 12 x2 @19 (S10); seq 14 x2 @21 (S10)

```json
{
  "pk": {"S": "S#orders#0"},
  "sk": {"S": "00000000000000001000"},
  "nxt": {"N": "24"},
  "pepoch": {"N": "0"},
  "v": {"N": "6"},
  "win": {"B": "W3sicyI6MTYsIm4iOjEsImIiOjIyLCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzcwNzc3ZTg1OGM5MzlhYTFhOGFmYjZiZGM0Y2JkMmQ5IiwibyI6NDk0LCJsIjoxMDl9LHsicyI6MTcsIm4iOjEsImIiOjIzLCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzcwNzc3ZTg1OGM5MzlhYTFhOGFmYjZiZGM0Y2JkMmQ5IiwibyI6NjAzLCJsIjoxMDl9LHsicyI6MTgsIm4iOjEsImIiOjI0LCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzcwNzc3ZTg1OGM5MzlhYTFhOGFmYjZiZGM0Y2JkMmQ5IiwibyI6NzEyLCJsIjoxMDl9LHsicyI6MTksIm4iOjEsImIiOjI1LCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzcwNzc3ZTg1OGM5MzlhYTFhOGFmYjZiZGM0Y2JkMmQ5IiwibyI6ODIxLCJsIjoxMDl9LHsicyI6MjAsIm4iOjQsImIiOjMwLCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmL2EwYTdhZWI1YmNjM2NhZDFkOGRmZTZlZGY0ZmIwMjA5IiwibyI6NTkwLCJsIjoyNTN9XQ=="}
}
```

`S#orders#0` producer 1000: epoch 0, next expected sequence 24, version 6, window (oldest first): seq 16 x1 @22 (S7); seq 17 x1 @23 (S7); seq 18 x1 @24 (S7); seq 19 x1 @25 (S7); seq 20 x4 @30 (S10)

```json
{
  "pk": {"S": "S#orders#1"},
  "sk": {"S": "00000000000000001000"},
  "nxt": {"N": "16"},
  "pepoch": {"N": "0"},
  "v": {"N": "4"},
  "win": {"B": "W3sicyI6NSwibiI6MiwiYiI6NSwiayI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi8zMDM3M2U0NTRjNTM1YTYxNjg2Zjc2N2Q4NDhiOTI5OSIsIm8iOjM0OSwibCI6MTU3fSx7InMiOjcsIm4iOjIsImIiOjcsImsiOiJzZWcvYnJva2VyLTEvYTBhMWEyYTNhNGE1YTZhN2E4YTlhYWFiYWNhZGFlYWYvMzAzNzNlNDU0YzUzNWE2MTY4NmY3NjdkODQ4YjkyOTkiLCJvIjo1MDYsImwiOjE1N30seyJzIjo5LCJuIjozLCJiIjo5LCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzUwNTc1ZTY1NmM3MzdhODE4ODhmOTY5ZGE0YWJiMmI5IiwibyI6NTQyLCJsIjoyMDV9LHsicyI6MTIsIm4iOjIsImIiOjE0LCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzkwOTc5ZWE1YWNiM2JhYzFjOGNmZDZkZGU0ZWJmMmY5IiwibyI6MjA0LCJsIjoxNTd9LHsicyI6MTQsIm4iOjIsImIiOjE2LCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzkwOTc5ZWE1YWNiM2JhYzFjOGNmZDZkZGU0ZWJmMmY5IiwibyI6MzYxLCJsIjoxNTd9XQ=="}
}
```

`S#orders#1` producer 1000: epoch 0, next expected sequence 16, version 4, window (oldest first): seq 5 x2 @5 (S3); seq 7 x2 @7 (S3); seq 9 x3 @9 (S5); seq 12 x2 @14 (S9); seq 14 x2 @16 (S9)

```json
{
  "pk": {"S": "S#orders#2"},
  "sk": {"S": "00000000000000001001"},
  "nxt": {"N": "15"},
  "pepoch": {"N": "0"},
  "v": {"N": "4"},
  "win": {"B": "W3sicyI6NiwibiI6MiwiYiI6NiwiayI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi80MDQ3NGU1NTVjNjM2YTcxNzg3Zjg2OGQ5NDliYTJhOSIsIm8iOjYzOCwibCI6MTU3fSx7InMiOjgsIm4iOjEsImIiOjgsImsiOiJzZWcvYnJva2VyLTEvYTBhMWEyYTNhNGE1YTZhN2E4YTlhYWFiYWNhZGFlYWYvODA4NzhlOTU5Y2EzYWFiMWI4YmZjNmNkZDRkYmUyZTkiLCJvIjoyNDAsImwiOjEwOX0seyJzIjo5LCJuIjoyLCJiIjo5LCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzgwODc4ZTk1OWNhM2FhYjFiOGJmYzZjZGQ0ZGJlMmU5IiwibyI6MzQ5LCJsIjoxNTd9LHsicyI6MTEsIm4iOjMsImIiOjExLCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzgwODc4ZTk1OWNhM2FhYjFiOGJmYzZjZGQ0ZGJlMmU5IiwibyI6NTA2LCJsIjoyMDV9LHsicyI6MTQsIm4iOjEsImIiOjE5LCJrIjoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmL2EwYTdhZWI1YmNjM2NhZDFkOGRmZTZlZGY0ZmIwMjA5IiwibyI6MTI2NiwibCI6MTA5fV0="}
}
```

`S#orders#2` producer 1001: epoch 0, next expected sequence 15, version 4, window (oldest first): seq 6 x2 @6 (S4); seq 8 x1 @8 (S8); seq 9 x2 @9 (S8); seq 11 x3 @11 (S8); seq 14 x1 @19 (S10)

### 3.7 Segment blobs (`G#<segkey>` / `-`)

One per committed segment, 1.0 to 2.6 KB each. Two shown in full: S5 (the Dup segment) and S6 (the out-of-order one), because they are the smallest and the most instructive. The other eight blobs are shown with the base64 cut off after 48 characters; decode with `base64 -d | jq`.

```json
{
  "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/50575e656c737a81888f969da4abb2b9"},
  "sk": {"S": "-"},
  "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRpdGlvbiI6MSwicmVjb3JkcyI6NCwic2VnX2tleSI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi81MDU3NWU2NTZjNzM3YTgxODg4Zjk2OWRhNGFiYjJiOSIsInNlZ19vZmYiOjEzMiwic2VnX2xlbiI6MjUzLCJwcm9kdWNlciI6MTAwMH0seyJ0b3BpYyI6ImNsaWNrcyIsInBhcnRpdGlvbiI6MCwicmVjb3JkcyI6Miwic2VnX2tleSI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi81MDU3NWU2NTZjNzM3YTgxODg4Zjk2OWRhNGFiYjJiOSIsInNlZ19vZmYiOjM4NSwic2VnX2xlbiI6MTU3fSx7InRvcGljIjoib3JkZXJzIiwicGFydGl0aW9uIjoxLCJyZWNvcmRzIjozLCJzZWdfa2V5Ijoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzUwNTc1ZTY1NmM3MzdhODE4ODhmOTY5ZGE0YWJiMmI5Iiwic2VnX29mZiI6NTQyLCJzZWdfbGVuIjoyMDUsInByb2R1Y2VyIjoxMDAwLCJiYXNlX3NlcSI6OX1dLCJSYW5nZXMiOlt7InRvcGljIjoib3JkZXJzIiwicGFydGl0aW9uIjoxLCJiYXNlIjowLCJjb3VudCI6NCwic2VnX2tleSI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi8xMDE3MWUyNTJjMzMzYTQxNDg0ZjU2NWQ2NDZiNzI3OSIsInNlZ19vZmYiOjYzOCwic2VnX2xlbiI6MjUzLCJkdXAiOnRydWV9LHsidG9waWMiOiJjbGlja3MiLCJwYXJ0aXRpb24iOjAsImJhc2UiOjksImNvdW50IjoyLCJzZWdfa2V5Ijoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzUwNTc1ZTY1NmM3MzdhODE4ODhmOTY5ZGE0YWJiMmI5Iiwic2VnX29mZiI6Mzg1LCJzZWdfbGVuIjoxNTd9LHsidG9waWMiOiJvcmRlcnMiLCJwYXJ0aXRpb24iOjEsImJhc2UiOjksImNvdW50IjozLCJzZWdfa2V5Ijoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzUwNTc1ZTY1NmM3MzdhODE4ODhmOTY5ZGE0YWJiMmI5Iiwic2VnX29mZiI6NTQyLCJzZWdfbGVuIjoyMDV9XX0="}
}
```

S5: 3 request batches, 3 answered ranges, blob 1085 bytes. Decoded blob is shown in [section 5](#5-the-special-cases).

```json
{
  "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/60676e757c838a91989fa6adb4bbc2c9"},
  "sk": {"S": "-"},
  "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRpdGlvbiI6MiwicmVjb3JkcyI6Miwic2VnX2tleSI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi82MDY3NmU3NTdjODM4YTkxOTg5ZmE2YWRiNGJiYzJjOSIsInNlZ19vZmYiOjEzMiwic2VnX2xlbiI6MTU3LCJwcm9kdWNlciI6MTAwMSwiYmFzZV9zZXEiOjE4fSx7InRvcGljIjoib3JkZXJzIiwicGFydGl0aW9uIjowLCJyZWNvcmRzIjoyLCJzZWdfa2V5Ijoic2VnL2Jyb2tlci0xL2EwYTFhMmEzYTRhNWE2YTdhOGE5YWFhYmFjYWRhZWFmLzYwNjc2ZTc1N2M4MzhhOTE5ODlmYTZhZGI0YmJjMmM5Iiwic2VnX29mZiI6Mjg5LCJzZWdfbGVuIjoxNTcsInByb2R1Y2VyIjoxMDAwLCJiYXNlX3NlcSI6MTJ9LHsidG9waWMiOiJjbGlja3MiLCJwYXJ0aXRpb24iOjEsInJlY29yZHMiOjEsInNlZ19rZXkiOiJzZWcvYnJva2VyLTEvYTBhMWEyYTNhNGE1YTZhN2E4YTlhYWFiYWNhZGFlYWYvNjA2NzZlNzU3YzgzOGE5MTk4OWZhNmFkYjRiYmMyYzkiLCJzZWdfb2ZmIjo0NDYsInNlZ19sZW4iOjEwOX1dLCJSYW5nZXMiOlt7InRvcGljIjoib3JkZXJzIiwicGFydGl0aW9uIjoyLCJiYXNlIjotMSwiY291bnQiOjIsInNlZ19rZXkiOiJzZWcvYnJva2VyLTEvYTBhMWEyYTNhNGE1YTZhN2E4YTlhYWFiYWNhZGFlYWYvNjA2NzZlNzU3YzgzOGE5MTk4OWZhNmFkYjRiYmMyYzkiLCJzZWdfb2ZmIjoxMzIsInNlZ19sZW4iOjE1NywiZXJyIjoiRXJyT3V0T2ZPcmRlciJ9LHsidG9waWMiOiJvcmRlcnMiLCJwYXJ0aXRpb24iOjAsImJhc2UiOjE4LCJjb3VudCI6Miwic2VnX2tleSI6InNlZy9icm9rZXItMS9hMGExYTJhM2E0YTVhNmE3YThhOWFhYWJhY2FkYWVhZi82MDY3NmU3NTdjODM4YTkxOTg5ZmE2YWRiNGJiYzJjOSIsInNlZ19vZmYiOjI4OSwic2VnX2xlbiI6MTU3fSx7InRvcGljIjoiY2xpY2tzIiwicGFydGl0aW9uIjoxLCJiYXNlIjoxMSwiY291bnQiOjEsInNlZ19rZXkiOiJzZWcvYnJva2VyLTEvYTBhMWEyYTNhNGE1YTZhN2E4YTlhYWFiYWNhZGFlYWYvNjA2NzZlNzU3YzgzOGE5MTk4OWZhNmFkYjRiYmMyYzkiLCJzZWdfb2ZmIjo0NDYsInNlZ19sZW4iOjEwOX1dfQ=="}
}
```

S6: 3 request batches, 3 answered ranges, blob 1114 bytes. Decoded blob is shown in [section 5](#5-the-special-cases).

```json
{ "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/10171e252c333a41484f565d646b7279"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/20272e353c434a51585f666d747b8289"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/30373e454c535a61686f767d848b9299"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/40474e555c636a71787f868d949ba2a9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/70777e858c939aa1a8afb6bdc4cbd2d9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/80878e959ca3aab1b8bfc6cdd4dbe2e9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/90979ea5acb3bac1c8cfd6dde4ebf2f9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/broker-1/a0a1a2a3a4a5a6a7a8a9aaabacadaeaf/a0a7aeb5bcc3cad1d8dfe6edf4fb0209"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
```

| label | key suffix | batches | ranges | dup | rejected | blob bytes |
|---|---|---|---|---|---|---|
| S1 | …646b7279 | 7 | 7 | 0 | 0 | 2452 |
| S2 | …747b8289 | 5 | 5 | 0 | 0 | 1789 |
| S3 | …848b9299 | 6 | 6 | 0 | 0 | 2125 |
| S4 | …949ba2a9 | 7 | 7 | 0 | 0 | 2579 |
| S5 | …a4abb2b9 | 3 | 3 | 1 | 0 | 1085 |
| S6 | …b4bbc2c9 | 3 | 3 | 0 | 1 | 1114 |
| S7 | …c4cbd2d9 | 7 | 7 | 0 | 0 | 2556 |
| S8 | …d4dbe2e9 | 6 | 6 | 0 | 0 | 2202 |
| S9 | …e4ebf2f9 | 5 | 5 | 0 | 0 | 1764 |
| S10 | …f4fb0209 | 7 | 7 | 0 | 0 | 2529 |
## 4. Timeline: how 10 commits build the table

Item count after each step (full Scan): setup 4, S1 16, S2 24, S3 28, S4 32, S5 35, S6 38, S7 41, (replay of S7: still 41), S8 46, S9 50, S10 55. `A` = producer 1000, `B` = producer 1001, `n` = non-idempotent. Each commit also adds one `G#` blob. "hwm a -> b" means that partition's `R#<topic>#<p> | !` item moved from a to b. The `S#` bullets show `nxt` (next expected sequence), the window of sequences kept, and the version `v` (bumped once per commit that changes that state).

### Setup (Register, 2x CreateTopic, 2x InitProducer)

Setup calls only; no segment.

- `N#broker-1 | -` epoch (absent) -> 1
- `PID | -` n (absent) -> 2
- `TOPICS | clicks` parts (absent) -> 2
- `TOPICS | orders` parts (absent) -> 3

### After S1

`G#…646b7279`, first commit: every partition starts at offset 0. Batches (producer+records): orders/0: A3 A2; orders/1: A4; clicks/0: n1 n1 n2; clicks/1: B5.

- NEW `G#…646b7279` (segment blob, 2452 bytes JSON)
- `R#clicks#0 | !` hwm 0 -> 4 (**new item**)
- NEW run `R#clicks#0 | 00000000000000000000`: 3 batch(es), 4 records
- `R#clicks#1 | !` hwm 0 -> 5 (**new item**)
- NEW run `R#clicks#1 | 00000000000000000000`: 1 batch(es), 5 records
- `R#orders#0 | !` hwm 0 -> 5 (**new item**)
- NEW run `R#orders#0 | 00000000000000000000`: 2 batch(es), 5 records
- `R#orders#1 | !` hwm 0 -> 4 (**new item**)
- NEW run `R#orders#1 | 00000000000000000000`: 1 batch(es), 4 records
- `S#clicks#1 | producer 1001` nxt 0 -> 5, window seqs [0], v 0 -> 1 (**new item**)
- `S#orders#0 | producer 1000` nxt 0 -> 5, window seqs [0 3], v 0 -> 1 (**new item**)
- `S#orders#1 | producer 1000` nxt 0 -> 4, window seqs [0], v 0 -> 1 (**new item**)

### After S2

`G#…747b8289`, second producer arrives on orders/2 and clicks/0. Batches (producer+records): orders/0: A2; orders/2: B3 B3; clicks/0: B1; clicks/1: n2.

- NEW `G#…747b8289` (segment blob, 1789 bytes JSON)
- `R#clicks#0 | !` hwm 4 -> 5
- NEW run `R#clicks#0 | 00000000000000000004`: 1 batch(es), 1 records
- `R#clicks#1 | !` hwm 5 -> 7
- NEW run `R#clicks#1 | 00000000000000000005`: 1 batch(es), 2 records
- `R#orders#0 | !` hwm 5 -> 7
- NEW run `R#orders#0 | 00000000000000000005`: 1 batch(es), 2 records
- `R#orders#2 | !` hwm 0 -> 6 (**new item**)
- NEW run `R#orders#2 | 00000000000000000000`: 2 batch(es), 6 records
- `S#clicks#0 | producer 1001` nxt 0 -> 1, window seqs [0], v 0 -> 1 (**new item**)
- `S#orders#0 | producer 1000` nxt 5 -> 7, window seqs [0 3 5], v 1 -> 2
- `S#orders#2 | producer 1001` nxt 0 -> 6, window seqs [0 3], v 0 -> 1 (**new item**)

### After S3

`G#…848b9299`, non-idempotent batches interleave with idempotent ones. Batches (producer+records): orders/1: A1 A2 A2; orders/0: n6; clicks/0: n3 n1.

- NEW `G#…848b9299` (segment blob, 2125 bytes JSON)
- `R#clicks#0 | !` hwm 5 -> 9
- NEW run `R#clicks#0 | 00000000000000000005`: 2 batch(es), 4 records
- `R#orders#0 | !` hwm 7 -> 13
- NEW run `R#orders#0 | 00000000000000000007`: 1 batch(es), 6 records
- `R#orders#1 | !` hwm 4 -> 9
- NEW run `R#orders#1 | 00000000000000000004`: 3 batch(es), 5 records
- `S#orders#1 | producer 1000` nxt 4 -> 9, window seqs [0 4 5 7], v 1 -> 2

### After S4

`G#…949ba2a9`, four small clicks batches. Batches (producer+records): orders/0: A1 A4; orders/2: B2; clicks/1: B1 B1 B1 B1.

- NEW `G#…949ba2a9` (segment blob, 2579 bytes JSON)
- `R#clicks#1 | !` hwm 7 -> 11
- NEW run `R#clicks#1 | 00000000000000000007`: 4 batch(es), 4 records
- `R#orders#0 | !` hwm 13 -> 18
- NEW run `R#orders#0 | 00000000000000000013`: 2 batch(es), 5 records
- `R#orders#2 | !` hwm 6 -> 8
- NEW run `R#orders#2 | 00000000000000000006`: 1 batch(es), 2 records
- `S#clicks#1 | producer 1001` nxt 5 -> 9, window seqs [0 5 6 7 8], v 1 -> 2
- `S#orders#0 | producer 1000` nxt 7 -> 12, window seqs [0 3 5 7 8], v 2 -> 3
- `S#orders#2 | producer 1001` nxt 6 -> 8, window seqs [0 3 6], v 1 -> 2

### After S5

`G#…a4abb2b9`, DUP: producer A resends orders/1 seq 0 (already committed by S1). Batches (producer+records): orders/1: A4 A3; clicks/0: n2.

- NEW `G#…a4abb2b9` (segment blob, 1085 bytes JSON)
- `R#clicks#0 | !` hwm 9 -> 11
- NEW run `R#clicks#0 | 00000000000000000009`: 1 batch(es), 2 records
- `R#orders#1 | !` hwm 9 -> 12
- NEW run `R#orders#1 | 00000000000000000009`: 1 batch(es), 3 records
- `S#orders#1 | producer 1000` nxt 9 -> 12, window seqs [0 4 5 7 9], v 2 -> 3

### After S6

`G#…b4bbc2c9`, OUT-OF-ORDER: producer B sends orders/2 with a sequence gap. Batches (producer+records): orders/2: B2; orders/0: A2; clicks/1: n1.

- NEW `G#…b4bbc2c9` (segment blob, 1114 bytes JSON)
- `R#clicks#1 | !` hwm 11 -> 12
- NEW run `R#clicks#1 | 00000000000000000011`: 1 batch(es), 1 records
- `R#orders#0 | !` hwm 18 -> 20
- NEW run `R#orders#0 | 00000000000000000018`: 1 batch(es), 2 records
- `S#orders#0 | producer 1000` nxt 12 -> 14, window seqs [3 5 7 8 12], v 3 -> 4

### After S7

`G#…c4cbd2d9`, six A batches on orders/0: overflows the 5-entry dup window. Batches (producer+records): orders/0: A1 A1 A1 A1 A1 A1; orders/1: n2.

- NEW `G#…c4cbd2d9` (segment blob, 2556 bytes JSON)
- `R#orders#0 | !` hwm 20 -> 26
- NEW run `R#orders#0 | 00000000000000000020`: 6 batch(es), 6 records
- `R#orders#1 | !` hwm 12 -> 14
- NEW run `R#orders#1 | 00000000000000000012`: 1 batch(es), 2 records
- `S#orders#0 | producer 1000` nxt 14 -> 20, window seqs [15 16 17 18 19], v 4 -> 5

### Replay of S7 (same key, same batches)

Same key and same batches as S7 again.

- (no item changed)

### After S8

`G#…d4dbe2e9`, producer A appears on clicks/0. Batches (producer+records): orders/2: B1 B2 B3; clicks/0: A2 A2; clicks/1: B3.

- NEW `G#…d4dbe2e9` (segment blob, 2202 bytes JSON)
- `R#clicks#0 | !` hwm 11 -> 15
- NEW run `R#clicks#0 | 00000000000000000011`: 2 batch(es), 4 records
- `R#clicks#1 | !` hwm 12 -> 15
- NEW run `R#clicks#1 | 00000000000000000012`: 1 batch(es), 3 records
- `R#orders#2 | !` hwm 8 -> 14
- NEW run `R#orders#2 | 00000000000000000008`: 3 batch(es), 6 records
- `S#clicks#0 | producer 1000` nxt 0 -> 4, window seqs [0 2], v 0 -> 1 (**new item**)
- `S#clicks#1 | producer 1001` nxt 9 -> 12, window seqs [5 6 7 8 9], v 2 -> 3
- `S#orders#2 | producer 1001` nxt 8 -> 14, window seqs [3 6 8 9 11], v 2 -> 3

### After S9

`G#…e4ebf2f9`, mixed. Batches (producer+records): orders/1: A2 A2; clicks/1: n1 n3; orders/2: n5.

- NEW `G#…e4ebf2f9` (segment blob, 1764 bytes JSON)
- `R#clicks#1 | !` hwm 15 -> 19
- NEW run `R#clicks#1 | 00000000000000000015`: 2 batch(es), 4 records
- `R#orders#1 | !` hwm 14 -> 18
- NEW run `R#orders#1 | 00000000000000000014`: 2 batch(es), 4 records
- `R#orders#2 | !` hwm 14 -> 19
- NEW run `R#orders#2 | 00000000000000000014`: 1 batch(es), 5 records
- `S#orders#1 | producer 1000` nxt 12 -> 16, window seqs [5 7 9 12 14], v 3 -> 4

### After S10

`G#…f4fb0209`, last segment: five partitions touched. Batches (producer+records): orders/0: n2 n2 A4; clicks/0: A1; clicks/1: B2 B2; orders/2: B1.

- NEW `G#…f4fb0209` (segment blob, 2529 bytes JSON)
- `R#clicks#0 | !` hwm 15 -> 16
- NEW run `R#clicks#0 | 00000000000000000015`: 1 batch(es), 1 records
- `R#clicks#1 | !` hwm 19 -> 23
- NEW run `R#clicks#1 | 00000000000000000019`: 2 batch(es), 4 records
- `R#orders#0 | !` hwm 26 -> 34
- NEW run `R#orders#0 | 00000000000000000026`: 3 batch(es), 8 records
- `R#orders#2 | !` hwm 19 -> 20
- NEW run `R#orders#2 | 00000000000000000019`: 1 batch(es), 1 records
- `S#clicks#0 | producer 1000` nxt 4 -> 5, window seqs [0 2 4], v 1 -> 2
- `S#clicks#1 | producer 1001` nxt 12 -> 16, window seqs [7 8 9 12 14], v 3 -> 4
- `S#orders#0 | producer 1000` nxt 20 -> 24, window seqs [16 17 18 19 20], v 5 -> 6
- `S#orders#2 | producer 1001` nxt 14 -> 15, window seqs [6 8 9 11 14], v 3 -> 4

Reading the timeline: the high watermarks only ever move forward and each run item starts exactly where the previous hwm stopped (compare S2: `orders/0` hwm 5 -> 7 and the new run has sk `...05`). A partition untouched by a segment gets neither an hwm update nor a run (S3 never touches `clicks/1`). The `S#` items are the only per-batch growth, and even those are capped by the 5-entry window: after S7, `S#orders#0` for A holds only seqs 15 to 19, and S10 slides it to 16 to 20.

## 5. The special cases

### 5.1 Dup: S5 resends a batch committed by S1

Producer A re-sent `orders/1`, seq 0, 4 records (it had already been committed in S1 at offsets [0,4)), bundled with a fresh `clicks/0` batch and a new `orders/1` batch (seq 9, 3 records). The control plane found seq 0 in A's dup window, saw the same record count, and answered with the **original** range. The answer in the `G#` blob of S5 (abbreviated):

```
R orders/1 base 0 count 4 seg S1 off 638 len 253 dup:true   <- points into S1's object, not S5's
R clicks/0 base 9 count 2 seg S5 off 385 len 157
R orders/1 base 9 count 3 seg S5 off 542 len 205
```

Rows written by S5: the `G#` blob; hwm `clicks#0` 9 -> 11 and a run at `...09` (2 records); hwm `orders#1` 9 -> 12 (3 records, not 7) and a run at `...09` (1 batch, 3 records); `S#orders#1` for A: `nxt` 9 -> 12, window `[0 4 5 7]` + seq 9, `v` 2 -> 3.

Rows **not** written: no run for the duplicate (the original run at `R#orders#1 | ...00` is untouched), no hwm advance for its 4 records, no window entry or `nxt` change for seq 0. The duplicate's own bytes (off 132, len 253 inside S5's object) are simply orphaned in the object store; the `G#` blob still records the request, which is what makes a replay of S5 return the same answer.

### 5.2 OutOfOrder: S6 sends a sequence gap

Producer B sent `orders/2` with base seq 18 while the state said `nxt` = 8. The segment still commits (the other two batches are fine); the bad batch gets an error range:

```
R orders/2 base -1 count 2 err ErrOutOfOrder seg S6 off 132 len 157
R orders/0 base 18 count 2 seg S6 off 289 len 157
R clicks/1 base 11 count 1 seg S6 off 446 len 109
```

Rows written: the `G#` blob (which stores the `err` in its range), `orders#0` hwm 18 -> 20 plus run `...18`, `clicks#1` hwm 11 -> 12 plus run `...11`, and `S#orders#0` for A (`nxt` 12 -> 14, `v` 3 -> 4). Rows **not** written: nothing for `orders/2` at all. Its hwm stays 8, its `S#` item for B stays at `nxt` 8, `v` 2, and there is no run (the broker would see `ErrOutOfOrder` and have the client retry). Note that the commit as a whole answered OK; rejection is per batch.

### 5.3 Replay of an already committed segment key

Re-sending S7 (same key, same batches) returns the stored answer verbatim (byte-for-byte equal ranges) and the Scan before and after is identical: 41 items, no attribute changed. The replay check is the first read, a `BatchGetItem` that includes `G#<key>`, so it wins even if the node epoch has moved on. Two neighbours that also change nothing:

- Same key as S1 but a different batch list: answered `ErrConflict`, nothing written.
- A fresh key with the wrong fencing epoch (registered epoch + 7): answered `ErrFenced`, nothing written (the fence check rejects it before the transaction is built).

### 5.4 Lookup

`Lookup` is a consistent `GetItem` of the hwm plus a `Query` on `R#<topic>#<p>` with `sk BETWEEN <run holding From> AND <hwm-1>`; each run is expanded into per-batch ranges. Results on the final table:

`Lookup{orders/0, From: 5, MaxBytes: 400}` -> `OK`, `HighWatermark: 34`:

| base | count | segment | byte off | byte len |
|---|---|---|---|---|
| 5 | 2 | S2 | 204 | 157 |
| 7 | 6 | S3 | 663 | 349 |

The scan stops when the running byte total reaches `MaxBytes`, and the batch that crosses it is still returned: 157 < 400, then 157 + 349 = 506 >= 400, so two ranges. It started at the run with base 5 (the run holding offset 5 is `R#orders#0 | ...05`); an earlier batch of a run that ends before `From` would have been dropped.

`Lookup{clicks/1, From: 0, MaxBytes: 1 MiB}` -> `OK`, `HighWatermark: 23`: 12 ranges from 7 run items, in offset order: `[0,5) S1`, `[5,7) S2`, `[7,8) [8,9) [9,10) [10,11) S4`, `[11,12) S6`, `[12,15) S8`, `[15,16) S9`, `[16,19) S9`, `[19,21) [21,23) S10`. The four one-record S4 batches come from a single run item.

## 6. Inspect it yourself

With the backends from `scripts/it-up.sh` running (ExtendDB on `http://127.0.0.1:18080`; it is in-memory, so a restart empties it) the stock AWS CLI works with the seeded dummy credential. Nothing here touches AWS.

```sh
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY AWS_DEFAULT_REGION=us-east-1
EP=http://127.0.0.1:18080
aws dynamodb list-tables --endpoint-url $EP
aws dynamodb scan --table-name demo_cp --endpoint-url $EP --select COUNT            # 55 for this data set
aws dynamodb scan --table-name demo_cp --endpoint-url $EP --output json > scan.json
# one partition: HWM plus runs, in offset order
aws dynamodb query --table-name demo_cp --endpoint-url $EP \
  --key-condition-expression "pk = :p" --expression-attribute-values '{":p":{"S":"R#orders#0"}}'
# decode a segment blob
jq -r '.Items[] | select(.pk.S|startswith("G#")) | .blob.B' scan.json | head -1 | base64 -d | jq .
```

The CLI hands back `B` attributes base64-encoded, which is how they are printed above. A real service would also page a Scan at 1 MB; the sample is small enough for one page, and the dump program used `Limit: 25` and followed `LastEvaluatedKey` to prove the paging.

To reproduce the dump: start ExtendDB, build a `dynamo.New(client, "demo_")` store on a `dynamodb.New(Options{BaseEndpoint: ...})` client as in `cp/dynamo/commit_ext_test.go`, call `CreateTables`, `Register`, `CreateTopic` x2, `InitProducer` x2, then `CommitSegment` ten times and scan. The part that makes the bytes realistic:

<details><summary>Key parts of the throwaway program (not committed)</summary>

```go
st := dynamo.New(db, "demo_")
st.CreateTables(ctx)
reg, _ := st.Register(ctx, core.Register{Self: self})      // self = {broker-1, incarnation a0a1..af}
st.CreateTopic(ctx, "orders", 3); st.CreateTopic(ctx, "clicks", 2)
A, _ := st.InitProducer(ctx); B, _ := st.InitProducer(ctx)  // 1000, 1001

// segment bytes and Loc ranges exactly as the broker builds them
obj, ents, _ := format.Encode([]format.Batch{{TP: tp, Records: n, Data: bytes.Repeat([]byte{'x'}, 61+48*n)}, ...})
key := format.SegmentKey("broker-1", inc, id)             // seg/broker-1/<inc hex>/<id hex>
pb := core.PendingBatch{TP: tp, Records: n, Loc: ents[i].Ref(key), Producer: A, BaseSeq: seq}
resp, code, err := st.CommitSegment(ctx, core.CommitSegment{SegmentKey: key, Owner: self, Epoch: reg.Epoch, Batches: pbs})

// full scan, all pages, sorted by (pk, sk)
for { out, _ := db.Scan(ctx, &dynamodb.ScanInput{TableName: &tbl, ConsistentRead: aws.Bool(true), Limit: aws.Int32(25), ExclusiveStartKey: lek}); ...; if out.LastEvaluatedKey == nil { break }; lek = out.LastEvaluatedKey }
```

Packed runs are decoded by reading the format byte, the key table and the `(keyIdx, count, off, len)` varints (zigzag for the last two), mirroring `decodeRun` in `cp/dynamo/runs.go`.

</details>

## What was surprising

- Producer state is keyed by *producer*, not by node: `S#orders#0` / `1000` is the same row whichever broker commits for A.
- The `win` blob and every `G#` blob repeat the full ~90-character segment key once per entry, while `runs` stores each distinct key once; the state items are therefore the heaviest per-batch cost (about 820 base64 characters for a full 5-entry window, versus 100 to 150 for a run).
- Sequence numbers count records, not batches: after S1 producer A's next seq on `orders/0` is 5 (3 + 2 records).
- A segment containing a Dup or a rejected batch still commits and writes a `G#` blob; the rejected or duplicate batch's bytes stay orphaned in the object.
