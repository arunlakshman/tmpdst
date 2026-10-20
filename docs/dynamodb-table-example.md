# What the DynamoDB control-plane table looks like after 10 commits

This page shows a real `Scan` of the control-plane table (`cp/dynamo`) after brokers committed ten segments across two topics. Nothing here is invented. A throwaway Go program drove the real `dynamo.Store` API against ExtendDB (`scripts/it-up.sh`), then scanned the whole table with the DynamoDB API and decoded every item. The segment layout and keys come from the real `format.Encode` / `format.SegmentKey`, so the byte offsets and lengths are what the broker would build for batches of those sizes. Batch payload bytes are filler (`x`); the control plane never reads them.

The layout is the one of decisions 41 to 44: no producer state, no fencing epoch, no high-watermark item. The control plane assigns every offset itself, as the end of the partition's newest run, and appends with a compare-and-set.

Sections: [1 Key schema](#1-key-schema-legend) · [2 Scan summary](#2-scan-summary) · [3 Every item](#3-the-scan-item-by-item) · [4 Timeline](#4-timeline-how-10-commits-build-the-table) · [5 Replay / conflict / lost race](#5-the-special-cases) · [6 Inspect it yourself](#6-inspect-it-yourself)

## 1. Key schema (legend)

One table, `demo_cp` (prefix `demo_` + `cp`), string keys `pk` (hash) and `sk` (range), on-demand billing. Design source: `cp/doc.go`.

| pk | sk | attributes | what it is |
|---|---|---|---|
| `TOPICS` | `<topic>` | `parts` N | partition count (`CreateTopic` and growth only ever raise it). |
| `G#<segkey>` | `-` | `blob` B | one item per committed segment: JSON of the request batches plus the full answer. Written with `attribute_not_exists(pk)`, so it is the idempotency key: a replay reads it and returns the stored answer, a different request under the same key is `ErrConflict`. |
| `R#<topic>#<p>` | 20-digit base offset | `base` N, `cnt` N, `runs` B | a **run**: all batches one commit assigned to one partition. Sort key is the first offset, `cnt` the total records, `runs` the packed batch list. Written with `attribute_not_exists(sk)`: that is the compare-and-set. |

There is no `N#<node>` item (no epoch), no `PID` counter, no `S#` producer state and no `R#<topic>#<p> | !` high watermark any more. The next offset of a partition is the end of its **newest run**: the control plane reads it with one consistent descending `Query` (`Limit 1`) on `R#<topic>#<p>` and takes `base + cnt` of the item it gets (0 if there is none).

Decoding the packed `runs` blob (all integers are varints): `0x01` format byte; `nkeys`, then `nkeys` x (length, bytes) distinct segment keys; `nbatches`, then per batch `keyIdx, count, off, len` (`off` and `len` zigzag). Batch *i* starts at `base + sum(count[:i])`, so offsets are never stored. `off`/`len` are the byte range of the batch inside the segment object (the object starts with a header and an index, so the first batch never sits at 0).

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
   "seg_len": 205
  },
  {
   "topic": "orders",
   "partition": 0,
   "records": 2,
   "seg_key": "…646b7279",
   "seg_off": 481,
   "seg_len": 157
  },
  {
   "topic": "orders",
   "partition": 1,
   "records": 4,
   "seg_key": "…646b7279",
   "seg_off": 638,
   "seg_len": 253
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

Segment keys are flat and random, `seg/<32 hex>` (decision 43); the control plane treats them as opaque strings. Below a key is shortened to its last 8 hex digits (`…646b7279`) or to its label `S1`..`S10` (commit order).

### Log store layout (`-store dynamolog`, decisions 48-53)

The log store uses the SAME table (`<prefix>cp`, keys `pk`/`sk`) with a different set of items; nothing below is written by
`-store dynamo` and the `G#` idempotency items are NOT written (the idempotency window lives in the applier). After a run:

| pk | sk | attributes | what it is |
|---|---|---|---|
| `LOG` | slot, 20 digits | `d` B (entry: version byte, rank, writer, append id, ts, count, commands), `v`, `w` writer, `id`, `ts`, `n` commands, `ttl` | one item per log slot, written with `attribute_not_exists(pk)`: the slot is the fence. Zero commands is a no-op (heartbeat, hole fill). Trimmed slots are overwritten with a tombstone stub. `ttl` (epoch s, `-log-ttl` 7 d) lets the table's TTL expire old slots. |
| `CKPT` | `-` | `ck_slot` N, `ck_key` S, `ck_size` N, `ck_sum` S (sha-256), `ck_ts` N (ms) | forward-only conditional put; the object is `cp/ckpt/<slot>` in the checkpoint bucket. |
| `IDX` | `-` | `ix_slot` N | index watermark: slots below it are in the R# rows. |
| `TOPICS` | `<topic>` | `parts` N | written by the indexer, after the ack (same as the old store). |
| `R#<topic>#<p>` | 20-digit base offset | `base`, `cnt`, `runs` | the same run rows as section 1, but written by the indexer up to 10 s AFTER the commit is acked, per partition in offset order; a scan right after an ack may not show them yet. They are not read by the log service (Lookup is answered from Pebble); they serve a rollback to the old store and a future S3 index. |

So after N acked commits a scan shows about N/(commands per slot) `LOG` items plus heartbeats (one a second when idle), one
`CKPT`, one `IDX`, and the `R#` rows once the next flush has passed. The worked scan below is of the old store; it was not
regenerated for the log store (no live scan was taken, so no example item is invented here).

## 2. Scan summary

| type | items | notes |
|---|---|---|
| TOPICS | 2 | orders, clicks |
| G# | 10 | one per committed segment |
| R# run | 31 | one per (commit, partition) with new batches |
| **total** | **43** | about 17909 bytes of attribute names plus values |

The Scan was paged (`Limit` 25, `LastEvaluatedKey` followed) and sorted client-side by (`pk`, `sk`); a Scan itself returns items in no useful order. The two topics: `orders` (3 partitions) and `clicks` (2). The earlier layout (before decisions 41 to 44) needed 55 items for a similar ten commits: a node item, a producer counter, 5 high watermarks and 6 producer states on top of the topics, blobs and runs. This one needs 43.

Hot spot: 31 run items for 10 commits. The table grows by about one run per (commit, partition), not per batch: S7 put 6 batches into one run item. A commit writes **1 + (distinct partitions) + (grown topics)** items in one transaction.

## 3. The scan, item by item

Sorted by (`pk`, `sk`) within each group. `B` values are base64 of the packed or JSON bytes described above.

### 3.1 Topics (`TOPICS`)

```json
{ "pk": {"S": "TOPICS"}, "sk": {"S": "clicks"}, "parts": {"N": "2"} }
{ "pk": {"S": "TOPICS"}, "sk": {"S": "orders"}, "parts": {"N": "3"} }
```

- `clicks`: 2 partitions.
- `orders`: 3 partitions.

### 3.2 Range runs (`R#<topic>#<p>` / base offset)

One item per commit per partition; there is no other item under `R#...`. Two shown in full with the packed `runs` blob decoded; the end of a partition is the `base + cnt` of its last item in sort order.

```json
{
  "pk": {"S": "R#clicks#0"},
  "sk": {"S": "00000000000000000000"},
  "base": {"N": "0"},
  "cnt": {"N": "4"},
  "runs": {"B": "AQEkc2VnLzEwMTcxZTI1MmMzMzNhNDE0ODRmNTY1ZDY0NmI3Mjc5AwAB9g3aAQAB0A/aAQACqhG6Ag=="}
}
```

Decoded `runs` (58 bytes):

| # | segment | offsets | records | byte offset | byte length |
|---|---|---|---|---|---|
| 0 | …646b7279 (S1) | [0,1) | 1 | 891 | 109 |
| 1 | …646b7279 (S1) | [1,2) | 1 | 1000 | 109 |
| 2 | …646b7279 (S1) | [2,4) | 2 | 1109 | 157 |

`R#clicks#0` / `00000000000000000000`: base 0, cnt 4, so this run covers offsets [0, 4).

The largest run, `R#orders#0` / `00000000000000000020` (S7, six single-record batches in one item):

```json
{
  "pk": {"S": "R#orders#0"},
  "sk": {"S": "00000000000000000020"},
  "base": {"N": "20"},
  "cnt": {"N": "6"},
  "runs": {"B": "AQEkc2VnLzcwNzc3ZTg1OGM5MzlhYTFhOGFmYjZiZGM0Y2JkMmQ5BgABqATaAQABggbaAQAB3AfaAQABtgnaAQABkAvaAQAB6gzaAQ=="}
}
```

Decoded `runs` (76 bytes):

| # | segment | offsets | records | byte offset | byte length |
|---|---|---|---|---|---|
| 0 | …c4cbd2d9 (S7) | [20,21) | 1 | 276 | 109 |
| 1 | …c4cbd2d9 (S7) | [21,22) | 1 | 385 | 109 |
| 2 | …c4cbd2d9 (S7) | [22,23) | 1 | 494 | 109 |
| 3 | …c4cbd2d9 (S7) | [23,24) | 1 | 603 | 109 |
| 4 | …c4cbd2d9 (S7) | [24,25) | 1 | 712 | 109 |
| 5 | …c4cbd2d9 (S7) | [25,26) | 1 | 821 | 109 |

`R#orders#0` / `00000000000000000020`: base 20, cnt 6, so this run covers offsets [20, 26).

All 31 runs, in (`pk`, `sk`) order (`end` is `base + cnt`):

| pk | sk | base | cnt | end | batches | segments |
|---|---|---|---|---|---|---|
| `R#clicks#0` | `00000000000000000000` | 0 | 4 | 4 | 3 | S1 |
| `R#clicks#0` | `00000000000000000004` | 4 | 1 | 5 | 1 | S2 |
| `R#clicks#0` | `00000000000000000005` | 5 | 4 | 9 | 2 | S3 |
| `R#clicks#0` | `00000000000000000009` | 9 | 2 | 11 | 1 | S5 |
| `R#clicks#0` | `00000000000000000011` | 11 | 4 | 15 | 2 | S8 |
| `R#clicks#0` | `00000000000000000015` | 15 | 1 | 16 | 1 | S10 |
| `R#clicks#1` | `00000000000000000000` | 0 | 5 | 5 | 1 | S1 |
| `R#clicks#1` | `00000000000000000005` | 5 | 2 | 7 | 1 | S2 |
| `R#clicks#1` | `00000000000000000007` | 7 | 4 | 11 | 4 | S4 |
| `R#clicks#1` | `00000000000000000011` | 11 | 1 | 12 | 1 | S6 |
| `R#clicks#1` | `00000000000000000012` | 12 | 3 | 15 | 1 | S8 |
| `R#clicks#1` | `00000000000000000015` | 15 | 4 | 19 | 2 | S9 |
| `R#clicks#1` | `00000000000000000019` | 19 | 4 | 23 | 2 | S10 |
| `R#orders#0` | `00000000000000000000` | 0 | 5 | 5 | 2 | S1 |
| `R#orders#0` | `00000000000000000005` | 5 | 2 | 7 | 1 | S2 |
| `R#orders#0` | `00000000000000000007` | 7 | 6 | 13 | 1 | S3 |
| `R#orders#0` | `00000000000000000013` | 13 | 5 | 18 | 2 | S4 |
| `R#orders#0` | `00000000000000000018` | 18 | 2 | 20 | 1 | S6 |
| `R#orders#0` | `00000000000000000020` | 20 | 6 | 26 | 6 | S7 |
| `R#orders#0` | `00000000000000000026` | 26 | 8 | 34 | 3 | S10 |
| `R#orders#1` | `00000000000000000000` | 0 | 4 | 4 | 1 | S1 |
| `R#orders#1` | `00000000000000000004` | 4 | 5 | 9 | 3 | S3 |
| `R#orders#1` | `00000000000000000009` | 9 | 7 | 16 | 2 | S5 |
| `R#orders#1` | `00000000000000000016` | 16 | 2 | 18 | 1 | S7 |
| `R#orders#1` | `00000000000000000018` | 18 | 4 | 22 | 2 | S9 |
| `R#orders#2` | `00000000000000000000` | 0 | 6 | 6 | 2 | S2 |
| `R#orders#2` | `00000000000000000006` | 6 | 2 | 8 | 1 | S4 |
| `R#orders#2` | `00000000000000000008` | 8 | 2 | 10 | 1 | S6 |
| `R#orders#2` | `00000000000000000010` | 10 | 6 | 16 | 3 | S8 |
| `R#orders#2` | `00000000000000000016` | 16 | 5 | 21 | 1 | S9 |
| `R#orders#2` | `00000000000000000021` | 21 | 1 | 22 | 1 | S10 |

Final end offset per partition = `end` of the last row of each `pk` (the newest run): `R#clicks#0` 16, `R#clicks#1` 23, `R#orders#0` 34, `R#orders#1` 22, `R#orders#2` 22.

### 3.3 Segment blobs (`G#<segkey>` / `-`)

One per committed segment, the idempotency key. S5 is shown in full; the others are cut after 48 base64 characters (decode with `base64 -d | jq`).

```json
{ "pk": {"S": "G#seg/10171e252c333a41484f565d646b7279"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/20272e353c434a51585f666d747b8289"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/30373e454c535a61686f767d848b9299"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/40474e555c636a71787f868d949ba2a9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{
  "pk": {"S": "G#seg/50575e656c737a81888f969da4abb2b9"},
  "sk": {"S": "-"},
  "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRpdGlvbiI6MSwicmVjb3JkcyI6NCwic2VnX2tleSI6InNlZy81MDU3NWU2NTZjNzM3YTgxODg4Zjk2OWRhNGFiYjJiOSIsInNlZ19vZmYiOjEzMiwic2VnX2xlbiI6MjUzfSx7InRvcGljIjoib3JkZXJzIiwicGFydGl0aW9uIjoxLCJyZWNvcmRzIjozLCJzZWdfa2V5Ijoic2VnLzUwNTc1ZTY1NmM3MzdhODE4ODhmOTY5ZGE0YWJiMmI5Iiwic2VnX29mZiI6Mzg1LCJzZWdfbGVuIjoyMDV9LHsidG9waWMiOiJjbGlja3MiLCJwYXJ0aXRpb24iOjAsInJlY29yZHMiOjIsInNlZ19rZXkiOiJzZWcvNTA1NzVlNjU2YzczN2E4MTg4OGY5NjlkYTRhYmIyYjkiLCJzZWdfb2ZmIjo1OTAsInNlZ19sZW4iOjE1N31dLCJSYW5nZXMiOlt7InRvcGljIjoib3JkZXJzIiwicGFydGl0aW9uIjoxLCJiYXNlIjo5LCJjb3VudCI6NCwic2VnX2tleSI6InNlZy81MDU3NWU2NTZjNzM3YTgxODg4Zjk2OWRhNGFiYjJiOSIsInNlZ19vZmYiOjEzMiwic2VnX2xlbiI6MjUzfSx7InRvcGljIjoib3JkZXJzIiwicGFydGl0aW9uIjoxLCJiYXNlIjoxMywiY291bnQiOjMsInNlZ19rZXkiOiJzZWcvNTA1NzVlNjU2YzczN2E4MTg4OGY5NjlkYTRhYmIyYjkiLCJzZWdfb2ZmIjozODUsInNlZ19sZW4iOjIwNX0seyJ0b3BpYyI6ImNsaWNrcyIsInBhcnRpdGlvbiI6MCwiYmFzZSI6OSwiY291bnQiOjIsInNlZ19rZXkiOiJzZWcvNTA1NzVlNjU2YzczN2E4MTg4OGY5NjlkYTRhYmIyYjkiLCJzZWdfb2ZmIjo1OTAsInNlZ19sZW4iOjE1N31dfQ=="}
}
{ "pk": {"S": "G#seg/60676e757c838a91989fa6adb4bbc2c9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/70777e858c939aa1a8afb6bdc4cbd2d9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/80878e959ca3aab1b8bfc6cdd4dbe2e9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/90979ea5acb3bac1c8cfd6dde4ebf2f9"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
{ "pk": {"S": "G#seg/a0a7aeb5bcc3cad1d8dfe6edf4fb0209"}, "sk": {"S": "-"}, "blob": {"B": "eyJCYXRjaGVzIjpbeyJ0b3BpYyI6Im9yZGVycyIsInBhcnRp..."} }
```

| label | key suffix | batches | ranges | blob bytes |
|---|---|---|---|---|
| S1 | …646b7279 | 7 | 7 | 1787 |
| S2 | …747b8289 | 5 | 5 | 1279 |
| S3 | …848b9299 | 6 | 6 | 1534 |
| S4 | …949ba2a9 | 7 | 7 | 1788 |
| S5 | …a4abb2b9 | 3 | 3 | 778 |
| S6 | …b4bbc2c9 | 3 | 3 | 779 |
| S7 | …c4cbd2d9 | 7 | 7 | 1788 |
| S8 | …d4dbe2e9 | 6 | 6 | 1538 |
| S9 | …e4ebf2f9 | 5 | 5 | 1284 |
| S10 | …f4fb0209 | 7 | 7 | 1792 |

## 4. Timeline: how 10 commits build the table

Item count after each step (full Scan; setup, then S1 to S10): Setup 2, S1 7, S2 12, S3 16, S4 20, S5 23, S6 27, S7 30, S8 34, S9 38, S10 43. Each commit adds one `G#` blob and one run per distinct partition; topics never change after the setup because the partitions were created up front (a commit that names a partition beyond the topic's count would also `SET parts` in the same transaction).

### Setup

Two `CreateTopic` calls; no segment yet.

- NEW `TOPICS | clicks` parts = 2
- NEW `TOPICS | orders` parts = 3

### After S1

Segment key `seg/10171e252c333a41484f565d646b7279` (S1). First commit: every partition starts at offset 0; the two orders/0 batches get consecutive bases 0 and 3.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/0 | [0,3) | 3 | off 276 len 205 |
| 1 | orders/0 | [3,5) | 2 | off 481 len 157 |
| 2 | orders/1 | [0,4) | 4 | off 638 len 253 |
| 3 | clicks/0 | [0,1) | 1 | off 891 len 109 |
| 4 | clicks/0 | [1,2) | 1 | off 1000 len 109 |
| 5 | clicks/0 | [2,4) | 2 | off 1109 len 157 |
| 6 | clicks/1 | [0,5) | 5 | off 1266 len 301 |


- NEW `G#…646b7279` (S1): segment blob, 1787 bytes JSON
- NEW run `R#clicks#0 | 00000000000000000000`: 3 batch(es), 4 records, covers [0, 4)
- NEW run `R#clicks#1 | 00000000000000000000`: 1 batch(es), 5 records, covers [0, 5)
- NEW run `R#orders#0 | 00000000000000000000`: 2 batch(es), 5 records, covers [0, 5)
- NEW run `R#orders#1 | 00000000000000000000`: 1 batch(es), 4 records, covers [0, 4)

### After S2

Segment key `seg/20272e353c434a51585f666d747b8289` (S2). Orders/2 starts.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/0 | [5,7) | 2 | off 204 len 157 |
| 1 | orders/2 | [0,3) | 3 | off 361 len 205 |
| 2 | orders/2 | [3,6) | 3 | off 566 len 205 |
| 3 | clicks/0 | [4,5) | 1 | off 771 len 109 |
| 4 | clicks/1 | [5,7) | 2 | off 880 len 157 |


- NEW `G#…747b8289` (S2): segment blob, 1279 bytes JSON
- NEW run `R#clicks#0 | 00000000000000000004`: 1 batch(es), 1 records, covers [4, 5)
- NEW run `R#clicks#1 | 00000000000000000005`: 1 batch(es), 2 records, covers [5, 7)
- NEW run `R#orders#0 | 00000000000000000005`: 1 batch(es), 2 records, covers [5, 7)
- NEW run `R#orders#2 | 00000000000000000000`: 2 batch(es), 6 records, covers [0, 6)

### After S3

Segment key `seg/30373e454c535a61686f767d848b9299` (S3). Three batches for orders/1 in one request.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/1 | [4,5) | 1 | off 240 len 109 |
| 1 | orders/1 | [5,7) | 2 | off 349 len 157 |
| 2 | orders/1 | [7,9) | 2 | off 506 len 157 |
| 3 | orders/0 | [7,13) | 6 | off 663 len 349 |
| 4 | clicks/0 | [5,8) | 3 | off 1012 len 205 |
| 5 | clicks/0 | [8,9) | 1 | off 1217 len 109 |


- NEW `G#…848b9299` (S3): segment blob, 1534 bytes JSON
- NEW run `R#clicks#0 | 00000000000000000005`: 2 batch(es), 4 records, covers [5, 9)
- NEW run `R#orders#0 | 00000000000000000007`: 1 batch(es), 6 records, covers [7, 13)
- NEW run `R#orders#1 | 00000000000000000004`: 3 batch(es), 5 records, covers [4, 9)

### After S4

Segment key `seg/40474e555c636a71787f868d949ba2a9` (S4). Four small clicks/1 batches.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/0 | [13,14) | 1 | off 276 len 109 |
| 1 | orders/0 | [14,18) | 4 | off 385 len 253 |
| 2 | orders/2 | [6,8) | 2 | off 638 len 157 |
| 3 | clicks/1 | [7,8) | 1 | off 795 len 109 |
| 4 | clicks/1 | [8,9) | 1 | off 904 len 109 |
| 5 | clicks/1 | [9,10) | 1 | off 1013 len 109 |
| 6 | clicks/1 | [10,11) | 1 | off 1122 len 109 |


- NEW `G#…949ba2a9` (S4): segment blob, 1788 bytes JSON
- NEW run `R#clicks#1 | 00000000000000000007`: 4 batch(es), 4 records, covers [7, 11)
- NEW run `R#orders#0 | 00000000000000000013`: 2 batch(es), 5 records, covers [13, 18)
- NEW run `R#orders#2 | 00000000000000000006`: 1 batch(es), 2 records, covers [6, 8)

### After S5

Segment key `seg/50575e656c737a81888f969da4abb2b9` (S5). A partition's batches need not be adjacent in the request.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/1 | [9,13) | 4 | off 132 len 253 |
| 1 | orders/1 | [13,16) | 3 | off 385 len 205 |
| 2 | clicks/0 | [9,11) | 2 | off 590 len 157 |


- NEW `G#…a4abb2b9` (S5): segment blob, 778 bytes JSON
- NEW run `R#clicks#0 | 00000000000000000009`: 1 batch(es), 2 records, covers [9, 11)
- NEW run `R#orders#1 | 00000000000000000009`: 2 batch(es), 7 records, covers [9, 16)

### After S6

Segment key `seg/60676e757c838a91989fa6adb4bbc2c9` (S6). Orders/2, orders/0, clicks/1.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/2 | [8,10) | 2 | off 132 len 157 |
| 1 | orders/0 | [18,20) | 2 | off 289 len 157 |
| 2 | clicks/1 | [11,12) | 1 | off 446 len 109 |


- NEW `G#…b4bbc2c9` (S6): segment blob, 779 bytes JSON
- NEW run `R#clicks#1 | 00000000000000000011`: 1 batch(es), 1 records, covers [11, 12)
- NEW run `R#orders#0 | 00000000000000000018`: 1 batch(es), 2 records, covers [18, 20)
- NEW run `R#orders#2 | 00000000000000000008`: 1 batch(es), 2 records, covers [8, 10)

### After S7

Segment key `seg/70777e858c939aa1a8afb6bdc4cbd2d9` (S7). Six batches for orders/0 become ONE run item.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/0 | [20,21) | 1 | off 276 len 109 |
| 1 | orders/0 | [21,22) | 1 | off 385 len 109 |
| 2 | orders/0 | [22,23) | 1 | off 494 len 109 |
| 3 | orders/0 | [23,24) | 1 | off 603 len 109 |
| 4 | orders/0 | [24,25) | 1 | off 712 len 109 |
| 5 | orders/0 | [25,26) | 1 | off 821 len 109 |
| 6 | orders/1 | [16,18) | 2 | off 930 len 157 |


- NEW `G#…c4cbd2d9` (S7): segment blob, 1788 bytes JSON
- NEW run `R#orders#0 | 00000000000000000020`: 6 batch(es), 6 records, covers [20, 26)
- NEW run `R#orders#1 | 00000000000000000016`: 1 batch(es), 2 records, covers [16, 18)

### After S8

Segment key `seg/80878e959ca3aab1b8bfc6cdd4dbe2e9` (S8). Three partitions.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/2 | [10,11) | 1 | off 240 len 109 |
| 1 | orders/2 | [11,13) | 2 | off 349 len 157 |
| 2 | orders/2 | [13,16) | 3 | off 506 len 205 |
| 3 | clicks/0 | [11,13) | 2 | off 711 len 157 |
| 4 | clicks/0 | [13,15) | 2 | off 868 len 157 |
| 5 | clicks/1 | [12,15) | 3 | off 1025 len 205 |


- NEW `G#…d4dbe2e9` (S8): segment blob, 1538 bytes JSON
- NEW run `R#clicks#0 | 00000000000000000011`: 2 batch(es), 4 records, covers [11, 15)
- NEW run `R#clicks#1 | 00000000000000000012`: 1 batch(es), 3 records, covers [12, 15)
- NEW run `R#orders#2 | 00000000000000000010`: 3 batch(es), 6 records, covers [10, 16)

### After S9

Segment key `seg/90979ea5acb3bac1c8cfd6dde4ebf2f9` (S9). Mixed.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/1 | [18,20) | 2 | off 204 len 157 |
| 1 | orders/1 | [20,22) | 2 | off 361 len 157 |
| 2 | clicks/1 | [15,16) | 1 | off 518 len 109 |
| 3 | clicks/1 | [16,19) | 3 | off 627 len 205 |
| 4 | orders/2 | [16,21) | 5 | off 832 len 301 |


- NEW `G#…e4ebf2f9` (S9): segment blob, 1284 bytes JSON
- NEW run `R#clicks#1 | 00000000000000000015`: 2 batch(es), 4 records, covers [15, 19)
- NEW run `R#orders#1 | 00000000000000000018`: 2 batch(es), 4 records, covers [18, 22)
- NEW run `R#orders#2 | 00000000000000000016`: 1 batch(es), 5 records, covers [16, 21)

### After S10

Segment key `seg/a0a7aeb5bcc3cad1d8dfe6edf4fb0209` (S10). Last segment: five partitions touched.

| # | partition | assigned | records | seg bytes |
|---|---|---|---|---|
| 0 | orders/0 | [26,28) | 2 | off 276 len 157 |
| 1 | orders/0 | [28,30) | 2 | off 433 len 157 |
| 2 | orders/0 | [30,34) | 4 | off 590 len 253 |
| 3 | clicks/0 | [15,16) | 1 | off 843 len 109 |
| 4 | clicks/1 | [19,21) | 2 | off 952 len 157 |
| 5 | clicks/1 | [21,23) | 2 | off 1109 len 157 |
| 6 | orders/2 | [21,22) | 1 | off 1266 len 109 |


- NEW `G#…f4fb0209` (S10): segment blob, 1792 bytes JSON
- NEW run `R#clicks#0 | 00000000000000000015`: 1 batch(es), 1 records, covers [15, 16)
- NEW run `R#clicks#1 | 00000000000000000019`: 2 batch(es), 4 records, covers [19, 23)
- NEW run `R#orders#0 | 00000000000000000026`: 3 batch(es), 8 records, covers [26, 34)
- NEW run `R#orders#2 | 00000000000000000021`: 1 batch(es), 1 records, covers [21, 22)

Reading the timeline: a run item starts exactly where the previous run of that partition stopped (compare S1 and S2 on `orders/0`: 5 records, then the next run has sk `...05`). A partition untouched by a segment gets no item (S3 never touches `clicks/1`). There is no per-partition counter to update: the previous run **is** the counter.

## 5. The special cases

### 5.1 Several batches of one partition in one request

S1 sent two `orders/0` batches (3 and 2 records), one `orders/1`, three `clicks/0` and one `clicks/1`. Every batch gets its partition's end as base, in request order, the end growing by the batch's records: `orders/0` got [0,3) then [3,5); `clicks/0` got three consecutive bases. All batches of a partition land in **one** run item. S7 shows it at scale: six `orders/0` batches are one 6-batch run (section 3.2), and the transaction had 1 (`G#`) + 2 (`orders/0`, `orders/1`) = 3 items, whatever the number of batches.

### 5.2 Replay of an already committed segment key

Re-sending S7 (same key, same batches) answered `OK` and returned the stored answer verbatim (identical ranges: true). The Scan before and after is identical: 43 items, no attribute changed (true). The replay check is the first read, a `BatchGetItem` that includes `G#<key>`, so it wins even though `orders/0` and `orders/1` have moved on since S7 (S10 appended to `orders/0`): a replay never gets new offsets.

### 5.3 Conflict: same key, different request

Sending S1's key with only the first two of its seven batches answered `ErrConflict`; nothing is written (true). The stored request in `G#<S1>` is compared batch by batch with the new one.

### 5.4 Invalid requests

A request with no batches answered `ErrInvalid` and a batch with 0 records `ErrInvalid`. Neither writes anything, and a failed key stays reusable.

### 5.5 The compare-and-set: losing a race

Two control-plane replicas that read the same end both try to write a run at that base; the second finds the sort key taken. A real demonstration against the table: a stale writer believes the end of `orders/1` is 16 (it was, before S7) and sends the transaction a commit would send, a `Put G#<new key>` with `attribute_not_exists(pk)` and a `Put R#orders#1 / 00000000000000000016` with `attribute_not_exists(sk)`. ExtendDB's answer:

```
TransactionCanceledException: Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]
cancellation reasons: [None ConditionalCheckFailed]
```

The transaction is cancelled as a whole (the `G#` put, fine on its own, is not applied either): the Scan afterwards is identical, true. The real commit sees this as a lost race, reads the newest run of the partitions whose run item failed, recomputes the bases (here 16 would become the current end, 22) and retries, up to 20 times with jittered backoff, then answers 503.

### 5.6 Lookup

`Lookup` is a consistent descending `Query` (`Limit 1`) for the end, then a `Query` on `R#<topic>#<p>` with `sk BETWEEN <run holding From> AND <end-1>`; each run is expanded into per-batch ranges. Results on the final table:

`Lookup{orders/0, From: 5, MaxBytes: 400}` -> `OK`, `End: 34`:

| base | count | segment | byte off | byte len |
|---|---|---|---|---|
| 5 | 2 | S2 | 204 | 157 |
| 7 | 6 | S3 | 663 | 349 |

The scan stops when the running byte total reaches `MaxBytes`, and the batch that crosses it is still returned. It started at the run holding offset 5; an earlier batch of that run that ends before `From` is dropped.

`Lookup{clicks/1, From: 0, MaxBytes: 1 MiB}` -> `OK`, `End: 23`: 12 ranges, in offset order: [0,5) S1, [5,7) S2, [7,8) S4, [8,9) S4, [9,10) S4, [10,11) S4, [11,12) S6, [12,15) S8, [15,16) S9, [16,19) S9, [19,21) S10, [21,23) S10.

## 6. Inspect it yourself

With the backends from `scripts/it-up.sh` running (ExtendDB on `http://127.0.0.1:18080`; it is in-memory, so a restart empties it) the stock AWS CLI works with the seeded dummy credential. Nothing here touches AWS.

```sh
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY AWS_DEFAULT_REGION=us-east-1
EP=http://127.0.0.1:18080
aws dynamodb list-tables --endpoint-url $EP
aws dynamodb scan --table-name demo_cp --endpoint-url $EP --select COUNT            # 43 for this data set
aws dynamodb scan --table-name demo_cp --endpoint-url $EP --output json > scan.json
# the end of one partition: its newest run (descending, one item)
aws dynamodb query --table-name demo_cp --endpoint-url $EP --no-scan-index-forward --max-items 1 \
  --consistent-read --key-condition-expression "pk = :p" --expression-attribute-values '{":p":{"S":"R#orders#0"}}'
# decode a segment blob
jq -r '.Items[] | select(.pk.S|startswith("G#")) | .blob.B' scan.json | head -1 | base64 -d | jq .
```

The CLI hands back `B` attributes base64-encoded, which is how they are printed above. A real service would also page a Scan at 1 MB; the sample is small enough for one page, and the dump program used `Limit: 25` and followed `LastEvaluatedKey` to prove the paging.

To reproduce the dump: start ExtendDB, build a `dynamo.New(client, "demo_")` store on a `dynamodb.New(Options{BaseEndpoint: ...})` client as in `cp/dynamo/commit_ext_test.go`, call `CreateTables`, `CreateTopic` x2, then `CommitSegment` ten times (batches built with `format.Encode`, `Loc: ents[i].Ref(key)`, `key := format.SegmentKey(id)`) and scan.

## What was surprising

- There is nothing to fence and nothing to deduplicate, so the control plane keeps no per-node and no per-producer state: the whole table is topics, one blob per segment and one run per (commit, partition).
- The high watermark is gone, and so is the cost of keeping it in step with the runs. The price is a read: a commit does one `Query` per distinct partition (concurrently) instead of reading and updating a counter item. The compare-and-set moved into the run item's own key.
- The run item is both the data and the counter, so a lost race is visible only as a cancelled transaction; no partial state can exist.
- Every `G#` blob repeats the full `seg/<32 hex>` key once per batch and range, while `runs` stores each distinct key once.
