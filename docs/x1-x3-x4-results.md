# X1, X3, X4 results (AWS, ap-south-2)

Status: measured once, on 2026-10-20, from one c6i.xlarge (x86_64, Amazon Linux 2023) in ap-south-2 to a throwaway
on-demand DynamoDB table in the same region (default VPC, public subnet, SSM only). Tool: `cmd/ddbprobe` (Go, AWS SDK
already in go.mod, SDK retries disabled so throttling is visible). Raw JSON: `results/x1x3x4/*.jsonl` (not committed;
the `results/` directory is untracked in this repo). Context: `docs/delos-logdrive-ideas.md` section 7,
`docs/design-a-prototype.md` section 4, `docs/commit-path-redesign.md` section 10.

## 0. Caveats first

- **Short runs, one sample.** The docs ask for 10 minutes per point; the product owner capped the runs. X1 points ran
  60 s (not 2 min: the whole session had to finish before the SSO token expired), X3 45 s, X4 60-120 s. Each number
  is one run, from one instance, in one AZ, within about 30 minutes. No p99.9 is claimed: a 60 s run at 64/s has
  3,840 samples, so p99 is about the 38th worst and the max is a single event.
- Latency is measured from the moment the request is sent (open-loop pacing at the target rate, at most K in flight;
  when K is exhausted the dispatcher waits and `delayed_ticks` counts it, so a rate K cannot sustain shows as a lower
  achieved rate).
- Consumed capacity is the API's `ReturnConsumedCapacity=TOTAL`. Cross-check with CloudWatch on the main table:
  `ConsumedWriteCapacityUnits` sum 1,611,667 versus the tool's 1,581,662 (1.9% more in CloudWatch, from throttled and
  failed calls whose answer had no capacity and a 100-slot smoke test); reads 12,642 versus 12,600.
- A first X4 round on the first table had a tool bug (duplicate keys inside one BatchWriteItem call gave about 0.5-1%
  `ValidationException`). It was fixed (distinct keys per call) and X4 was re-run on a second, fresh table; both
  rounds are shown.
- The contention model is optimistic (section 2.3). Failed conditional PutItems are not charged in the tool's WRU
  column because the error carries no capacity; DynamoDB does charge a failed condition check, so the true contention
  WRU is higher than shown.
- Prices were not measured. Cost of this experiment is an estimate (section 6).

## 1. X1: PutItem on consecutive slots of one partition key (pk=LOG, sk=%020d)

Item: 4,096 byte payload plus keys, `attribute_not_exists(pk)`; each item costs 5 WRU
(the item is just over 4 KB), not 4. Single appender, K in flight on consecutive slots.

| K | target/s | achieved/s | p50 ms | p90 | p99 | max | CCF | errors (throttle etc.) | p99 1st/2nd half |
|--:|--:|--:|--:|--:|--:|--:|--:|--|--|
| 1 | 30 | 30.0 | 2.9 | 3.4 | 5.2 | 13.0 | 0 | 0 | 5.0 / 5.2 |
| 1 | 64 | 64.0 | 2.9 | 3.2 | 4.1 | 12.9 | 0 | 0 | 3.7 / 4.4 |
| 1 | 128 | 128.0 | 2.9 | 3.2 | 4.7 | 13.6 | 0 | 0 | 4.6 / 4.8 |
| 1 | 250 | 206.4 | 2.9 | 3.6 | 4.7 | 17.9 | 0 | other:InternalServerError=10, throttle=2603 | 4.4 / 5.0 |
| 4 | 30 | 30.0 | 3.6 | 4.2 | 6.1 | 12.5 | 0 | 0 | 6.2 / 6.1 |
| 4 | 64 | 64.0 | 3.6 | 4.2 | 6.4 | 12.9 | 0 | 0 | 6.1 / 6.5 |
| 4 | 128 | 128.0 | 3.6 | 4.1 | 5.9 | 33.6 | 0 | 0 | 5.9 / 5.8 |
| 4 | 250 | 203.3 | 3.5 | 3.9 | 5.7 | 13.6 | 0 | throttle=2801 | 5.5 / 5.8 |
| 16 | 30 | 30.0 | 3.6 | 4.0 | 5.7 | 9.8 | 0 | 0 | 5.7 / 5.6 |
| 16 | 64 | 64.0 | 3.6 | 4.2 | 5.7 | 12.3 | 0 | 0 | 5.7 / 5.8 |
| 16 | 128 | 128.0 | 3.5 | 3.9 | 5.6 | 14.4 | 0 | 0 | 5.5 / 5.7 |
| 16 | 250 | 203.3 | 3.6 | 4.0 | 5.6 | 19.3 | 0 | throttle=2800 | 5.6 / 5.5 |

Reading:

- **Latency is far below the assumption.** p50 2.9 ms (K=1) and 3.5-3.6 ms (K=4, 16), p99 4-6.4 ms, max 10-34 ms. The
  prototype doc assumed 5-8 ms (mild) and 15 ms (pessimistic). **The pessimistic 15 ms assumption is killed.**
- **K=1 is enough at 30, 64 and 128/s** (3 ms per Put gives a ceiling of about 340/s); K=4 and K=16 add nothing
  below 250/s and only add 0.7 ms of p50. So sticky routing with K=1 to 4 has large headroom for 30-128 entries/s.
- **No hot-key latency drift** inside 60 s: p99 in the first and second halves agree within 1 ms.
- **250/s fails for every K, with throttling.** All three K values stop at about 203/s (K=4, 16) or 206/s (K=1) and
  report `throttle` (ProvisionedThroughputExceeded) errors: 203 x 5 WRU = about 1,015 WRU/s, which is the
  1,000 WCU/s limit of one partition key. **The ideas doc's "about 250 entries/s before striping" assumed 4 WRU; with
  entries just over 4 KB the ceiling is about 200/s.** An entry of 4,096 bytes including keys or less would be 4 WRU
  and reach about 250/s (not measured). Either way the hot-key ceiling is real, close to 3x the 64/s needed, and not
  reachable with K alone.
- **ConditionalCheckFailed is 0** for the single appender, as expected.

## 2. X1 comparison: 3-item transaction, and contention

| mode | target/s | K | achieved/s | calls per entry | p50 | p90 | p99 | max | WRU per entry (successful calls) | CCF seen |
|--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| txn | 64 | 16 | 64.0 | 1.00 | 14.5 | 16.6 | 18.0 | 29.9 | 20.0 | 0 |
| txn | 128 | 16 | 128.0 | 1.00 | 14.6 | 16.5 | 18.5 | 42.7 | 20.0 | 0 |
| contend | 64 | 4 | 64.0 | 2.50 | 7.8 | 11.8 | 13.8 | 23.3 | 5.0 | 5751 |

### 2.1 PutItem versus a 3-item transaction

`TransactWriteItems` (2 x 2 KB `G#` puts plus a 3 KB LOG slot, all `attribute_not_exists`) at K=16: p50 14.5 ms, p99
18-18.5 ms, max 30-43 ms, 20 WRU per transaction, no throttling at 64 and 128/s. A single PutItem is **about 4x faster
at p50 (3.6 versus 14.5 ms) and 4x cheaper in WRU (5 versus 20)**. This matches design L's model between the mild
(8 + 0.7 x items = 10 ms) and pessimistic (15 + 1.5 x items = 19.5 ms) cases, and it confirms A's single-Put commit
path is the cheaper one. Even the transaction alone costs more than the whole A commit.

### 2.2 Cost of contention (3 writers racing, K=4 each, 64/s total)

2.50 PutItems per committed entry (5,751 ConditionalCheckFailed for 3,840 commits, 1.5 lost races per entry), commit
p50 7.8 ms, p99 13.8 ms (versus 3.6 / 5.7 ms for one appender at the same rate). Every lost race costs one extra
Put, so the contention tax is about 2.5x in PutItems and WRU and about 2x in p50. That is acceptable only as a
failover penalty, which is what the product owner decided; **with three active appenders it would waste about
60% of the write capacity**.

### 2.3 Why the contention number is a lower bound

The three appenders keep independent cursors and, after a loss, jump to just past the highest slot any appender has
committed. That "tail" is shared in process memory at no cost; a real replica must learn it with a consistent Query
(section 3: 2 RRU each), or by another loss. Real contention therefore costs more PutItems and some reads.

## 3. X3: consistent Query on pk=LOG

Table state: about 90,000 entries of 4 KB under pk=LOG from X1 (the tail is the highest slot).
`desc` is `Limit 1`, `ScanIndexForward=false`. `after` is `sk > tail-5` (returns 4 entries).

| query | lookups/s | window | queries/s | p50 | p90 | p99 | max | RRU per query | RRU per lookup |
|--|--:|--|--:|--:|--:|--:|--:|--:|--:|
| desc | 20 | none | 20.0 | 2.2 | 2.8 | 4.8 | 8.1 | 2.00 | 2.00 |
| after | 20 | none | 20.0 | 2.4 | 3.0 | 4.9 | 13.8 | 5.00 | 5.00 |
| desc | 200 | 50ms | 20.0 | 27.8 | 47.4 | 52.8 | 57.8 | 2.00 | 0.20 |
| after | 200 | 50ms | 20.0 | 27.8 | 47.5 | 52.9 | 58.8 | 5.00 | 0.50 |

- **A consistent Query is cheap in latency**: p50 2.2-2.4 ms, p99 4.8-4.9 ms, max 8-14 ms. The prototype doc assumed
  4 ms and 8 ms per query: **killed (faster)**, and its backlog replay estimate (1-2 s at 10-30 ms a page) is now
  pessimistic.
- **RRU depends on bytes returned, not on the number of calls**: a consistent read is 1 RRU per 4 KB, so one 4.1 KB
  entry is 2 RRU, and four entries are 5 RRU (about 1.25 RRU per entry). The ideas doc's "one Query per 50 ms per
  replica = 20 RRU/s" is only right for an empty result (not measured here). A replica that tails every committed
  entry pays about 1.25 RRU per 4 KB entry, so at 64 entries/s about 80 RRU/s per replica, which grows with replicas
  and entry size.
- **50 ms batching works as intended**: 200 lookups/s share 20 queries/s, so RRU per lookup falls 10x (0.2 for
  `desc`, 0.5 for `after`) and the query rate stays fixed at the window rate. The lookup latency becomes the wait
  plus the query: p50 28 ms, p99 53 ms (window plus 3 ms), which is the price of the batching.

## 4. X4: BatchWriteItem of 60-byte items (the 40k-partition index write)

25 items per call, 8 in flight, partitions drawn from 40,000 keys (distinct within a call in the fixed rounds).
Rows 1-2 are the first round (with the duplicate-key tool bug in about 0.5-1% of calls: the
`ValidationException`s), rows 3-5 the fixed round. Rows 1 and 3 are the **fresh-table** cases (row 3 a new table
that had received nothing before). Row 4 and 5 are on the first table after about 25 minutes of other traffic.

| table | target items/s | duration | achieved items/s | calls (25 items) p50 / p99 / max ms | errors (calls) | UnprocessedItems seen | WRU per item |
|--|--:|--:|--:|--|--|--:|--:|
| x1x3x4-probe | 6000 | 156 s | 3653 | 27.4 / 136.5 / 543 | other=1135, other:InternalServerError=153, other:ValidationException=200, throttle=5462 | 252248 | 1.190 |
| x1x3x4-probe | 3000 | 90 s | 2984 | 4.2 / 6.6 / 23 | other:ValidationException=59 | 0 | 1.000 |
| x1x3x4-probe2 | 6000 | 139 s | 4083 | 26.9 / 137.8 / 586 | other=828, other:InternalServerError=81, throttle=5889 | 200884 | 1.204 |
| x1x3x4-probe | 6000 | 60 s | 6000 | 4.2 / 7.9 / 22 | 0 | 0 | 1.000 |
| x1x3x4-probe | 3000 | 60 s | 2956 | 4.2 / 8.5 / 600 | other:InternalServerError=107 | 292 | 1.014 |


("Achieved items/s" divides by the elapsed time, which includes catch-up when K=8 was exhausted. `other` is an op that
still had UnprocessedItems after 3 retries; `throttle` is a whole-call throttling error.)

- **1 WRU per item is confirmed** (1.000 WRU/item whenever calls complete). A 60-byte item is not rounded up to
  anything larger. The values above 1.0 in the fresh rows are capacity consumed by calls that were later counted as
  failed.
- **On a fresh on-demand table 6,000 items/s is not sustained**: both fresh tables stopped at 3,650-4,080 items/s
  (about 4,000 WRU/s, which matches the documented initial 4,000 WCU of a new on-demand table), with p50 27 ms, p99
  137 ms, max 586 ms, 250,000 or 200,000 UnprocessedItems seen, about 5,500-5,900 throttled calls and about 80-150
  `InternalServerError`. The dispatcher stayed blocked (28,000 delayed ticks).
- **The same 6,000 items/s ran cleanly on the first table 25 minutes later**: 6,000 items/s, 0 errors, 0
  UnprocessedItems, p50 4.2 ms, p99 7.9 ms, max 22 ms. I cannot separate "time since creation" from "traffic it had
  already seen" (that table had taken a failed 6k attempt, 3k and 12 minutes of PutItems), so the sound statement is:
  **a cold table throttles at about 4k WRU/s and a table that has seen this load first handles it**. A 6k/s index
  therefore needs a pre-warm (or a prior ramp) on a new table, or a larger flush interval.
- **3,000 items/s is comfortable** (p50 4.2 ms, p99 6.6-8.5 ms; max 23 ms in one run, 600 ms in the other with 107
  `InternalServerError` and 292 UnprocessedItems that retried). It sits under the 4,000 WRU/s of a new table. This
  is also the stated F=10 s design point, and the 600 ms max says UnprocessedItems and retries must be handled.
- **40k-partition index write cost**: at 1 WRU per 60-byte item, 6,000 items/s is 6,000 WRU/s and 3,000 items/s is
  3,000 WRU/s; neither fits the commit-path hot key since they spread over many partitions. The WRU price follows the
  region price (not measured here).

## 5. Assumptions: confirmed or killed

| Assumption (where) | Verdict | Evidence |
|--|--|--|
| PutItem 5-8 ms mild, 15 ms pessimistic (design-a section 4) | **Pessimistic case killed; mild case is conservative.** | p50 2.9-3.6 ms, p99 4-6.4 ms, max under 35 ms, K=1..16 |
| K=4 is enough, K=16 adds nothing (design-a) | **Confirmed**, and K=1 is enough up to 128/s. | achieved = target at 30-128/s for all K |
| Sticky single appender is the main case | **Confirmed.** 0 conditional failures. | section 1 |
| Hot key about 250 entries/s before striping (ideas doc) | **Killed for >4 KB entries**: about 200/s (5 WRU). | throttle at about 203/s, 1,015 WRU/s |
| No latency drift on one hot pk | **Confirmed for 60 s**, not for 10 min. | first/second half p99 within 1 ms |
| One Put is cheaper and faster than L's transaction | **Confirmed**: 4x latency, 4x WRU. | 3.6 versus 14.5 ms; 5 versus 20 WRU |
| Contention returns without stickiness (design-a) | **Confirmed**: 2.5 Puts/entry, p50 2x, WRU 2.5x (lower bound). | section 2.2 |
| Consistent tail Query 4-8 ms | **Killed (faster)**: p50 2.2 ms, p99 4.8 ms. | section 3 |
| Tail poll = 20 RRU/s per replica | **Not confirmed**: cost is about 1.25 RRU per 4 KB entry read, so about 80 RRU/s per replica at 64/s. Empty polls not measured. | section 3 |
| 50 ms batching of lookups | **Confirmed**: 10x fewer RRU per lookup at 200/s; +28 ms p50 lookup latency. | section 3 |
| BatchWriteItem is 1 WRU per 60-byte item | **Confirmed.** | 1.000 WRU/item |
| 6k items/s index writes (40k partitions, F=1 s) | **Killed on a cold table** (about 4k/s, throttling, p99 137 ms); **fine on a warmed table** (0 errors). | section 4 |
| 3k items/s (F=10 s) | **Confirmed**, with UnprocessedItems handling (292 seen once). | section 4 |
| HoleAfter 250 ms versus Put tail | **Consistent**: observed max 34 ms (K up to 16), 7x margin. A one-sample result; no p99.9 claim. | section 1 |

## 6. Cost and teardown

Estimated cost (not read from a bill): about 2.7M WRU in total (about 2.0M on the first table including the rerun, 0.68M on the
second, by CloudWatch and the tool totals) at an assumed on-demand price of roughly 1.25-1.5 USD per million, so
about 3.5-4 USD; reads negligible (12.6k RRU); the c6i.xlarge ran about 40 minutes (under 0.2 USD); S3 and SSM are
negligible. The total is under the 10 USD cap. At the end the instance, both tables, the S3 bucket, the IAM role and
instance profile, and the security group were deleted and verified gone (describe calls return NotFound).

## 7. Reproduce

`go build ./cmd/ddbprobe` (`GOOS=linux GOARCH=amd64` for the instance); `ddbprobe -table T -mode put -rate 64 -k 4
-dur 60s` (modes: put, txn, contend, query, batch; see `-h`). It prints one JSON line per run. Run it from an
instance in the table's region.
