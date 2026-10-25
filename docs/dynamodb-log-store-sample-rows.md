# DynamoDB rows of the log-based control plane (sample)

This is every item type that `cp/logcp` (store kind `dynamolog`) keeps in its one DynamoDB table, taken from a real run.
The run: one node over fakedb and an in-memory object store, topic `orders` with 4 partitions, 10 segments from 3 brokers with 1 to 5 batches each, then a checkpoint.
`LOG` is the source of truth (one item per slot); `CKPT` points at a snapshot object (outside DynamoDB). There are no other items: the `R#` index, `IDX` and `TOPICS` rows of the removed transaction-based store are not written (decision 54).
Binary attributes (`d`) are decoded below; sizes are DynamoDB item sizes (names plus values). Regenerate: `DUMP_ROWS=1 go test ./cp/logcp -run TestDumpSampleRows -v`.

### Log slots (pk=LOG)

**pk=`LOG` sk=`00000000000000000000`** (127 bytes): w=n0 id=n0/n0-115524.712764000/1 ts=1792909524713 n=1 v=1 ttl=1793514324 d=<48 bytes>
- d: version byte 1, rank 0, writer "n0"
- cmd CreateTopic topic="orders" parts=4

**pk=`LOG` sk=`00000000000000000001`** (183 bytes): w=n0 id=n0/n0-115524.712764000/2 ts=1792909524713 n=1 v=1 ttl=1793514324 d=<104 bytes>
- d: version byte 1, rank 0, writer "n0"
- cmd Commit segment="seg/broker-1/000001.seg": [orders/0 rec=10 off=0 len=2048]

**pk=`LOG` sk=`00000000000000000002`** (199 bytes): w=n0 id=n0/n0-115524.712764000/3 ts=1792909524714 n=1 v=1 ttl=1793514324 d=<120 bytes>
- d: version byte 1, rank 0, writer "n0"
- cmd Commit segment="seg/broker-2/000002.seg": [orders/0 rec=11 off=0 len=2148] [orders/1 rec=14 off=2148 len=2148] [orders/0 rec=17 off=4296 len=2148]
... (7 more LOG rows)

**pk=`LOG` sk=`00000000000000000010`** (185 bytes): w=n0 id=n0/n0-115524.712764000/11 ts=1792909524715 n=1 v=1 ttl=1793514324 d=<105 bytes>
- d: version byte 1, rank 0, writer "n0"
- cmd Commit segment="seg/broker-1/000010.seg": [orders/2 rec=19 off=0 len=2948]

### Pointer and bookkeeping rows

**pk=`CKPT` sk=`-`** (141 bytes): ck_slot=11 ck_key=ckpt/00000000000000000011 ck_size=1964 ck_sum=f6cc93f90e501f1904acc4aea4468bcc2d2788fff1d2bc993cfd57fe0daa9f8e ck_ts=1792909524715

### Item sizes (bytes, DynamoDB accounting: attribute names + values)

| item type | count | min | max |
|---|---|---|---|
| CKPT | 1 | 141 | 141 |
| LOG | 11 | 127 | 217 |
