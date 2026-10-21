# DynamoDB rows of the log-based control plane (sample)

This is every item type that `cp/logcp` (store kind `dynamolog`) keeps in its one DynamoDB table, taken from a real run.
The run: one node over fakedb and an in-memory object store, topic `orders` with 4 partitions, 10 segments from 3 brokers with 1 to 5 batches each, then an index flush and a checkpoint.
`LOG` is the source of truth (one item per slot); `R#` rows are the read index; `CKPT` points at a snapshot object (outside DynamoDB); `IDX` is the indexer watermark; `TOPICS` holds partition counts.
Binary attributes (`d`, `runs`) are decoded below; sizes are DynamoDB item sizes (names plus values). Regenerate: `DUMP_ROWS=1 go test ./cp/logcp -run TestDumpSampleRows -v`.

### Log slots (pk=LOG)

**pk=`LOG` sk=`00000000000000000000`** (127 bytes): w=n0 id=n0/n0-063306.009909000/1 ts=1792544586010 n=1 v=1 ttl=1793149386 d=<48 bytes>
- d: version byte 1, rank 0, writer "n0"
- cmd CreateTopic topic="orders" parts=4

**pk=`LOG` sk=`00000000000000000001`** (116 bytes): w=n0 id=n0/n0-063306.009909000/2 ts=1792544586010 n=0 v=1 ttl=1793149386 d=<37 bytes>
- d: version byte 1, rank 0, writer "n0"

**pk=`LOG` sk=`00000000000000000002`** (183 bytes): w=n0 id=n0/n0-063306.009909000/3 ts=1792544586010 n=1 v=1 ttl=1793149386 d=<104 bytes>
- d: version byte 1, rank 0, writer "n0"
- cmd Commit segment="seg/broker-1/000001.seg": [orders/0 rec=10 off=0 len=2048]
... (8 more LOG rows)

**pk=`LOG` sk=`00000000000000000011`** (185 bytes): w=n0 id=n0/n0-063306.009909000/12 ts=1792544586012 n=1 v=1 ttl=1793149386 d=<105 bytes>
- d: version byte 1, rank 0, writer "n0"
- cmd Commit segment="seg/broker-1/000010.seg": [orders/2 rec=19 off=0 len=2948]

### Index rows (pk=R#<topic>#<partition>, sk=base offset)

**pk=`R#orders#0` sk=`00000000000000000000`** (80 bytes): base=0 cnt=10 runs=<32 bytes>
- format byte 1, segment keys ["seg/broker-1/000001.seg"]
- offsets [0,10) -> seg/broker-1/000001.seg off=0 len=2048

**pk=`R#orders#0` sk=`00000000000000000010`** (87 bytes): base=10 cnt=28 runs=<38 bytes>
- format byte 1, segment keys ["seg/broker-2/000002.seg"]
- offsets [10,21) -> seg/broker-2/000002.seg off=0 len=2148
- offsets [21,38) -> seg/broker-2/000002.seg off=4296 len=2148
... (4 more R#orders#0 rows)

**pk=`R#orders#1` sk=`00000000000000000000`** (81 bytes): base=0 cnt=14 runs=<33 bytes>
- format byte 1, segment keys ["seg/broker-2/000002.seg"]
- offsets [0,14) -> seg/broker-2/000002.seg off=2148 len=2148

**pk=`R#orders#1` sk=`00000000000000000014`** (99 bytes): base=14 cnt=70 runs=<50 bytes>
- format byte 1, segment keys ["seg/broker-1/000004.seg"]
- offsets [14,27) -> seg/broker-1/000004.seg off=0 len=2348
- offsets [27,43) -> seg/broker-1/000004.seg off=2348 len=2348
- offsets [43,62) -> seg/broker-1/000004.seg off=4696 len=2348
- offsets [62,84) -> seg/broker-1/000004.seg off=7044 len=2348
... (2 more R#orders#1 rows)

**pk=`R#orders#2` sk=`00000000000000000000`** (80 bytes): base=0 cnt=12 runs=<32 bytes>
- format byte 1, segment keys ["seg/broker-3/000003.seg"]
- offsets [0,12) -> seg/broker-3/000003.seg off=0 len=2248
... (4 more R#orders#2 rows)

**pk=`R#orders#3` sk=`00000000000000000000`** (81 bytes): base=0 cnt=15 runs=<33 bytes>
- format byte 1, segment keys ["seg/broker-3/000003.seg"]
- offsets [0,15) -> seg/broker-3/000003.seg off=2248 len=2248
... (3 more R#orders#3 rows)

### Pointer and bookkeeping rows

**pk=`TOPICS` sk=`orders`** (22 bytes): parts=4

**pk=`CKPT` sk=`-`** (141 bytes): ck_slot=12 ck_key=ckpt/00000000000000000012 ck_size=1964 ck_sum=0af70365aedc14b5c57d931b070b2bda6c3e8ecee7c538401c1705ea1a656821 ck_ts=1792544586012

**pk=`IDX` sk=`-`** (29 bytes): ix_slot=12 ix_ts=1792544586012

### Item sizes (bytes, DynamoDB accounting: attribute names + values)

| item type | count | min | max |
|---|---|---|---|
| CKPT | 1 | 141 | 141 |
| IDX | 1 | 29 | 29 |
| LOG | 12 | 116 | 219 |
| R# | 19 | 80 | 99 |
| TOPICS | 1 | 22 | 22 |
