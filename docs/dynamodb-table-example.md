# What the DynamoDB control-plane table looks like

Note: the DynamoDB-transaction control plane that this page used to show (`G#`, `R#` and `TOPICS` items written by `cp/dynamo`) was removed (decision 54); the log-based control plane is the only one. Its worked ten-commit scan is in git history.

The control plane (`-store dynamolog`, `cp/logcp`, decisions 48-53) uses one table, `<table-prefix>cp`, with string keys `pk` (hash) and `sk` (range) and on-demand billing (`logcp.CreateTable`, or Terraform). Sample rows, decoded, from a real run: `docs/dynamodb-log-store-sample-rows.md`. Design: `docs/log-cp-service.md`.

| pk | sk | attributes | what it is |
|---|---|---|---|
| `LOG` | slot, 20 digits | `d` B (entry: version byte, rank, writer, append id, ts, count, commands), `v`, `w` writer, `id`, `ts`, `n` commands, `ttl` | one item per log slot, written with `attribute_not_exists(pk)`: the slot is the fence. Zero commands is a no-op (heartbeat, hole fill). Trimmed slots are overwritten with a tombstone stub. `ttl` (epoch s, `-log-ttl` 7 d) lets the table's TTL expire old slots. |
| `CKPT` | `-` | `ck_slot` N, `ck_key` S, `ck_size` N, `ck_sum` S (sha-256), `ck_ts` N (ms) | forward-only conditional put; the object is `cp/ckpt/<slot>` in the checkpoint bucket. |

There are no `G#` idempotency items (the idempotency window lives in the applier and is a function of the slot number), no per-partition run rows and no topic rows: topics and offsets live in the log and in each node's Pebble state, and are rebuilt from the checkpoint plus the log after it.

So after N acked commits a scan shows about N/(commands per slot) `LOG` items plus heartbeats (one a second when idle) and one `CKPT`. Checkpoint objects (`cp/ckpt/<slot>`) are in the checkpoint bucket, not in the table.
