# SQL log backend (PostgreSQL)

Status: built and tested for PostgreSQL 17.11. Other SQL databases are not built.

The control plane keeps its log in a store. Until now the stores were DynamoDB
(`-store dynamolog`) and memory (for tests and demos). This document covers the
third store, `-store postgres`. The code is in `internal/controlplane/executor/sqllog.go`.

## 1. The contract a backend must meet

The log is the small interface `executor.Log` in `internal/controlplane/executor/log.go`.
It has five methods: `Append`, `Read`, `CheckTail`, `TrimThrough` and `ClaimEpoch`.

A backend needs three things from its database:

1. **Put-if-absent on a slot.** Two writers can try the same slot. Exactly one put
   stores the entry. The other put gets the entry that is already there.
2. **An ordered, consistent range read.** `Read` returns the entries from a slot on,
   in slot order. It must never return a stale result.
3. **A compare-and-swap of one row.** This is used for the writer epoch.

The floor adds one more need. A put must be checked against the floor in the same
transaction as the insert. Section 4 explains this.

One shared test suite runs on every backend. It is the `logKinds` list in
`internal/controlplane/executor/fence_test.go`. A new backend is added to that list.

## 2. The schema

The two tables use a name prefix. The default prefix is `mds_`.

```
<prefix>log   slot  BIGINT PRIMARY KEY
              data  BYTEA NOT NULL          one row per slot

<prefix>meta  name  TEXT PRIMARY KEY
              num   BIGINT NOT NULL
              txt   TEXT NOT NULL DEFAULT ''
              two rows:
                'epoch'  num = the writer epoch, txt = the owner node
                'floor'  num = the floor (-1 means no floor yet)
```

`CreateSchema` makes both tables and the two meta rows. It can run many times.
It uses `IF NOT EXISTS` and `ON CONFLICT DO NOTHING`, so a second run does nothing.

## 3. Epoch and floor in SQL

**The epoch.** Only one node writes at a time. The writer epoch is a number in the
`epoch` row. A standby that wants to write claims the next epoch with one statement:

```sql
UPDATE mds_meta SET num = $1::bigint + 1, txt = $2
 WHERE name = 'epoch' AND num = $1::bigint
 RETURNING num, txt
```

`$1` is the epoch the node read before. If another node already changed the row,
the `WHERE` part matches no row and the claim is lost. Two claims with the same
`expect` cannot both win. The second one waits for the first, then sees the new value.

**The floor.** The floor is the highest slot that a trim may have deleted. A put at
or below the floor is not a commit. An append does this in one transaction:

```sql
-- 1. lock the floor row so that a trim cannot raise it during this transaction
SELECT num FROM mds_meta WHERE name = 'floor' FOR SHARE
-- 2. if the slot is at or below the floor, stop (not a commit)
-- 3. otherwise insert; one row affected means the entry is stored
INSERT INTO mds_log (slot, data) VALUES ($1, $2) ON CONFLICT (slot) DO NOTHING
```

A trim raises the floor with an `UPDATE`. That `UPDATE` needs an exclusive lock on
the same row. So it waits for every append that holds the shared lock. After the
floor is raised and committed, the trim deletes the old rows in batches of 1000.

Because of the lock, a put cannot land at or below the floor. No stray row is left.

## 4. Differences from the DynamoDB log

| Topic | DynamoDB log (`-store dynamolog`) | SQL log (`-store postgres`) |
|---|---|---|
| Floor check on a put | Read the floor after the put, and refuse if it is too high | Read the floor with a shared row lock, in the same transaction as the insert |
| Stray row | Can stay in a slot after a refused put. `Read` skips it | Cannot happen |
| Read | Scan, then read the floor (two calls) | One statement. The floor and the rows come from one snapshot |
| `CheckTail` | Highest slot plus one | Highest slot plus one, but never below floor plus one |
| Trim | Raise the floor, then delete rows | Same order. Deletes run in batches of 1000 |
| Epoch claim | Conditional update of one item | One `UPDATE ... WHERE num = expect` |
| Cost model | You pay per request | You pay for the database instance. Per-request pricing does not apply |
| Consistency needed | Consistent reads | Reads must go to the primary (read committed on the primary) |

## 5. Configuration

Set the connection string in an environment variable. A DSN on the command line shows
in `ps`, so the environment is the better choice.

```sh
export MDS_POSTGRES_DSN='postgres://mds_user@db.example.internal:5432/mds'

mdscontrolplane \
  -store postgres \
  -postgres-prefix mds_ \
  -create-tables \
  -checkpoint-bucket my-checkpoints \
  -checkpoint-prefix cp/ \
  -node-name cp-0 \
  -rank 0
```

Flags for `-store postgres`:

- `-postgres-dsn`: the connection string. Empty means use `MDS_POSTGRES_DSN`.
  The value is never logged or written to the run manifest. Only host, port and
  database name are logged.
- `-postgres-prefix`: table name prefix (default `mds_`). The tables are `<prefix>log`
  and `<prefix>meta`.
- `-create-tables`: create the tables if they are missing (for development).
- The checkpoint and heartbeat flags, and the rank and takeover flags, are the same
  as for `-store dynamolog`. Checkpoints still go to S3 (`-checkpoint-bucket`), or
  they are off when that flag is empty.

All control plane nodes use the same database and the same prefix. Only one node
writes at a time. The epoch chooses that node.

**Do not point the control plane at a read replica or a lagging follower.**
Reads must go to the primary.

## 6. Running the tests

The SQL tests need a real PostgreSQL. They need Docker.

```sh
make it-postgres
```

This starts `postgres:17.11-alpine` in Docker (script `ops/scripts/postgres-test.sh`),
runs the tests, and removes the container. Set `POSTGRES_KEEP=1` to keep the container
after the run. Pass extra `go test` arguments with `PG_TEST_ARGS`.

Without `MDS_TEST_POSTGRES_DSN`, the Postgres test cases skip. The rest of the test
suite still runs.

Docker cannot be installed with mise. Install Docker first.

`make it-postgres` is not part of `make build`.

What the tests cover:

- The shared suite (`logKinds`) runs on the SQL log, the same as the other logs.
- Six SQL-only tests are in `internal/controlplane/executor/sqllog_test.go`.
- `TestPostgresThreeNodesTakeoverWithTrim` in `cmd/mdscontrolplane` runs three
  nodes on one Postgres. It kills the writer while checkpoints and trims run every
  100 ms. The test checks that the new writer has epoch 2 or more, that the floor
  is above 100, and that all about 258 acknowledged commits are present after the
  takeover.

## 7. Porting to another database (not built)

Only the statements in `sqlStatements` and the lock clause change. The rest of
`SQLLog` stays the same.

- **MySQL.** Use `FOR SHARE` or `LOCK IN SHARE MODE` for the floor lock. Use
  `INSERT IGNORE` or `ON DUPLICATE KEY` for the put-if-absent. `GREATEST` exists.
- **Spanner or CockroachDB.** Transactions are serializable. Read the floor row
  inside a read-write transaction, then insert. The epoch claim is also a read-write
  transaction. You may not need a lock clause at all, but this is not tested.

After a port, run `make it-postgres`-style tests on the new database. The shared
suite must pass.

## 8. Limits

- Throughput and cost depend on the database and the instance size you run.
- The DynamoDB per-request cost claim does not carry over. You pay for the instance.
- Only PostgreSQL is built and tested.

## 9. Not tested yet

- Failover of the database itself (for example, a primary switch).
- Connection pool exhaustion under heavy load.
- PgBouncer in transaction mode. It should work, but it is not tested.
