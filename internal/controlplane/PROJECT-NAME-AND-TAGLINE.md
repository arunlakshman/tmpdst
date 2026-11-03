# Control plane: name ideas, tagline and description

Status: DRAFT for the maintainer to choose from. Written 2026-11-03.
Nothing here has been checked for name availability, trademarks or domains.

## Name ideas

Short, simple English words. Pick one, then check the GitHub org, the package
registries and the domain.

| Name | Why it fits |
|---|---|
| Keel | The steady part at the bottom that keeps a ship on course |
| Lodestar | A fixed point that everything else follows |
| Fenceline | We fence old writers out with an epoch |
| Tailkeeper | One writer owns the tail of the log |
| Cairn | A stack of stones that marks the way: a log of checkpoints |
| Anchorlog | A log that holds the cluster state in place |
| Steward | Looks after the metadata for everyone else |
| Plumbline | A simple tool that tells you what is true and straight |

## Tagline

> One consistent key-value API on any cloud store (DynamoDB, Postgres, MySQL,
> Spanner or local memory), with writes batched to cost a fraction of the usual.

Shorter options:

- One consistent API. Any cloud store. A fraction of the write cost.
- A consistent key-value log that runs on the store you already have.

## Description

Short (one paragraph):

> A small control plane that keeps cluster metadata in an ordered log. One
> writer at a time is chosen by an epoch, old writers are fenced out, and
> checkpoints let a node restart fast. The log sits behind a small interface,
> so it can run on DynamoDB, a SQL database (Postgres, MySQL, Spanner) or local
> memory. A deterministic simulator tests it under crashes, stalls and lost
> responses.

Long (for a README):

> The control plane gives you a consistent, transactional key-value API without
> tying you to one cloud. Many small commands are packed into one log write, so
> the number of billed writes stays low. Reads are served from memory on any
> node after one shared check of the log tail. The log needs only three things
> from the store: put-if-absent on a slot, an ordered consistent range read,
> and a conditional update of one item. Most stores have all three.

## Cost note (back of the envelope, not measured)

Assumptions: 200-byte commands, 50 commands per log slot, DynamoDB on-demand
price of about $0.625 per million write units (from memory, not checked).

- A raw DynamoDB transactional write bills at least 2 write units (1 KB minimum,
  doubled), and 4 with a condition-check item.
- One batched slot of 10 KB bills about 10 write units, so about 0.2 per command.
- That is roughly 10x fewer write units for small commands under steady load.
- Reads come from memory after one shared tail check, so they cost far less.
- Two small nodes cost about $60 a month, so the break-even is near 20 writes
  per second. Below that, plain DynamoDB is cheaper.

## Caveats

- The cost claim is for stores that bill per request, such as DynamoDB. On SQL
  stores you pay for instances, so do not promise a cost saving there.
- "Runs on any store" is a goal. The DynamoDB, in-memory and PostgreSQL logs
  exist today. Other SQL backends are not built.
- Commands such as `CommitSegment` assign offsets when the log entry is applied.
  Plain get and put cannot express that. A generic release would expose a small
  state-machine API (apply a command, consistent read, compare-and-set) and put
  the Kafka commands in a layer above it.
