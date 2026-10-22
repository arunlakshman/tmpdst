# The Manifester onboarding book

This book describes the repository. The chapters describe what `manifester` is, how `manifester` works, and why the code has its current form. The chapters also give the steps for your first change. The chapters use the code and docs on the `toys3` branch. Each chapter cites the files the chapter describes.

Manifester is a Kafka-compatible broker with no local disk. Record batches live in S3 segment objects. A separate DynamoDB-backed control plane assigns offsets. The broker is a single-threaded deterministic `Step(Event) → []Action` state machine. The broker runs unchanged in production and in a seeded simulator.

---

## How to use this book

The list below describes use of this book.

- Read the chapters in order on your first read. Each chapter uses facts from the chapters before it. Each chapter ends with key takeaways, checkpoint questions, a Try it exercise, and further reading. The checkpoint answers are hidden. Answer the questions before you read the answers.
- Use the book as a reference after your first read. Chapter 2 has an "If you want X, go to Y" table. Chapter 12 has "Change X then touch Y and test Z" recipes.
- If a chapter and the code disagree, follow the code and `docs/decisions.md`. Fix the chapter in the same PR.

### Choose your reading path

The table below lists reading paths by the time you have.

| Time you have | Read | You will be able to |
|---|---|---|
| 1 hour | Ch 1, the map and binaries sections of Ch 2, and the cookbook section of Ch 12 | Explain the system, find anything, build and run it locally |
| 1 day | Ch 1-4, Ch 7, Ch 12 | Trace a Produce from TCP to ack, run the local stack, read broker code |
| 1 week | All 12 chapters and the Try-it labs | Own a change end to end, across core, control plane, simulation test, and all quality gates |

---

## Contents

The table below lists the 12 chapters in order.

| # | Chapter | Lines | Core question the chapter answers |
|---|---|---|---|
| 1 | [The Big Picture](01-the-big-picture.md) | 313 | What is this, why does it exist, what is it _not_? |
| 2 | [Map of the Repository](02-map-of-the-repository.md) | 336 | Where does everything live, and which binary does what? |
| 3 | [The Deterministic Core](03-the-deterministic-core.md) | 358 | What are Events and Actions, and what may the core never do? |
| 4 | [Broker Data Plane](04-broker-data-plane.md) | 416 | How does Produce get sealed, stored, committed, acked, and Fetched back? |
| 5 | [Wire and Storage Formats](05-wire-and-storage-formats.md) | 538 | What bytes go on the wire and into S3? |
| 6 | [Control Plane](06-control-plane.md) | 397 | Who assigns offsets, and how does failover work without double-assigning? |
| 7 | [Production Runtime](07-production-runtime.md) | 312 | How do Actions become real goroutines, S3 calls, and TCP writes? |
| 8 | [Deterministic Simulation](08-deterministic-simulation.md) | 286 | How does one seed replay an entire cluster, crashes included? |
| 9 | [Verification and Quality Gates](09-verification-and-quality-gates.md) | 263 | How do you _know_ it is correct, with specs C1-C4, detcheck, and CRAP? |
| 10 | [Deployment, Benchmarking, Observability](10-deployment-benchmarking-observability.md) | 265 | How does it run on AWS, and how fast is it? |
| 11 | [Decisions, History, and Roadmap](11-decisions-history-and-roadmap.md) | 305 | How did we get here, what was superseded, what is next? |
| 12 | [Newcomer Playbook](12-newcomer-playbook.md) | 558 | What do you do in your first week, and how do you land a first PR? |

The 12 chapters total 4,347 lines.

## Five ideas in every chapter

The list below names five ideas that appear in every chapter.

- Logic does no I/O. `core/` and `broker/` only transform `Step(Event) → []Action`. Executors in `sys/prod` and `sim/` do the I/O. See Ch 3, 7, and 8.
- Ack only after the data is durable and committed. The order is S3 PUT, control plane commit, and ack. Do not ack before the commit. See Ch 4 and 6.
- The control plane owns offsets. Brokers are stateless and interchangeable. The newest committed "run" defines a partition's end. See Ch 6.
- One seed produces the same history on each run. Simulation uses one random number generator, one queue, and no wall clock. Each failure replays with the same history. See Ch 8.
- Specs define correctness. `veritas/` holds executable specs C1-C4. Mutants must fail. `detcheck` and the CRAP gate check the rest. See Ch 9.

## After the book

The list below names the next steps after you finish the book.

- Skim [`../decisions.md`](../decisions.md). Read at least decisions I, 21, 35, 41-45, and 48-53. Chapter 11 explains the decisions in the list.
- Run the half-day lab at the end of [Chapter 12](12-newcomer-playbook.md).
- Pick a "safe first area" from Chapter 12 and ship a small change.
