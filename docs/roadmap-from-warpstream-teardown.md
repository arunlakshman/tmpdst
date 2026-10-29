# Roadmap from the WarpStream teardown, and open-source readiness

Date: 2026-10-21. Branch toys3. Read-only analysis of `docs/arun-warpstream-agent-reverse-engineering.md` (the
"teardown", untracked, the product owner's own document, NOT committed and not copied here; section refs only) against
our code. Our numbers: w4 (`docs/w4-log-store-results.md`, one sample per stage, 1 to 4 minutes, indicative),
`docs/surprises-audit.md` (S-ids; "computed" unless marked), code read on this branch. Effort is my estimate in
engineer-days (ed), one engineer, including sim coverage and review; not a commitment.

## 0. Read this first: the teardown is a legal problem, not just a document

- The teardown's method (sec 0, App E, App F) is extraction of protobuf descriptors, symbol tables and strings from the
  vendor's binary, plus running it. Its own legal note says "no disassembly", but recovering message schemas, the
  object-key scheme and the file layout from the binary is reverse engineering in the ordinary sense.
- Our earlier public-sources study (`warpstream-architecture-comparison.md` sec 0) already says: licence text not read,
  "treat reverse engineering as presumptively prohibited". Nobody has read the agent licence or the Confluent/IBM terms
  that now apply (the teardown itself notes the image carries IBM notices).
- Risk, stated plainly: it may breach the licence terms or ToS, and publishing derived material, or shipping code
  visibly modelled on it, could create legal exposure (contract, trade secret, and a credibility problem with the
  community). I am not a lawyer. **Recommendation: get legal advice before anything derived from it is published, keep
  the document and anything copied from it out of the OSS repo and out of git history, and implement our roadmap
  clean-room**: from public docs, the Kafka protocol spec, AWS guidance and our own measurements. For each item in
  section 2 I note whether a public or independent basis exists.
- Hygiene now: add `docs/arun-*` to `.git/info/exclude`; never `git add -A` (untracked `results/` is also at risk).

## 1. Side by side

Status: have / partial / missing / different (deliberately). "T:" = teardown section. Our evidence is code or decision #.

| Area | WarpStream agent per teardown | Ours | Status, evidence |
|---|---|---|---|
| Write path | Cross-partition batcher into one file; PUT to bucket, then commit to CP, then ack; 250 ms timer, ~1 MiB compressed default (T:8) | Same shape: seal at 16 MiB or 200 ms window, PUT, `CommitSegment`, ack after slot applied (dec 35, `internal/broker/config.go`: 200 ms, 16 MiB, 4096 partitions, 256 batches) | have. w4 1M: 99 partitions and 15.3 MiB per segment, 23 of 32 seals/s by size |
| Commit protocol | `PutFile` carries per-batch shape, CP assigns offsets at apply, returns them; file IDs pre-allocated from CP (T:8, E.5) | CP assigns offsets at apply, contiguous runs (dec 42, 48); key is client-chosen `seg/<random>` (dec 43), idempotent per key | have, different on ID allocation. No CP round trip before PUT, good |
| Metadata / control plane | Hosted sharded RSM per virtual cluster, 125-RPC API, journal plus snapshots, PITR (T:9, 10) | Own CP: DynamoDB log slots (if-absent), Pebble applier, one appender, hot standby, S3 checkpoint (dec 48-53, `cp/logcp`) | different. w4: append p99 8 ms, 20 to 32 slots/s, CP 0.2 of 4 cores. No PITR, no sharding |
| CP failover | Leader tracking, cluster migration (T:9.1, F.5) | Standby tails log, slot is the fence (dec 51), `-takeover-after 5s` | have. w4 kill at 500k: ack gap about 8 s, 0 errors. Not tested at 1M |
| File format | Custom segment: header, pages, footer with per-batch arrays, CRC per page; footer cache (F.2) | Batches stored verbatim plus index; Fetch patches baseOffset and leader epoch, CRC stays valid (dec 30, 43, 45) | have (simpler). No page checksums beyond the batch CRC; no scrub/inspect tool |
| Object keys | Sharded prefixes by file id (F.1) | Flat `seg/<128-bit random>` | partial. Fine at 32 PUT/s; S3 prefix limits matter at read-path scale (audit S6). AWS guidance is the independent basis |
| File cache / peer routing | RAM chunk cache, footer cache, AZ-aware peer ring, extra replicas (T:12) | None. Lookup per partition per Fetch, one ranged GET per batch (`internal/broker/fetch.go`), Fetch ignores MinBytes/MaxWait (dec 29) | missing. Audit S3/S6: 20.6k Lookups/s from one idle consumer, 6.5k GET/s at 1 GiB/s |
| Read path | CP returns pointers (file, index offset, batch), agent range-reads (E.4) | CP `Lookup` returns runs, broker GETs per batch | partial, same idea, no coalescing |
| Compaction | Planner family, levels and lanes, CP validates the swap (T:15) | None; log trimmed, segments never merged (dec 53) | missing |
| Retention / GC | Expire, dead-file view, bucket-scanning deadscanner for orphans (T:16) | None. Orphans from failed commits accumulate; bucket lifecycle can delete live `seg/` objects after 3 days (audit S13) | missing, and a correctness hazard |
| Consumer groups / offsets | In the CP; both classic and KIP-848; batched RPCs (T:9.2c) | None; `internal/wire/kafkawire/supported.go` serves 5 APIs (Produce, Fetch, ListOffsets, Metadata, ApiVersions); static `assign()` in the benchmark | missing. Audit S19 |
| Transactions / idempotence | CP state, per-batch producer ids in the commit | Rejected on purpose; brokers return INVALID_REQUEST for idempotent batches (dec 41, 44, TODO "not planned") | different (constraint) |
| Data lake, query, pipelines, schema registry | Same pattern again (35 `DL*` RPCs, DuckDB, Confluent-compatible SR) (T:17) | None | out of scope |
| Service discovery | Heartbeat to CP, `Discover`, power-of-2 choices, AZ steering via client.id (T:13, 7.2) | Metadata names only the answering broker (dec 29); live-broker list is a TODO needing a heartbeat lease | missing (planned, TODO "Metadata lists every live broker") |
| Auth / ACL | SASL PLAIN/SCRAM/OAUTH, mTLS, SPIFFE, ACL logs (T:19) | None: no SASL, no TLS, no ACL (audit S19) | missing |
| Observability | Prometheus on the agent, CP utilisation gauge, push telemetry by default (T:18) | Strong: facts-based metrics, JSON logs with run id, slot/index/lag series (dec 36-38, `docs/observability.md`). No CP utilisation gauge yet | have, no phone-home (good) |
| Deployment | One static binary, flags and env, 5-min drain, roles (proxy, jobs) | Two binaries (`manibroker`, `manicp`), Terraform bench stack, graceful `Stop` in core (dec 8) | partial. No role split, no drain-on-discovery yet |
| Chaos / determinism | Fault-injection env var in the binary (App A) | Deterministic simulation with faulting engines; `kafka-log`: 2,000 seeds, 0 failing, 50.7 M steps (`docs/sim-log-engine.md`); detcheck gate | have, stronger than anything in the teardown |

## 2. Top 5 takeaways for the roadmap

Constraints in force: no idempotent producers (41-44), brokers interchangeable and stateless (29, 44; TODO "per-broker
ownership: declined"), ack only after durable commit, deterministic core. Anything below that touches one is flagged
REVISIT and needs a product-owner decision; none is done silently.

Ranked by value over effort. Dependencies: 1 and 2 can start now in parallel; 3 after 1 starts; 4 needs 1; 5 needs 2 and
"index to S3" (accepted in TODO, my estimate 12 to 15 ed, not scheduled).
### 1. Consumer read path: long-poll Fetch, Lookup cache, coalesced ranged GETs, broker chunk cache (about 20 ed)
- Measured need: one idle franz-go consumer made 20.6k Lookups/s locally; 64 consumers predicted 48k Lookups/s (above the
  30k warm DynamoDB throughput, but note Lookup is now answered from Pebble on the log CP, so the limit becomes CP HTTP and
  CPU, not RCU: re-measure). One GET per batch is 6.5k GET/s at 1 GiB/s read, over the 5,500/s S3 prefix start, about
  $6.8k/month against the vendor's published $508 for its GETs (S6, computed).
- Teardown contribution: confirms pointer-based fetch, footer-first reads and a chunk cache; all of it is also the
  standard design (public docs describe "loaded once per AZ"). Build it from our own index and the audit numbers.
- Plan: (a) long-poll honouring MinBytes/MaxWait, 5 ed; (b) Lookup tail cache with bounded staleness, 4 ed; (c) merge
  adjacent batches of one segment into one ranged GET, 4 ed; (d) per-broker chunk cache with read-ahead, 8 ed. Peer ring
  across brokers is phase 2 and optional: without it, 6 brokers fetch a hot chunk up to 6 times, still about 190 GET/s
  (32 segments/s x 6, computed) instead of 6.5k.
- Regression risks: a stale Lookup cache on broker B after an ack through broker A breaks read-your-writes (brokers are
  interchangeable, so no broker knows every commit); keep staleness under 1 s and test it in the sim. Long-poll adds timers
  to the deterministic core: sim hashes re-baseline. A cache must not serve a segment the GC has deleted (item 2).
- Do NOT copy: RAM sized per vCPU as a rule (size from our w4 RSS of 237 MiB); consistent-hash peer routing before
  measured hit rates justify it (it starts to make brokers non-interchangeable for performance, not correctness: REVISIT
  decision 29's spirit if adopted).
### 2. Retention, orphan GC and bucket preflight (about 10 to 12 ed)
- Measured need: nothing is ever deleted (S13); 32 segments/s at 1M is 2.8M objects/day (computed); index-able state grows
  about 6 GB/hour (TODO). Worse, a 3-day lifecycle rule deletes `seg/` objects the index still points to.
- Plan: retention advances log-start in the CP; delete bytes only after the index no longer references them plus a grace
  period longer than the longest fetch and cache TTL; a separate min-age sweep of the bucket for orphans (we create them
  on every failed commit); startup preflight that writes a probe object and warns on lifecycle rules over `seg/`.
- Basis: the two-phase "index first, then sweep" idea is standard and also in public WarpStream docs; the preflight is
  ordinary operability. Regression risk: deleting a live object is unrecoverable; ship dry-run mode first, and add a sim
  invariant "no indexed run points to a deleted object". Not copy: the optimistic delete queue; start simple.
### 3. Broker membership, AZ hints and drain (about 10 to 13 ed, including a 1M failover test)
- Measured need: franz-go failover about 5 s on seed lists (TODO); CP failover ack gap 8 s at 500k, and a broker buffers
  at most 64 MiB (`MaxBufferedBytes`) while each broker takes about 163 MiB/s offered at 1M (84.5 MiB/s committed x about
  2), so an 8 s gap fills the buffer in well under a second and pauses client reads. Untested at 1M.
- Plan: heartbeat lease and `DescribeBrokers` (TODO design), Metadata lists live brokers with deterministic leader
  spread, optional AZ hint in client id, drain on `Stop`; try `-takeover-after 2s`; run the kill test at 1M.
- REVISIT: decision 29 says each broker names only itself; listing all brokers changes Metadata semantics, not
  interchangeability. Contract change touches core, veritas, cpengine, lockstep; hashes re-baseline.
### 4. Consumer groups, offsets and minimal admin APIs (about 35 to 45 ed)
- Measured need: 5 APIs advertised today, so any group-based client fails ("broker is too old"). This is the largest
  adoption blocker; the Kafka protocol spec is the independent basis, we do not need the teardown's API list.
- Design: group state in the applier; only membership changes and offset commits are log commands. Heartbeats stay in
  memory on the active CP: at 64 consumers and a 3 s heartbeat that is 21/s, roughly doubling the 20 to 32 slots/s we
  run today, and the hot key ceiling is about 200 entries/s (X1). Needs a deterministic time source for session expiry
  (log entry timestamps are advisory today: dec 53). After failover give all members a grace period.
- Include: FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit/Fetch, CreateTopics, DescribeConfigs.
- Regression risks: group writes compete with commit slots; offsets must not live in a topic that needs the read path.
  Idempotency window (dec 53, 131,072 slots, about 68 minutes at 32 slots/s, computed) is for segment-key dedupe and is
  unrelated to producer idempotence.
- Do NOT copy: KIP-848 protocol, transactions, producer ids. Producer idempotence stays out; this takeaway does not
  force REVISIT of 41-44, but note that Java clients default to idempotence on since Kafka 3.0, so INVALID_REQUEST is an
  adoption papercut: document it in the README; if the product owner wants a tolerant mode, that IS a revisit of 41.
### 5. Compaction of per-partition slices (about 30 ed, plus 12 to 15 for index to S3 first)
- Measured need: at 1M a segment holds about 99 of 288 partitions, so each partition gets a slice of about 155 KiB, about
  11 slices/s per partition (computed from w4 figures). Reading one hour of one partition cold is about 40k slices
  against about 380 if 16 MiB-equivalent; GETs 100x worse for catch-up reads and long retention. Index lag at 1M is 10 to
  13 s (bar was 10 s), so index and runs are already background-written.
- Plan: single-level time-window merge per partition range; a new log command swaps run pointers atomically, validated by
  the applier (the sound idea in teardown T:15, also in public docs). Log format bump uses the two-deploy rule (dec 53).
- Regression risks: rewrite cost is one read plus one write of all data (about 490 MiB/s committed each way at 1M, in-AZ
  traffic is free but broker NIC and CPU are not); keep it off the `Step` goroutine (audit S10); fetches racing a swap need
  the item 2 grace period.
- Do NOT copy: the multi-planner, levels-and-lanes family and the CP job scheduler (PollJobs). Start with one planner.

Not in the five but required before a serious OSS announcement: TLS and SASL (PLAIN/SCRAM) at the edge, about 10 ed, no
decision conflict.

## 3. Open-source readiness

Checklist. Status: OK / TODO / BLOCKER.

| Item | Status | Finding |
|---|---|---|
| LICENSE | BLOCKER | None at repo root. `build/omb/src/LICENSE` is the Apache-2.0 OMB checkout (git-ignored; our `ops/deploy/aws/bench/omb/patches/*` are derivatives and must keep its notice) |
| README, quickstart | BLOCKER | No root `README.md` (only deploy and api READMEs). Need: what it is, alpha status, "produce-only, no consumer groups", docker-compose quickstart (`ops/deploy/docker-compose.yml` exists), supported APIs |
| CONTRIBUTING, SECURITY.md, CODEOWNERS, issue templates | TODO | None exist. Pick DCO (sign-off) over a CLA for an alpha |
| Public CI | TODO | Workflow lives at `build/ci/determinism.workflow.yml`, not `.github/workflows`, so it is not running. It uses only `go vet`, `go test`, determinism script: no AWS creds needed. Docker-backed `test/integrationtest/` tests skip without Docker; add a service-container job. `make build` includes the CRAP gate (`docs/crap-gate.md`) |
| Module path | TODO | `module manifester` is not importable (`go get` needs a host path). Rename to `github.com/<org>/<repo>` before tagging; a mechanical change, but it touches every import and nothing hashes the path (verify with the sim hashes) |
| Versioning | TODO | No tags. Use `v0.1.0-alpha.N`, no compatibility promise; log entry format is versioned (dec 53), which is a real upgrade story only for the CP log |
| Maintainers | TODO | 192 commits, one author identity (GitHub noreply address, good). Bus factor 1: say so in the README |
| Name | CAUTION | "manifester": web search found no trademark, but an existing GitHub/PyPI project `SatelliteQE/manifester` (Red Hat subscription manifest tool, different domain). Not a blocker; do a USPTO/EUIPO search with a lawyer and consider a distinctive repo name. Use "Kafka" only descriptively with the Apache trademark notice |

### Licence choice
- Apache-2.0 recommended: explicit patent grant, matches the Go/cloud ecosystem, and is compatible with everything linked.
  The 49 modules linked into `./cmd/...` are Apache-2.0 (29), BSD (14, Pebble and franz-go/kmsg among them), MIT (5) and
  BSD-2 (DataDog/zstd, cgo only); no copyleft (checked from the module cache, headers only; run `go-licenses` for the record).
- MIT/BSD-3: simpler, no patent grant. AGPL, BUSL or SSPL deter hosted competitors but BUSL/SSPL are not open source by the
  OSI definition; that is a business choice, decide it before the first public commit because relicensing later needs
  contributor consent.

### Third-party audit
- 16 indirect modules from Pebble: reviewed in `docs/dep-audits/pebble-indirect-modules.md` (all permissive, edge-only,
  `internal/core/` and `internal/verify/sim/` do not reach them). State this in the README. cockroachdb/errors and sentry-go are linked into
  `manicp` (about 1 MB) and make no network call unless `sentry.Init` runs; ours never does.
- Open advisory: `golang.org/x/text` v0.34.0, GO-2026-5970, reached only via sentry-go; the fix (v0.39.0) needs `go 1.25`.
  Also go.mod says `go 1.24.0` (end of life); release builds already use go1.26.10. Before the first tag, bump the
  directive (product owner's call, the audit left it) and re-run `govulncheck` in CI.

### Secrets and identifiers (values masked; scanned tracked files and all history, 192 commits)
- No `glc_`/`glsa_` Grafana tokens, no private keys, no real AWS keys found. Tokens are read from the environment by design.
- AWS account id (masked 4784****4994): `docs/stage5-run-plan.md`, `docs/w4-log-store-results.md`; first added in commit
  `d4dd5d5`; present at HEAD and in history. Not a credential, but identifying. Same two files name the CLI profile
  `hydsandbox`.
- Grafana Cloud stack name and push URLs (org `manifesterio`, cluster ids) in `docs/aws-benchmark-runbook.md`, commit
  `1b9ab21`. Non-secret, identifying; replace with placeholders.
- AWS's documented example credentials (the public `...EXAMPLE` pair) appear in test code and docs: commits `6f5c3ef`,
  `ebea3be`, `e21eb81`, `176d541`. Harmless, but secret scanners will flag them: add an allowlist or change the fixture.
- `results/` (170 MB: `terraform-outputs.json`, raw metrics) is untracked and NOT ignored (`git status` shows `??`; the
  w4 doc says "git-ignored", which is wrong). Add to `.gitignore` now.
- Emails: only `example.com`; commits use the GitHub noreply address. No hostnames or instance/VPC ids in history.
- Remedy: rewrite with `git filter-repo --replace-text` before the first public push (history is 14 MB, rewrite is cheap),
  or publish from a fresh squashed repo and keep this one as the private design record. The remote `origin` is
  `github.com/arunlakshman/tmpdst`: confirm it is private; if it was ever public, treat the identifiers as already out.
- The teardown was never committed (`git log --all` shows no commit touching it). Keep it that way.

### Wording and competitor risk
- WarpStream appears 140 times in 30+ tracked files, including source comments (`internal/broker/config.go`, `cmd/manibench`),
  workload file names (`vanity-*`), `docs/aws-benchmark-warpstream-comparison.md` and decision text ("WarpStream style").
  Nominative factual comparison is normally fine. Do: attribute and date every vendor figure ("their blog, Oct 2026"),
  say "our reconstruction of their published settings", add a trademark line (WarpStream, Kafka and others belong to
  their owners; no affiliation). Don't: claim "parity", "faster than", or compare our 975 MiB/s produce-only with their
  symmetric figures; avoid "WarpStream clone/alternative" in the repo description. Rename `vanity` workloads.

### Reproducibility for outsiders
- Terraform stack (`ops/deploy/aws/terraform`, region has no default, good) plus runbook and OMB patches at a pinned commit.
  Needs an AWS account, ap-south-2 availability of m6in, a Grafana Cloud stack (SSM parameters) and SSO. Cost about
  $12.3/h fixed; w4 was about 37 min, estimated $10 to $13 (billing not checked). A realistic 3-run plus soak is $150 to
  $300 (estimate).
- Gap: a no-Grafana mode and a local docker-compose quickstart that reproduces the smoke stage (100k msg/s on a laptop).
  An outsider should be able to run the simulation campaign (`go run ./cmd/manisim -scenario kafka-log -seeds 300`) in minutes:
  that is the most reproducible and most differentiating asset.

### Gaps people will ask about (state them in the README, do not hide)
Consumer groups and any consumer-side result (missing); read path and cache (missing); compaction and retention (missing);
TLS, SASL, ACLs (missing); Kafka API coverage 5 of about 70 (Produce 3-9, Fetch 4-12, ListOffsets 1-7, Metadata 1-12,
ApiVersions 0-3); at-least-once with possible reorder when `max.in.flight` > 1 (audit S11); single appender by design;
single-AZ benchmark so no cross-AZ cost claim; upgrade story only for the CP log (two deploys), none for broker/CP wire
or Pebble data; no rollback path for the CP store (decision 54; an older git revision is needed); partitions measured only at 288.

## 4. Timeline (assumes one engineer about 4 productive ed/week; today Wed 2026-10-21)

| Date | Milestone | Gate (all must hold) |
|---|---|---|
| Oct 21 - Nov 6 | Hygiene sprint (about 8 ed) | Legal question sent (RE material, licence, name). Licence chosen. `results/` ignored. History rewritten or fresh repo. README, CONTRIBUTING, SECURITY, `.github` CI green on a public-style clean clone. Activate the AWS cost-allocation tag `run_id` now: it only applies from activation and bills lag about 1 to 2 days |
| Nov 9 - Nov 20 | Evidence runs w5/w6 (about 6 ed, about $150 to $300) | 3 independent 1M stages, each with 2 min warm-up and 15+ min steady; one 4 h soak at 750k; kill-appender at 1M; S3 503/slow injection; fix the missing `index_lag` bar or restate it |
| Nov 23 - Dec 4 | Write-up plus item 2 start | Cost per GiB computed from the Cost Explorer bill, not from unit prices; one outsider reproduces the laptop smoke run in under 30 min |
| **Tue Dec 1** | **OSS alpha `v0.1.0-alpha.1`, quiet** | Hygiene gates done; legal answer received for the licence and for the teardown (teardown stays out regardless); README states gaps. No announcement: lets people find leaks or build breaks before traffic |
| **Tue Dec 8** | **Blog 1** | Section 4a gates below |
| Dec 9 - Jan 29 | Items 1 and 2 (about 30 ed with holidays) | Sim invariants green, hashes re-baselined by hand |
| Feb 2 - Feb 20 | Symmetric runs, 3 repeats, bill lag | Section 4c gates |
| **Tue Mar 2, 2027** | **Blog 2** | |

If any gate slips, slip the date; never publish the blog before the repo is clean. Do not collapse OSS and blog on the same
day unless the hygiene sprint finished by Nov 20.

### 4a. Blog 1: "A Kafka-compatible log on S3 and DynamoDB that holds 975 MiB/s of writes, and the simulator that keeps it honest"
Honest claims from w4: about 975 MiB/s (998k msg/s) produce-only, random clients, 288 partitions, 6 x m6in.4xlarge, 4
minutes, p99 756 ms, 0 errors; CP idle (0.2 of 4 cores, append p99 8 ms); ack gap 8 s on appender kill at 500k.
Must not claim: symmetric 1 GiB/s; any comparison to WarpStream's numbers; production readiness; the 4 minute run as steady
state; cost per GiB (until billed); anything learned from the teardown. Extra evidence for credibility: the Nov gates
(3 runs with warm-up, soak, failure injection, bill-based cost), plus the simulation numbers (2,000 seeds, 50.7 M steps,
mutants caught within 6 seeds) and the old-design collapse (w2/w3: 240 to 275k msg/s) as an honest before and after.
Outline:
- Why: the cost of brokers, disks, cross-AZ; what we set out to test, and what we did not build (no consumers yet).
- Design in one picture: broker seals segment, S3 PUT, log slot commit, offsets assigned at apply.
- Why the first design collapsed (transaction store, 96-partition cap, 86 ms commits) and what replaced it.
- The result: 100k to 1M ladder, latency, CP load; all caveats in a box.
- Failure: killing the appender, 8 s gap, what the brokers did.
- Deterministic simulation: what it caught (mutants), how to run it.
- Cost, measured from the bill, with what it excludes.
- What is missing and what is next (read path, groups).
Charts: throughput and p50/p99 per stage; segment size and partitions per segment vs rate; CP append latency and slots/s;
kill-test timeline; old vs new commit path; cost stack.

4b. OSS alpha gates: section 3 BLOCKERs closed, secrets remedied, clean-clone CI, licence, no teardown content.

### 4c. Blog 2: "Reading it back: a consumer path for an S3-backed log, and the symmetric 1 GiB/s run"
Gates: items 1 and 2 done; 3 symmetric runs at 1M write plus 1M read meeting 95% of target both ways; measured GET/s and
Lookups/s vs the audit predictions (6.5k GET/s, 48k Lookups/s); end-to-end p99 reported; consumers use static assign (no
groups) disclosed; single-AZ disclosed; cost per GiB from the bill including GETs. Not before: item 4 is NOT required, but
say it is missing.
Outline:
- What blog 1 left out: consumers, and why the first read path would have failed (the Lookup storm, the GET math).
- Long-poll and the Lookup cache: Lookups/s before and after.
- Coalesced GETs and the chunk cache: GET/s, hit rate, S3 503s avoided.
- The symmetric run and what its latency is made of.
- Failure while reading: appender kill and broker kill under load.
- Retention and GC: the 3-day lifecycle trap we fell into.
- Cost per GiB, now with reads.
- Limits and what is next (groups, compaction).
Charts: GET/s and Lookup/s before and after; cache hit rate; end-to-end latency CDF; throughput timeline both directions;
bill breakdown.

Not before (any): legal answer; secrets remedy; 3 repeat runs; bill-based cost; a stranger reproduced the smoke run; name
search; README gap list. Blog 2 also: item 1 merged, sim green.
