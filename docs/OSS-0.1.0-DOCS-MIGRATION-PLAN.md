# MDS docs greenfield: OSS 0.1.0 migration plan

Status: in progress (Phases 1-5 largely done on branch; honesty pass + tag remain).
Goal: replace the current engineering scrapbook under `docs/`
with a GitHub-facing tree for the `v0.1.0` open-source launch, without breaking
CI that depends on audit-record paths.

Audience after launch: strangers on GitHub (try it, know what works, contribute).
Internal design history stays available but is not the front door.

---

## 0. Goals and non-goals

### Goals

1. A stranger can clone → run `mds-all` → produce/consume → know which Kafka APIs
   work → find how to build/contribute, without reading design notes.
2. Root `README.md` is product-first (what / quickstart / what works / links).
3. Module READMEs stay build-oriented; they point into the new `docs/` spine.
4. Determinism CI keeps working (`ops/*/scripts/ci-determinism.sh`).
5. Honest 0.1.0 surface: tables match `kafkawire.Supported` and call out gaps
   (including post-0.1.0 work in MDS-11) without aspirational claims.

### Non-goals (0.1.0)

- Docs site tooling (MkDocs / Docusaurus / VitePress). Markdown in-repo is enough.
- Rewriting every ADR into polished essays.
- Publishing Jira as the source of truth for users (link once; do not mirror).
- Moving deploy Containerfiles or Helm charts; only document and link them.

### Success checks

- [ ] Fresh clone path documented end-to-end in ≤5 steps (Podman + `mds-all`).
- [ ] `docs/what-works-in-0.1.0.md` API table ≡ `broker/internal/wire/kafkawire/supported.go`.
- [ ] `rg 'docs/(broker|controlplane|dst)/' --glob '!docs/engineering/archive/**'` shows only
      intentional surviving paths (dep-audits, retargeted refs, archive).
- [ ] `ops/broker/scripts/ci-determinism.sh` and `ops/controlplane/scripts/ci-determinism.sh` green.
- [ ] `CONTRIBUTING.md` + `CHANGELOG.md` exist at repo root; root README links them.
- [ ] No page claims KIP-848, transactions, or unsupported Kafka APIs as shipped.

---

## 1. Target information architecture

```
docs/
  README.md                      # TOC: users → ops → contribute → reference → engineering
  getting-started.md             # canonical quickstart (mds-all); link deploy/container
  what-works-in-0.1.0.md         # honest capability matrix + known gaps
  architecture.md                # broker ↔ CP ↔ stores/objects; module DAG
  kafka-compatibility.md         # Supported APIs + intentional Kafka diffs (classic groups)
  operations/
    run.md                       # binaries, flags, stores (mem/dynamo/postgres), objects
    observability.md             # metrics/logs/run_id (from current observability.md)
    deploy.md                    # separate images, kind+helm pointer → deploy/
  contribute/
    build.md                     # mise, make build, Podman, hooks, standalone modules
    determinism.md               # short: why, allow-list, audit records, sim campaigns
    risk-gate.md                 # threshold / how to read scores (keep or move from broker/)
  reference/
    glossary.md                  # from controlplane/glossary.md
    dst.md                       # public face of dst (trim of v2.md)
  engineering/
    README.md                    # “not required for users”
    decisions.md                 # short ADR index of still-true decisions only
    archive/                     # frozen pre-launch design notes (git history preserved)
      broker/…
      controlplane/…
      dst/…
  broker/dep-audits/             # CI contract — keep path bit-identical (Phase 1 default)
  controlplane/dep-audits/       # CI contract — keep path bit-identical
```

Repo root (launch pack, not only under `docs/`):

| File | Role |
|------|------|
| `README.md` | Product pitch + quickstart + links into `docs/` |
| `CONTRIBUTING.md` | How to build, test, PR; points to `docs/contribute/` |
| `CHANGELOG.md` | `## 0.1.0` section for the tag |
| `LICENSE` | already present |

Canonical vs satellite:

- Quickstart lives in **`docs/getting-started.md`**.
- `deploy/container/README.md` keeps Containerfile/Makefile detail; top of that file
  links to `docs/getting-started.md` as the human entry.
- Do not triple-copy the five-step flow into root README, getting-started, and
  container README — root gets a short paste + link; getting-started is full;
  container README is implementation detail.

---

## 2. Phase 1 — Freeze CI paths

**Decision (default): keep dep-audit paths bit-identical.**

Rationale: both gates resolve records as:

- `ops/broker/scripts/ci-determinism.sh` → `../docs/broker/dep-audits/$modver.md`
- `ops/controlplane/scripts/ci-determinism.sh` → `../docs/controlplane/dep-audits/$modver.md`
- `ops/broker/scripts/audit-dep.sh` checklist text names `docs/broker/dep-audits/…`

Moving them in the same PR as the nuke couples docs IA to CI and risks a red
`make build` / determinism workflow. Defer any move to a later housekeeping PR.

### Steps

1. List current audit files; treat them as frozen during the docs rewrite:

   - `docs/broker/dep-audits/**`
   - `docs/controlplane/dep-audits/**`

2. In the nuke/rewrite PR(s), **do not rename, merge, or restyle** those paths
   beyond fixing broken internal links if any.

3. Update prose that describes the policy to point at
   `docs/contribute/determinism.md`, but keep the on-disk path in scripts unchanged.

4. Optional later PR (post-tag): move to `docs/engineering/dep-audits/{broker,controlplane}/`
   only with a single coordinated change to both `ci-determinism.sh` scripts,
   `audit-dep.sh`, module READMEs, and code comments that cite the old path.

### Exit criteria

- [ ] Determinism scripts still find every required `$modver.md`.
- [ ] No Phase-1 commit changes audit filenames or directories.

### Alternative (only if you insist on one mega-PR)

One PR that: `git mv` audits → new path → patch both CI scripts + audit-dep.sh +
all README pointers → run both determinism scripts. Do not mix that with content
rewrites in the same review if avoidable.

---

## 3. Phase 2 — Inventory salvage

Mark every current non-audit file. Disposition key:

| Tag | Meaning |
|-----|---------|
| **PROMOTE** | Rewrite into a launch page (source of content). |
| **REWRITE** | Keep topic; new home and shorter public wording. |
| **ARCHIVE** | Move under `docs/engineering/archive/` as-is (fix links in code comments later). |
| **FOLD** | Absorb useful bits into another page; do not keep standalone. |
| **DELETE** | Only if content is false/superseded and git history is enough (prefer ARCHIVE). |

### 3.1 Broker (`docs/broker/`)

| File | Disposition | Target / notes |
|------|-------------|----------------|
| `read-consumer-gaps-for-kafka-clients.md` | **PROMOTE** | Primary source for `what-works-in-0.1.0.md` + parts of `kafka-compatibility.md`. Strip internal design cross-links; keep Fetch/group behavior that users need. |
| `observability.md` | **PROMOTE** | → `operations/observability.md` (ports, prefixes, run_id). |
| `sql-log-backend.md` | **REWRITE** | Fold operator-facing store notes into `operations/run.md`; archive full design. |
| `risk-gate.md` | **REWRITE** | → `contribute/risk-gate.md` (threshold, how to fix). Keep reachable from broker README / riskscore comment. |
| `decisions.md` | **FOLD + ARCHIVE** | Extract still-true rows into `engineering/decisions.md`; full log → `engineering/archive/broker/decisions.md`. |
| `consumer-groups-design.md` | **ARCHIVE** | `engineering/archive/broker/`. User-facing group behavior lives in what-works / kafka-compatibility. |
| `writer-fencing-design.md` | **ARCHIVE** | Internal; code comments may keep pointing at archive path after retarget. |
| `flow-control.md` | **ARCHIVE** | Contributor-deep; optional one-paragraph in architecture. |
| `lessons.md` | **FOLD + ARCHIVE** | Distill ½ page into `contribute/determinism.md`; archive rest. |
| `dep-audits/**` | **KEEP** | Phase 1 freeze. |

### 3.2 Control plane (`docs/controlplane/`)

| File | Disposition | Target / notes |
|------|-------------|----------------|
| `kafka-differences.md` | **PROMOTE** | → `kafka-compatibility.md` (group-protocol diffs section). |
| `glossary.md` | **PROMOTE** | → `reference/glossary.md`. |
| `simulation.md` | **REWRITE** | Short “how we test CP” in `contribute/determinism.md`; full text → archive. |
| `rebalance.md` | **ARCHIVE** | Spec/machine source of truth for contributors; link from `contribute/` and keep code comment paths updated to archive. |
| `dep-audits/**` | **KEEP** | Phase 1 freeze. |

### 3.3 DST (`docs/dst/`)

| File | Disposition | Target / notes |
|------|-------------|----------------|
| `v2.md` | **REWRITE** | → `reference/dst.md` (trim examples; point at `dst/examples/`). |
| `lessons.md` | **FOLD** | Into `contribute/determinism.md` (and keep archive copy if lesson 12 is cited by determinismcheck). |
| `paper.md` | **ARCHIVE** | Research mapping; not launch-facing. |

### 3.4 Suggested working table (fill during Phase 2 PR)

For each file above, record in the PR description:

```
path | disposition | new path | owner | done?
```

### Exit criteria

- [ ] Every current `docs/**/*.md` has a disposition.
- [ ] No PROMOTE item lacks a named target page.
- [ ] ARCHIVE list is complete before any delete.

---

## 4. Phase 3 — Write launch spine first

Write new pages **before** deleting old paths that still receive traffic from
READMEs. Prefer additive PR: add spine + temporary redirects/stubs, then remove.

### 4.1 Page briefs

#### `docs/getting-started.md`

- Prerequisites: Podman (macOS: `podman machine start`).
- Five steps from `deploy/container/README.md` (`make image-all` / `mds-all:dev`).
- Smoke: curl metrics ports; TCP 9092; one produce/consume example (kcat or
  kafka-console-*, and/or franz-go snippet — pick one primary).
- Explicit: mem stores; state lost on stop; not for production.
- Next links: what-works, operations/run, operations/deploy.

#### `docs/what-works-in-0.1.0.md`

- One sentence product scope.
- API table generated from / verified against `Supported()` (16 APIs).
- Classic consumer groups: what works; FindCoordinator hashing; no KIP-848.
- CreateTopics quirks (replication factor ignored; configs ignored).
- Unsupported: connection closed (current behavior); list explicitly “not in table”.
- “Not in 0.1.0” section: point at MDS-11 once; no feature promises.
- E2E script pointer: `ops/scripts/e2e-kafka-cli.sh`.

#### `docs/architecture.md`

- ASCII or mermaid: client → mdsbroker → object store; mdsbroker → mdscontrolplane → log store + metadata.
- Module DAG: `dst ← shared ← controlplane ← broker`.
- One paragraph on deterministic core vs I/O edge (no deep lesson dump).
- Link glossary + kafka-compatibility.

#### `docs/kafka-compatibility.md`

- Supported version ranges (same source of truth as what-works; avoid drift —
  either single source page or “table lives in what-works; this page is diffs”).
- Intentional diffs from Apache Kafka classic groups (from `kafka-differences.md`).
- Compression: opaque bytes, codecs accepted, no decompress on broker.

#### `CONTRIBUTING.md` (repo root)

- Prerequisites: mise, Podman for broker IT / full hook.
- `mise install` → `make generate` → `make build`.
- When to `SKIP_BUILD_HOOK=1`.
- Pointers: `docs/contribute/build.md`, determinism, risk-gate.
- PR expectations: ascii, fmt, tests you touched; no secret commits.

#### Supporting ops/contribute pages (same phase or immediate follow-on)

- `operations/run.md`, `operations/observability.md`, `operations/deploy.md`
- `contribute/build.md`, `contribute/determinism.md`, `contribute/risk-gate.md`
- `reference/glossary.md`, `reference/dst.md`
- `docs/README.md` TOC

### 4.2 Root README rewrite (same phase)

Structure:

1. What MDS is (one short paragraph).
2. Quickstart (3–5 lines + link to `docs/getting-started.md`).
3. What works in 0.1.0 (link).
4. Modules / build (condensed from today).
5. License / contributing / changelog links.

### 4.3 PR strategy

| PR | Contents |
|----|----------|
| A | Add spine pages + CONTRIBUTING + CHANGELOG stub; do not delete old docs yet. Point root README at new pages **and** keep old paths for a short window. |
| B | Module README “Read more” sections retargeted (Phase 5 can merge with B). |

### Exit criteria

- [ ] All spine files exist and are linked from `docs/README.md` and root README.
- [ ] Getting-started steps executed once on a clean machine/VM notes recorded in PR.

---

## 5. Phase 4 — Compress engineering

### 5.1 Decisions → ADR index

From `docs/broker/decisions.md` (and any CP-only decisions still only in prose):

1. List decisions that are **still true and load-bearing** for 0.1.0
   (examples of likely keepers: import allow-list / audits; metrics-at-edge;
   compression opaque; classic groups only; log backends mem/dynamo/postgres;
   sticky appender / fencing at high level — not the full fencing essay).
2. Write `docs/engineering/decisions.md` as a **short table**:
   `# | one-line decision | where enforced (code/doc)`.
3. Move the full historical log to
   `docs/engineering/archive/broker/decisions.md`.

Do not renumber historical decisions. Archive keeps original numbers.

### 5.2 Designs → archive

```bash
# illustrative; exact mv list from Phase 2 inventory
mkdir -p docs/engineering/archive/{broker,controlplane,dst}
git mv docs/broker/consumer-groups-design.md docs/engineering/archive/broker/
git mv docs/broker/writer-fencing-design.md docs/engineering/archive/broker/
git mv docs/broker/flow-control.md docs/engineering/archive/broker/
# … remaining ARCHIVE rows …
```

Keep `docs/engineering/README.md` explaining: archive is historical; prefer spine
pages; CI audits live under `docs/*/dep-audits/`.

### 5.3 Lessons / simulation / paper

- Fold operator/contributor essentials into `contribute/determinism.md`.
- Archive long forms.
- **Special case:** `dst/cmd/determinismcheck` cites `docs/dst/lessons.md` lesson 12.
  Either:
  - keep a stub at `docs/dst/lessons.md` that points to the new section, or
  - update the Go strings to `docs/contribute/determinism.md` in the same PR as the move.

Prefer updating the Go references (Phase 5) and archiving lessons, with a one-line
stub only if you need a soft redirect for external bookmarks.

### Exit criteria

- [ ] `engineering/decisions.md` is skim-able in &lt;5 minutes.
- [ ] No design essay remains under `docs/broker/` or `docs/controlplane/` except
      `dep-audits/` and intentional stubs.
- [ ] determinismcheck / riskscore doc paths resolve.

---

## 6. Phase 5 — Retarget links

### 6.1 Mechanical sweep

```bash
rg -n 'docs/(broker|controlplane|dst)/' --glob '!docs/engineering/archive/**'
```

Update classes of references:

| Class | Examples | Action |
|-------|----------|--------|
| Root / module READMEs | `README.md`, `broker/README.md`, `controlplane/README.md`, `dst/README.md`, `controlplane/spec/README.md` | Point to spine / contribute / reference. |
| Ops scripts | `ci-determinism.sh` comment, `audit-dep.sh`, `deploy/container/broker-compose.yml` | Keep audit paths; retarget decision comments to archive or ADR index. |
| Tools | `ops/tools/riskscore/main.go`, `dst/cmd/determinismcheck/*.go` | New contribute paths. |
| Package docs | `broker/internal/core/doc.go`, `controlplane/.../doc.go`, rebalance packages, simulator docs | Point to archive for deep design; spine for overview. |
| Metrics comments | `controlplanemetrics.go` → observability | `operations/observability.md`. |

### 6.2 Stub policy (optional, 1 release)

If external links matter, leave tiny stubs at old paths:

```markdown
# Moved

This page moved to [`docs/…`](…).
```

Remove stubs after `v0.1.0` or in MDS-11 docs cleanup.

### 6.3 Duplicate dep-audit trees

Broker and controlplane both carry near-duplicate Pebble / prometheus / kmsg
audits. **Do not merge in 0.1.0** (CI paths differ). Note as post-0.1.0 cleanup.

### Exit criteria

- [ ] Sweep command shows only: dep-audits, stubs, archive, or updated intentional refs.
- [ ] `make build` (or at least determinism + compile) green after comment path edits.

---

## 7. Phase 6 — Honesty pass

### 7.1 Single source for API matrix

Source of truth: `broker/internal/wire/kafkawire/supported.go` (`supported` table /
`Supported()`).

Checklist against current table (verify again at freeze time):

| API | Key | Versions (as of plan authoring) |
|-----|-----|----------------------------------|
| Produce | 0 | 3–9 |
| Fetch | 1 | 4–12 |
| ListOffsets | 2 | 1–7 |
| Metadata | 3 | 1–12 |
| OffsetCommit | 8 | 0–8 |
| OffsetFetch | 9 | 0–8 |
| FindCoordinator | 10 | 0–4 |
| JoinGroup | 11 | 0–7 |
| Heartbeat | 12 | 0–4 |
| LeaveGroup | 13 | 0–5 |
| SyncGroup | 14 | 0–5 |
| DescribeGroups | 15 | 0–5 |
| ListGroups | 16 | 0–4 |
| ApiVersions | 18 | 0–3 |
| CreateTopics | 19 | 0–7 |
| DescribeConfigs | 32 | 0–4 |

Process:

1. Diff doc table vs `supported` slice before merge.
2. Prefer a short note: “If this table disagrees with `supported.go`, the code wins.”
3. Optional follow-up (post-0.1.0): generate the markdown table in CI from the Go
   table to prevent drift.

### 7.2 Behavioral honesty (from current gaps doc)

Call out explicitly in what-works / kafka-compatibility:

- Classic groups only; KIP-848 not advertised.
- CreateTopics ignores replication factor and configs.
- DescribeConfigs returns empty config lists for topics.
- Unsupported API key/version: connection closed (no error payload).
- Fetch long-poll / MinBytes behavior summary (user-visible).
- `mds-all` = demo only; production = separate images + real stores.

### 7.3 MDS-11

- One link to [MDS-11](https://manifester-e50593c9.atlassian.net/browse/MDS-11) under
  “After 0.1.0”.
- Do not paste the Jira backlog into docs.
- Do not imply private Jira is required to use the OSS build.

### 7.4 Language scrub

- Remove internal branch names, “stage N”, “T6.1”, “accepted by product owner”
  from launch pages.
- Archive may keep them.

### Exit criteria

- [ ] Doc API table matches `supported.go` byte-for-byte on keys/min/max.
- [ ] Launch pages pass a “would I be mad as a Kafka user?” read.
- [ ] No launch page promises unfinished MDS-11 items.

---

## 8. Phase 7 — Tag alignment (`v0.1.0`)

### 8.1 CHANGELOG

Create/update `CHANGELOG.md`:

```markdown
## 0.1.0 — YYYY-MM-DD

### Added
- Kafka-protocol broker (`mdsbroker`) and control plane (`mdscontrolplane`).
- Classic consumer groups via control plane.
- Demo all-in-one image `mds-all` (in-memory).
- Deterministic simulation library (`dst`).
…

### Known limitations
- Link to docs/what-works-in-0.1.0.md
```

Keep bullets user-facing; no internal ticket dumps.

### 8.2 Pre-tag checklist

- [ ] Phases 1–6 done on the release branch.
- [ ] Root README, CONTRIBUTING, CHANGELOG, LICENSE present.
- [ ] `docs/README.md` TOC complete.
- [ ] Quickstart verified on clean Podman.
- [ ] Determinism CI green.
- [ ] Archive present; launch spine has no broken relative links (`lychee` or
      manual click-through of TOC).
- [ ] GitHub repo description / topics (kafka, go, …) set outside this plan.
- [ ] Tag `v0.1.0` only after docs merge (or same release commit).

### 8.3 Suggested commit / PR slicing (summary)

| Order | PR theme | Risk |
|-------|----------|------|
| 1 | Docs spine + CONTRIBUTING + CHANGELOG + README soft-cutover | Low |
| 2 | Archive moves + engineering/decisions compress | Medium (link churn) |
| 3 | Retarget code comments + tool Doc strings | Low–medium |
| 4 | Delete stubs / old trees left behind; honesty pass | Low |
| 5 | Release commit / tag | Process |

Phase 1 (freeze audits) is a **constraint on all PRs**, not its own content PR.

---

## 9. Roles, effort, risks

### Rough effort

| Phase | Effort (order of magnitude) |
|-------|-----------------------------|
| 1 Freeze | Hours (decision + discipline) |
| 2 Inventory | Half day |
| 3 Spine | 1–2 days writing + one dry-run quickstart |
| 4 Compress | Half–1 day |
| 5 Retarget | Half day + review |
| 6 Honesty | Half day |
| 7 Tag | Process / hours |

### Risks

| Risk | Mitigation |
|------|------------|
| Break determinism CI by moving audits | Default: do not move (Phase 1). |
| Doc drift from `supported.go` | Honesty pass + “code wins” note; later generate. |
| Broken Go comments after archive | Phase 5 sweep mandatory before tag. |
| Dual quickstarts diverge | One canonical getting-started; satellites link. |
| Over-sharing internal Jira / design | Archive + language scrub. |

---

## 10. Immediate next actions

1. Confirm Phase 1 default: **keep `docs/*/dep-audits` paths**.
2. Approve Phase 2 disposition table (edit any PROMOTE ↔ ARCHIVE calls).
3. Open PR A: spine pages + root README/CONTRIBUTING/CHANGELOG without deleting
   old docs yet.
4. Run getting-started dry-run; fix steps from reality.
5. Continue Phases 4–7 on follow-up PRs through `v0.1.0`.

This plan file itself should move to `docs/engineering/archive/` (or be deleted)
once the migration is complete so it is not mistaken for user documentation.
