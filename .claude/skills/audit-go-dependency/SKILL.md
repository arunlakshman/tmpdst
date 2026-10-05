---
name: audit-go-dependency
description: Audit a Go module for determinism before the deterministic packages (core/, sim/) may import it, or after bumping its version. Use when adding or upgrading a dependency of core/ or sim/, when detcheck reports an import that is not on the allow-list, or when scripts/ci-determinism.sh reports a missing audit record.
---

# Audit a Go dependency for determinism

The deterministic packages (`core/`, `sim/`) may import only what is on the
exact-path allow-list `tools/detcheck/allow.txt`. A third-party module also
needs a reviewed record at `docs/dep-audits/<module>@<version>.md`, or CI fails.
Production-only code (`sys/` adapters such as the AWS SDK and the HTTP client)
needs no determinism audit.

## Steps

1. **Run the audit script** (it never modifies this repo):
   `scripts/audit-dep.sh <module>@<version> [package-pattern]`
   It prints the direct imports, the third-party packages pulled in, detcheck
   findings, manual-review hints, and a checklist.

2. **Review every detcheck finding.** For each, decide whether it can affect
   behavior across runs of the same seed:
   - imports of `time`, `math/rand`, `crypto/rand`, `os`, `net`, `sync`,
     `runtime`, `hash/maphash`, `maps`: find every call site;
   - `go`, channels, `select`: reject unless provably unreachable from our use;
   - range over map: acceptable only if the result is sorted or order cannot
     escape;
   - package-level vars: acceptable only if never written at runtime (no pools,
     caches or registries).

3. **Check what grep cannot:**
   - **Aliasing:** do returned `[]byte`/strings point into the input? Look for
     three-index slices (`b[i:j:k]`) and `unsafe`. If yes, write a rule for our
     code (for example "never reuse a buffer after decoding").
   - **Hidden defaults:** timestamps, random IDs, checksums filled in implicitly?
   - **Required Go version:** a newer `go` directive means the repo toolchain
     must move too.

4. **Prove it empirically.** In the script's scratch module (or a new one under
   the session scratchpad), write a small program that encodes and decodes
   representative values twice and in two separate processes. Compare bytes and
   `reflect.DeepEqual` results.

5. **Decide:** ACCEPTED, ACCEPTED WITH RULES, or REJECTED. If rejected, stop
   and tell the user why and what alternatives exist.

6. **If accepted:**
   - Write `docs/dep-audits/<module>@<version>.md` using the template in
     `docs/dep-audits/README.md`, with file:line for every finding.
   - Add the **exact** import path(s) our code will use to
     `tools/detcheck/allow.txt`. Never add a bare prefix like `math`; use
     `/...` only for a whole subtree you audited.
   - `go get <module>@<version>` in the repo and run `scripts/ci-determinism.sh`.
   - Add any rules for our code to `docs/decisions.md`.

7. **On a version bump:** rerun from step 1 for the new version and diff the
   findings against the previous record. Keep the old record until the old
   version is gone from `go.mod`.

## Never

- Never weaken `tools/detcheck` or delete an allow-list check to make CI pass.
- Never suppress a finding in our code without `//detcheck:ok <reason>`.
- Never accept a dependency on "it's just a codec": check aliasing and hidden defaults.
