# Dependency audit records

Every third-party module that the deterministic packages (`internal/core/`, `internal/verify/simulator/`)
depend on needs a reviewed record here, at exactly
`docs/dep-audits/<module path>@<version>.md`. `ops/scripts/ci-determinism.sh` fails
CI when one is missing, so a version bump forces a re-audit.

Only modules that the deterministic packages really run are checked. The script does not
follow imports into the generated gRPC API (`internal/controlplane/api/v1`), because that is
the input/output edge. So grpc, protobuf, genproto and golang.org/x/{net,sys,text} need no
record here. Deterministic packages may use the generated API for type definitions only
(decision 57).

How to produce one: run `ops/scripts/audit-dep.sh <module>@<version>` and follow
`.claude/skills/audit-go-dependency/SKILL.md`. Use the template below.

```markdown
# <module>@<version>

- Date / auditor:
- Requires Go:
- Verdict: ACCEPTED | ACCEPTED WITH RULES | REJECTED
- Import paths allowed (ops/tools/determinismcheck/allow.txt):

## Findings
| Finding (file:line) | Matters? | Why |
|---|---|---|

## Rules for our code
1. ...

## Evidence
- determinismcheck output summary
- encode/decode twice, separate processes: identical?
```
