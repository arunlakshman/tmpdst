# Dependency audit records

Every third-party module that the deterministic packages (`core/`, `sim/`)
depend on needs a reviewed record here, at exactly
`docs/dep-audits/<module path>@<version>.md`. `scripts/ci-determinism.sh` fails
CI when one is missing, so a version bump forces a re-audit.

How to produce one: run `scripts/audit-dep.sh <module>@<version>` and follow
`.claude/skills/audit-go-dependency/SKILL.md`. Use the template below.

```markdown
# <module>@<version>

- Date / auditor:
- Requires Go:
- Verdict: ACCEPTED | ACCEPTED WITH RULES | REJECTED
- Import paths allowed (tools/detcheck/allow.txt):

## Findings
| Finding (file:line) | Matters? | Why |
|---|---|---|

## Rules for our code
1. ...

## Evidence
- detcheck output summary
- encode/decode twice, separate processes: identical?
```
