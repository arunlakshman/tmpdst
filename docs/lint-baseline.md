# Lint baseline

Original rule set (errcheck, revive, errorlint, staticcheck, gocritic, nakedret,
usestdlibvars, copyloopvar): clean on all code, enforced everywhere.

Design rules added later are grandfathered by `issues.new-from-rev: 41894c2` in
`.golangci.yml`: only lines changed after that commit are checked. Debt at that
point (list it with `golangci-lint run --new-from-rev= --max-issues-per-linter=0 --max-same-issues=0 ./...`):

| linter | existing | rule | refactor task |
|---|---|---|---|
| nonamedreturns | 55 | no named results (defer-recover allowed) | 20 |
| forbidigo | 21 | no `panic`; no `time.Now` in cp/logcp, cp/logsm | 15, 22 |
| funlen | 4 | func > 80 lines / 50 statements | 13, 14 |
| gocognit | 3 | cognitive complexity > 25 | 13, 14 |
| errname | 3 | sentinel `ErrX`, type `XError` | 17 |
| interfacebloat | 2 | interface > 8 methods | 24 |
| nestif | 1 | nested if complexity > 5 | 21 |
| dupl | 7 | clone >= 50 tokens (logcp/logsm twins only show at ~35, too noisy) | 10 |

Paying a batch: fix, then move `new-from-rev` forward to the fixing commit.
CI needs `fetch-depth: 0` or `new-from-rev` cannot resolve.
Not linter-checkable: tasks 7, 9, 23, 25.
