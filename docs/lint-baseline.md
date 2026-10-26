# Lint baseline

All enabled rules in `.golangci.yml` are enforced on the whole tree. Any
finding fails `golangci-lint` (and therefore `make lint` / `make build`):
`run.issues-exit-code: 1`, with unlimited `max-issues-per-linter` /
`max-same-issues` so nothing is capped into a soft warning. The
`issues.new-from-rev` grandfathering (formerly `41894c2`) has been removed;
design-rule debt (nonamedreturns, forbidigo, funlen, gocognit, nestif, errname,
interfacebloat, dupl) is paid.

Original rule set (errcheck, revive, errorlint, staticcheck, gocritic, nakedret,
usestdlibvars, copyloopvar): still clean everywhere.

Panic remains allowed only where the config excludes it: `core/`, `sim/`,
`tools/`, `veritas/` (reference machine), `cp/applier/` (mirrors veritas), and
`*_test.go`. `time.Now` remains banned in `cp/logcp` and `cp/logsm` except
`env.go` and tests.

Not linter-checkable: tasks 7, 9, 23, 25.
