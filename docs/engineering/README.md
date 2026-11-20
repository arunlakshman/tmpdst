# Engineering notes

Not required for users. Operators and contributors should start at
[`docs/README.md`](../README.md) (getting started, what works, operations,
contribute, reference).

This folder holds:

- [`decisions.md`](decisions.md) — short index of still-true 0.1.0 design decisions
- [`archive/`](archive/) — frozen pre-launch design essays (`broker/`,
  `controlplane/`, `dst/`); prefer spine pages above for day-to-day work

Dependency audit records used by CI stay at `docs/broker/dep-audits/` and
`docs/controlplane/dep-audits/` (do not move them without updating the
determinism scripts).
