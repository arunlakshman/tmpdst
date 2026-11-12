# ShardStore (SOSP'21) mapped to dst

Bornholt et al., *Finding and Preventing Bugs in Systems through Deterministic Simulation Testing*, SOSP 2021, centers on ShardStore. This note maps the paper's ideas onto this module. It is not a full proof of any product.

## Reference model

The paper keeps a sequential reference model and compares it to the system under test. Package `model` sits on [rapid](https://pkg.go.dev/pgregory.net/rapid). A product supplies a `Model` and a `System`; `NewStep` builds the lockstep `Step`. Observations must match; an optional invariant runs after each op.

## Property-based testing lockstep

ShardStore generates operation sequences and checks each step. `model.SimplestFirst` wraps `rapid.OneOf` so earlier generators are simpler for shrink. Put reads and no-ops first, crash/reboot ops last. `Step.CheckGen` draws from that generator and uses `CheckAllowFailed` for the paper "has failed" relaxation: after an injected failure, a subject error is allowed, a wrong answer is not.

## Shrink

When a long sequence fails, shrink toward a shorter one that still fails. Ordered alphabets via `SimplestFirst` and pure ops make shrink cheap. rapid owns the shrinker.

## Crash property

After a crash and recover, answered writes must still be present, and a clean restart must match the model again. `model.DirtyReboot` runs `CheckPersistence` then `CheckForwardProgress`. Both take callbacks so product types stay out of this module. Issue-time crash faults belong in `fault` / `sim` Crash+Restart.

## What this is not

dst does not prove linearizability, invent a product Machine type, or replace a full network-partition simulator. It hosts deterministic runs, fingerprints (`trace`, `golden`), and lockstep checks so a product can encode its own ShardStore-style campaign.
