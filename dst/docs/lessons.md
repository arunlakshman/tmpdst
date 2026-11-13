# Lessons for deterministic simulation tests

Short distillation of Moonpool and DST practice for this module.

## One RNG

Drive every random choice from one seeded stream per run. Split the stream when a subsystem needs its own draws. Do not call `math/rand` or wall-time seeds inside the scenario. Same seed must replay the same schedule and the same fingerprint.

## Virtual time

Advance time only through the scheduler heap. Timers and delayed messages are events on that heap. Package `clock` wraps the same heap with a wall offset for standalone tests. Wall clocks and sleep stay out of the subject under test.

## Issue-time faults

Decide whether a fault fires when the op is issued, not when a later observer notices pain. Latch per site so a drop or crash is stable under replay. Cooperative injectors (`fault`) beat hidden probability inside product code.

## Phases

Separate setup, fault window, heal, and check. Keep crash and reboot ops late in an ordered alphabet so shrinkers land on short failing prefixes. Prefer simplest ops first (reads and no-ops before writes).

## Record and continue

An assertion failure should record and keep running when you still want coverage of later properties. Package `assert` supports Always / Sometimes / Reachable / Unreachable with coverage counts. Fail the run at the end from the log, not at the first check.

## Go sans I/O

Keep the subject free of real network, disk, and wall clocks inside the sim. `kernel` runs the typed `machine.Machine` values; it owns rng, sched, faults, assert, and trace. Products bring their own typed Machine and domain messages. This module stays the host, not the product vocabulary.
