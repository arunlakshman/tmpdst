// Package logsm is the PROTOTYPE of idea A (docs/delos-logdrive-ideas.md,
// docs/design-a-prototype.md): the control-plane state machine over a log of
// COMMANDS. Nothing here is wired into cmd/manicp or any default path.
//
// The log holds the raw command (a CommitSegment request, a CreateTopic, or a
// no-op that fills a hole), never sequencer-computed bases. Every Replica
// applies the log in slot order through veritas.ControlPlane (the oracle:
// decision 21) and so derives identical offsets, identical replay and
// ErrConflict answers (first slot wins) and identical state. A Committer
// answers a commit only after its own replica has applied through the slot.
//
// Log is the loglet-style seam: Append at a slot if the slot is empty, Read,
// CheckTail. MemLog and DynLog (over a DynamoDB client such as cp/dynamo/fakedb)
// implement it.
package logsm
