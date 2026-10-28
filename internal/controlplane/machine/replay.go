package machine

import (
	"fmt"

	"manifester/internal/controlplane/applier"
	"manifester/internal/core"
)

// ReplaySlot folds one durable log entry into fold. Used for CPTruth (reference
// replay of the Dynamo log). Client waiters and BugWriterOffsets stamps are
// ignored: truth assigns offsets at apply time like a cold replica.
func ReplaySlot(fold *applier.Applier, slot core.LogSlot, data []byte) error {
	_, _, payload, ok := unwrap(data)
	if !ok {
		return fmt.Errorf("machine: truncated log envelope at slot %d", slot)
	}
	if _, cmds, ok := decodeGroup(payload); ok {
		for _, c := range cmds {
			c.WriterBases = nil
			fold.Apply(int64(slot), c)
		}
		return nil
	}
	_, err := fold.ApplyBytes(int64(slot), payload)
	return err
}
