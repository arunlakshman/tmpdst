package statemachine

import (
	"fmt"

	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

// ReplayPosition folds one durable log entry into fold. Used for the reference
// replay of the Dynamo log. Client waiters and InjectedBugWriterOffsets stamps are
// ignored: truth assigns offsets at apply time like a cold replica.
func ReplayPosition(fold *stateupdater.StateUpdater, slot core.LogPosition, data []byte) error {
	_, _, payload, ok := unwrap(data)
	if !ok {
		return fmt.Errorf("statemachine: truncated log envelope at slot %d", slot)
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
