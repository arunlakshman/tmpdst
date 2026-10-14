package sim

import (
	"fmt"
	"testing"
	"time"

	"manifester/toys3/control"
	"manifester/toys3/veritas/veritastest"
)

// syncStorage drives a StorageModel synchronously, with no injected failure,
// as a veritastest.FSUnderTest.
type syncStorage struct{ fs *StorageModel }

func (s syncStorage) Exec(a control.Actuation) control.Feedback {
	return s.fs.Land(a, ActuationPlan{}).(control.Feedback)
}

func newSyncStorage() veritastest.FSUnderTest {
	return syncStorage{NewStorageModel(NewRand(1), Disturbances{}, veritastest.FSDirs()...)}
}

// TestC2StorageModelMatchesVeritasFS is conformance check C2: the storage
// model with no disturbances (no faults, no crashes) agrees with veritas.FS on
// every storage command. Flags: -seeds N, -seed S (replay one seed),
// -duration D, as for TestSeeds.
func TestC2StorageModelMatchesVeritasFS(t *testing.T) {
	o := veritastest.Options{
		Seeds: *flagSeeds,
		Seed:  *flagSeed,
		Replay: func(seed uint64) string {
			return fmt.Sprintf("go test ./toys3/sim -run TestC2StorageModelMatchesVeritasFS -seed=%d -v", seed)
		},
	}
	if *flagDuration > 0 {
		start := time.Now()
		o.Stop = func() bool { return time.Since(start) >= *flagDuration }
	}
	veritastest.CheckFS(t, newSyncStorage, o)
}
