package check

import "fmt"

// The helpers in this file came from dst/model. They check the two crash
// properties of the Bornholt et al. paper (SOSP 2021, section 5) by hand,
// for a test that does not use Lockstep:
//
//   - persistence: every write that was answered before a crash is still there
//     after it;
//   - forward progress: after a clean restart, the subject answers like the spec.

// Write is one answered key and value, for Persistence.
type Write[K, V any] struct {
	Key   K
	Value V
}

// Persistence checks that every answered write is still present after a
// crash and a restart. present(key, value) reports whether the durable state
// has that write. answered is the list of writes the client saw succeed
// before the crash.
func Persistence[K, V any](answered []Write[K, V], present func(K, V) bool) error {
	for i, w := range answered {
		if !present(w.Key, w.Value) {
			return fmt.Errorf("persistence: answered write %d is missing", i)
		}
	}
	return nil
}

// ForwardProgress compares the answer of the spec (specClean) with the
// answer of the subject after a clean restart (subjectRestart). equal says
// whether they match.
func ForwardProgress[Res any](specClean, subjectRestart func() (Res, error), equal func(want, got Res) error) error {
	want, err := specClean()
	if err != nil {
		return fmt.Errorf("spec clean: %w", err)
	}
	got, err := subjectRestart()
	if err != nil {
		return fmt.Errorf("subject restart: %w", err)
	}
	if err := equal(want, got); err != nil {
		return fmt.Errorf("forward progress: %w", err)
	}
	return nil
}

// DirtyReboot runs both crash checks: first Persistence, then
// ForwardProgress.
func DirtyReboot[K, V, Res any](
	answered []Write[K, V],
	present func(K, V) bool,
	specClean, subjectRestart func() (Res, error),
	equal func(want, got Res) error,
) error {
	if err := Persistence(answered, present); err != nil {
		return err
	}
	return ForwardProgress(specClean, subjectRestart, equal)
}
