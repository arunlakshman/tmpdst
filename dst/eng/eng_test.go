package eng_test

import (
	"errors"
	"testing"

	"github.com/manifesterio/mds/dst/eng"
)

// Doc-level smoke: ErrNotMine is the sentinel engines return for foreign acts.
func TestErrNotMine(t *testing.T) {
	if !errors.Is(eng.ErrNotMine, eng.ErrNotMine) {
		t.Fatal("ErrNotMine identity")
	}
	err := errors.Join(eng.ErrNotMine, errors.New("wrap"))
	if !errors.Is(err, eng.ErrNotMine) {
		t.Fatal("ErrNotMine must match through Join")
	}
}
