package controlplane_test

import (
	"testing"

	cp "manifester/controlplane"
	"manifester/core"
)

// ParseErrCode inverts String for every live code and skips the retired slot
// (the old ErrFenced, decision 44) and spellings that are not codes.
func TestParseErrCode(t *testing.T) {
	for c := core.OK; c <= core.ErrIO; c++ {
		got, ok := cp.ParseErrCode(c.String())
		if retired := c.String() == "ErrCode(5)"; ok == retired || (ok && got != c) {
			t.Errorf("%v: parsed %v, %v", c, got, ok)
		}
	}
	for _, s := range []string{"ErrFenced", "ErrOutOfOrder", "ErrCode(5)", "ErrCode(99)", "nope", ""} {
		if c, ok := cp.ParseErrCode(s); ok {
			t.Errorf("%q parsed as %v", s, c)
		}
	}
}
