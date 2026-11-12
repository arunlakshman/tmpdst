package replica

import "testing"

// The names appear in the checkpoint_decision log line, so they must not change by accident.
func TestPendingKindNames(t *testing.T) {
	cases := map[pendingKind]string{
		pendingNone:     "none",
		pendingTail:     "tail_check",
		pendingRead:     "log_read",
		pendingObjGet:   "checkpoint_get",
		pendingKind(99): "none",
	}
	for kind, want := range cases {
		if got := kind.String(); got != want {
			t.Errorf("pendingKind(%d).String() = %q, want %q", kind, got, want)
		}
	}
}
