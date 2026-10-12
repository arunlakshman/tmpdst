package sim

import (
	"testing"

	"manifester/toys3/control"
)

// TestTelemetryHashKeyFrozen pins the hash encoding of telemetry. If this fails, a
// change altered every trace hash: see telemetryHashKey.
func TestTelemetryHashKeyFrozen(t *testing.T) {
	for _, c := range []struct {
		f    control.Telemetry
		want string
	}{
		{control.Committed{Client: 7, Key: "k1", Sum: 0xbeef}, `Committed{c7 "k1" crc=0000beef}`},
		{control.Committed{Client: 8, Key: "k2", Deleted: true}, `Committed{c8 delete "k2"}`},
		{control.Recovered{TempsRemoved: 3}, `Recovered{temps=3}`},
		{control.CorruptionDetected{Key: "k3"}, `CorruptObject{"k3"}`},
	} {
		if got := telemetryHashKey(c.f); got != c.want {
			t.Errorf("telemetryHashKey(%#v) = %s, want %s", c.f, got, c.want)
		}
	}
}
