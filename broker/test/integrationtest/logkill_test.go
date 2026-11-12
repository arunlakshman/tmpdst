package integrationtest

import "testing"

// Mid-produce control-plane kill used an in-process log CP (replica/server/dynamolog).
// That server now lives in mds-controlplane; reconnect this test against an external CP.
func TestKillTheAppenderMidProduce(t *testing.T) {
	t.Skip("control plane extracted to mds-controlplane; reconnect later")
}
