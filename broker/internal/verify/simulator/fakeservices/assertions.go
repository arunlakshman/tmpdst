package fakeservices

import (
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/check/faulty"
)

// The assertions of the simulated services. Each one is declared once here, as a
// value of its own kind (Reachable, Sometimes), and the code uses the value. The
// declaration puts the name in the catalog of dst/assert. A name is never built at
// run time, and it must not change: reports and replays use it.
var (
	objectAsserts = faulty.Asserts{ //determinismcheck:ok catalog handles, set once at start
		Outage:      assert.NewReachable("obj outage started"),
		Unavailable: assert.NewReachable("obj 503 injected"),
		Timeout:     assert.NewReachable("obj timeout injected"),
		Lost:        assert.NewReachable("obj lost response injected"),
		Applied:     assert.NewSometimes("obj timed-out op applied"),
		NotApplied:  assert.NewSometimes("obj timed-out op not applied"),
	}
	controlPlaneAsserts = faulty.Asserts{ //determinismcheck:ok catalog handles, set once at start
		Outage:      assert.NewReachable("cp outage started"),
		Unavailable: assert.NewReachable("cp 503 injected"),
		Timeout:     assert.NewReachable("cp timeout injected"),
		Lost:        assert.NewReachable("cp lost response injected"),
		Applied:     assert.NewSometimes("cp timed-out op applied"),
		NotApplied:  assert.NewSometimes("cp timed-out op not applied"),
	}
	objectGetCorrupted = assert.NewReachable("obj get corrupted") //determinismcheck:ok catalog handle, set once at start
)
