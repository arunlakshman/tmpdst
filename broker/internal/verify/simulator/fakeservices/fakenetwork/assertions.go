package fakenetwork

import "github.com/manifesterio/mds/dst/assert"

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/assert. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	crashResetPeer  = assert.NewSometimes("net: crash reset a peer") //determinismcheck:ok catalog handle, set once at start
	partitionHealed = assert.NewSometimes("net: partition healed")   //determinismcheck:ok catalog handle, set once at start
)

// netFault is one kind of injected network fault. name is the name of the fault on
// the timeline. happened and injected are the two assertions that the kind records.
type netFault struct {
	name     string
	happened assert.Sometimes
	injected assert.Reachable
}

// The kinds of injected network fault.
var (
	faultPartition = netFault{ //determinismcheck:ok catalog handles, set once at start
		"partition", assert.NewSometimes("net: partition"), assert.NewReachable("net: partition injected"),
	}
	faultRandomClose = netFault{ //determinismcheck:ok catalog handles, set once at start
		"random_close", assert.NewSometimes("net: random_close"), assert.NewReachable("net: random_close injected"),
	}
	faultBlackHole = netFault{ //determinismcheck:ok catalog handles, set once at start
		"black_hole", assert.NewSometimes("net: black_hole"), assert.NewReachable("net: black_hole injected"),
	}
	faultClog = netFault{ //determinismcheck:ok catalog handles, set once at start
		"clog", assert.NewSometimes("net: clog"), assert.NewReachable("net: clog injected"),
	}
)
