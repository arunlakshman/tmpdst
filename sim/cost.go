package sim

import "manifester/core"

// CostModel says how long a node's loop goroutine stays busy for one Step
// (docs/flow-control.md sec. 4.1). Deliveries go into the node's simulated
// control/data inboxes; the node picks with core.InboxPolicy and is busy for
// Sample(...) after each Step. Zero cost reproduces an instantaneous Step.
type CostModel interface {
	Sample(body core.EventBody, r CountedRand) core.Duration
}

// CostParams configures the default CostModel. Seeded, swarmable, and
// calibrated against production load tests.
type CostParams struct {
	PerEvent core.Duration // base cost of any Step
	PerByte  core.Duration // extra cost per BytesReceived byte (decode, copy)
	// Pause episodes stand in for Go GC stop-the-world and CPU contention:
	// with probability PauseProb a step takes an extra Pause sample.
	PauseProb float64
	Pause     TwoSpeed
}

// NodeInbox is the simulator's model of one node's two inboxes and its busy
// loop goroutine. Queue lengths and BusyUntil are folded into the state hash.
type NodeInbox interface {
	Control() core.Queue
	Data() core.Queue
	// Push routes with core.ClassOf.
	Push(body core.EventBody)
	BusyUntil() Time
}
