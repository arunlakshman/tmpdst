// Package truth names what a rule may ask the fake outside systems: what the
// object store really holds, and what the control plane really committed. The
// answers use plain types only (an anti-corruption layer), so a rule never
// reads the state of a fake directly.
//
// The fake object store and the fake control plane have these methods. A rule
// gets a typed value when the host builds it. A test can give a fake of its own.
package truth

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
)

// ObjectStore is what the object store really holds. Iteration is in sorted key
// order.
type ObjectStore interface {
	Object(key string) ([]byte, bool)
	EachObject(fn func(key string, data []byte))
	// Gen grows whenever anything applied changes, so a rule can skip steps.
	Gen() uint64
}

// ControlPlane is what the control plane really committed.
type ControlPlane interface {
	Committed(topicPartition core.TopicPartition) []contract.BatchRange
	HighWatermark(topicPartition core.TopicPartition) core.Offset
	// Gen grows whenever anything applied changes.
	Gen() uint64
}
