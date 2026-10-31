package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// FindCoordinator (Kafka key 10, decision Q3). A group is coordinated by one
// live broker, picked by hashing its name. Only the connection moves: any
// broker serves every group call, so a stale answer is harmless.

// Key types of FindCoordinator.
const (
	coordGroup       int8 = 0
	coordTransaction int8 = 1
)

// findCoordinator answers FindCoordinator. Version 4 asks for many keys at once.
func (b *Broker) findCoordinator(s *slot, r *kmsg.FindCoordinatorRequest) []core.Action {
	resp := kmsg.NewPtrFindCoordinatorResponse()
	s.resp = resp
	if r.Version < 4 {
		addr, code := b.answerCoordinator(r.CoordinatorKey, r.CoordinatorType)
		resp.ErrorCode, resp.NodeID, resp.Host, resp.Port = int16(code), addr.NodeID, addr.Host, addr.Port
		return nil
	}
	for _, key := range r.CoordinatorKeys {
		addr, code := b.answerCoordinator(key, r.CoordinatorType)
		resp.Coordinators = append(resp.Coordinators, kmsg.FindCoordinatorResponseCoordinator{
			Key: key, NodeID: addr.NodeID, Host: addr.Host, Port: addr.Port, ErrorCode: int16(code)})
	}
	return nil
}

// answerCoordinator is the answer for one key: a broker, or an error with no
// broker (node -1). Transactions are not supported, so they get
// COORDINATOR_NOT_AVAILABLE.
func (b *Broker) answerCoordinator(key string, typ int8) (core.BrokerAddr, kafkaErrorCode) {
	none := core.BrokerAddr{NodeID: -1, Port: -1}
	switch {
	case typ == coordTransaction:
		return none, errCoordNotAvailable
	case typ != coordGroup:
		return none, errInvalidRequest
	case key == "":
		return none, errInvalidGroupID
	}
	return b.coordinatorOf(key), errNone
}

// coordinatorOf is the broker that coordinates group: the FNV-1a hash of the
// name, modulo the live list. The list is never empty, since it has this
// broker. With no other live broker, the answer is this broker.
func (b *Broker) coordinatorOf(group string) core.BrokerAddr {
	live := b.liveList()
	return live[fnv1a32(group)%uint32(len(live))]
}

// fnv1a32 is the 32 bit FNV-1a hash of s. Its value is fixed, so every broker
// picks the same coordinator for a name.
func fnv1a32(s string) uint32 {
	h := uint32(2166136261)
	for i := range len(s) {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// coordinatorIsSelf reports whether this broker coordinates group.
func (b *Broker) coordinatorIsSelf(group string) bool {
	return b.coordinatorOf(group).NodeID == b.cfg.BrokerID
}
