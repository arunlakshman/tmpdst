package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// heldHash runs do on a started broker whose control-plane replies are held,
// so the requests it sends stay in flight as operations. It returns the state
// hash and checks that at least one operation is in flight.
func heldHash(t *testing.T, do func(h *harness)) uint64 {
	t.Helper()
	h := startedHarness(t, DefaultConfig())
	h.holdCP = true
	do(h)
	if len(h.b.ops) == 0 {
		t.Fatal("no operation in flight")
	}
	return h.b.StateHash()
}

// TestStateHashInFlightOps: the hash covers the operations in flight. The same
// requests give the same hash, and a change in one operation changes it.
func TestStateHashInFlightOps(t *testing.T) {
	createOf := func(parts int32, validate bool) func(h *harness) {
		return func(h *harness) {
			c := h.open(1)
			h.send(c, verCreate, createReq(validate, newTopic("orders", parts, 1)))
		}
	}
	metadataOf := func(topic string) func(h *harness) {
		return func(h *harness) {
			c := h.open(1)
			r := kmsg.NewPtrMetadataRequest()
			r.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
			h.send(c, verMetadata, r)
		}
	}
	listTwice := func(h *harness) {
		c := h.open(1)
		h.send(c, 4, kmsg.NewPtrListGroupsRequest())
		h.send(c, 4, kmsg.NewPtrListGroupsRequest())
	}
	listOnce := func(h *harness) {
		c := h.open(1)
		h.send(c, 4, kmsg.NewPtrListGroupsRequest())
	}
	fetchOf := func(max int32) func(h *harness) {
		return func(h *harness) {
			c := h.open(1)
			h.send(c, verFetch, fetchReq(core.NewTopicPartition("t", 0), 0, max))
		}
	}

	tests := []struct {
		name    string
		same    func(h *harness)
		changed func(h *harness)
	}{
		{name: "create topic", same: createOf(2, false), changed: createOf(3, false)},
		{name: "validate only create", same: createOf(2, true), changed: createOf(2, false)},
		{name: "metadata", same: metadataOf("zzz"), changed: metadataOf("yyy")},
		{name: "group call", same: listOnce, changed: listTwice},
		{name: "fetch", same: fetchOf(100), changed: fetchOf(200)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := heldHash(t, tc.same)
			if b := heldHash(t, tc.same); a != b {
				t.Fatalf("same requests hash differently: %x vs %x", a, b)
			}
			if c := heldHash(t, tc.changed); c == a {
				t.Fatalf("changed operation kept hash %x", a)
			}
		})
	}
}
