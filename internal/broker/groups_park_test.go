package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// A JoinGroup waits at the control plane (holdCP). It parks its own
// connection only: another connection is answered at once, and the answers
// of one connection keep their request order.
func TestParkedJoinDoesNotBlockOtherConnections(t *testing.T) {
	h, _ := withGroupCP(t, DefaultConfig(), func(operation core.ControlPlaneOperation) core.ControlPlaneDone {
		if _, ok := operation.(core.GroupJoin); ok {
			return cpOK(core.GroupJoinResp{MemberID: "m-1", Leader: "m-1", Generation: 1})
		}
		return cpOK(nil)
	})
	h.holdCP = true
	c1, c2 := h.open(1), h.open(2)

	// Conn 1: a join (held at the control plane), then a heartbeat that must wait behind it.
	h.send(c1, 7, joinRequest("g1", 10000, 30000))
	h.send(c1, 4, heartbeatRequest("g1", "m-1", 1))
	if len(c1.out) != 0 {
		t.Fatalf("conn 1 answered while its join is parked: %d frames", len(c1.out))
	}

	// Conn 2 is served now (ApiVersions is answered without the control plane).
	if v := h.call(c2, 0, kmsg.NewPtrApiVersionsRequest()); v == nil {
		t.Fatal("conn 2 not answered")
	}

	// Release the control plane: the join answer goes first, then the heartbeat.
	h.releaseHeld()
	j := h.next(c1).(*kmsg.JoinGroupResponse)
	hb := h.next(c1).(*kmsg.HeartbeatResponse)
	if j.ErrorCode != 0 || j.MemberID != "m-1" || hb.ErrorCode != 0 {
		t.Fatalf("join %+v, heartbeat %+v", j, hb)
	}
	h.noViolations()
}

// A client that closes while its join is parked gets no answer, and the
// broker does not fail. The late control plane answer is dropped.
func TestJoinAnsweredAfterConnectionCloseIsDropped(t *testing.T) {
	h, _ := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.GroupJoinResp{MemberID: "m-1", Leader: "m-1"})
	})
	h.holdCP = true
	c := h.open(1)
	h.send(c, 7, joinRequest("g1", 10000, 30000))
	h.do(core.ConnClosed{Conn: 1})
	h.releaseHeld()
	if len(c.out) != 0 {
		t.Fatalf("%d frames sent to a closed connection", len(c.out))
	}
	if len(h.b.ops) != 0 {
		t.Fatalf("%d operations left after the close", len(h.b.ops))
	}
	h.noViolations()
}

// A stop answers the group calls still at the control plane as unavailable,
// and the broker exits. The late answer from the control plane is ignored.
func TestStopAnswersParkedGroupCalls(t *testing.T) {
	h, _ := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.GroupJoinResp{MemberID: "m-1"})
	})
	h.holdCP = true
	c := h.open(1)
	h.send(c, 7, joinRequest("g1", 10000, 30000))
	h.do(core.StopRequest{Deadline: h.now + 1})
	j := h.next(c).(*kmsg.JoinGroupResponse)
	if kafkaErrorCode(j.ErrorCode) != errCoordNotAvailable {
		t.Fatalf("join during stop: %d, want 15", j.ErrorCode)
	}
	if !h.exited {
		t.Fatal("broker did not exit after the parked join was answered")
	}
	h.releaseHeld() // the late answer: nothing happens
	h.noViolations()
}
