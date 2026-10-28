package core

import "testing"

func TestLogIOCallsAreIOCalls(t *testing.T) {
	acts := []IOCall{
		LogAppend{Req: 1, Slot: 2, Data: []byte("x")},
		LogRead{Req: 3, From: 0, Limit: 10},
		LogGetEnd{Req: 4},
		LogDeleteOld{Req: 5, Through: 1},
	}
	for _, a := range acts {
		if a.RequestID() == 0 {
			t.Fatalf("%T: RequestID zero", a)
		}
	}
	dones := []IOResult{
		LogAppendDone{Req: 1, Slot: 2, Outcome: Applied},
		LogReadDone{Req: 3, Entries: []LogEntry{{Slot: 0, Data: []byte("x")}}, Outcome: Applied},
		LogGetEndDone{Req: 4, Tail: 1, Outcome: Applied},
		LogDeleteOldDone{Req: 5, Outcome: Applied},
	}
	for _, d := range dones {
		if InboxKindOf(d) != Control {
			t.Fatalf("%T: InboxKindOf = %v, want Control", d, InboxKindOf(d))
		}
		if d.RequestID() == 0 {
			t.Fatalf("%T: RequestID zero", d)
		}
	}
}

func TestCPCallAndCPDoneClassOf(t *testing.T) {
	call := ControlPlaneCall{Req: 9, Op: FindSegments{TP: NewTopicPartition("t", 1), From: 7}}
	if call.RequestID() != 9 {
		t.Fatalf("RequestID = %d", call.RequestID())
	}
	done := ControlPlaneDone{Req: 9, Outcome: NotApplied, Err: ErrUnavailable}
	if InboxKindOf(done) != Control || done.RequestID() != 9 {
		t.Fatalf("CPDone class/req broken")
	}
	offer := ControlPlaneDeliver{From: NodeInstance{Node: "b-0"}, Req: 9, Op: call.Op}
	if InboxKindOf(offer) != Control {
		t.Fatalf("CPOffer class = %v", InboxKindOf(offer))
	}
	_ = ControlPlaneReply{To: offer.From, Done: done}
}
