package core

import "testing"

func TestLogIOActionsAreIOActions(t *testing.T) {
	acts := []IOAction{
		LogAppend{Req: 1, Slot: 2, Data: []byte("x")},
		LogRead{Req: 3, From: 0, Limit: 10},
		LogCheckTail{Req: 4},
		LogTrim{Req: 5, Through: 1},
	}
	for _, a := range acts {
		if a.ReqID() == 0 {
			t.Fatalf("%T: ReqID zero", a)
		}
	}
	dones := []Completion{
		LogAppendDone{Req: 1, Slot: 2, Outcome: Applied},
		LogReadDone{Req: 3, Entries: []LogEntry{{Slot: 0, Data: []byte("x")}}, Outcome: Applied},
		LogCheckTailDone{Req: 4, Tail: 1, Outcome: Applied},
		LogTrimDone{Req: 5, Outcome: Applied},
	}
	for _, d := range dones {
		if ClassOf(d) != Control {
			t.Fatalf("%T: ClassOf = %v, want Control", d, ClassOf(d))
		}
		if d.ReqID() == 0 {
			t.Fatalf("%T: ReqID zero", d)
		}
	}
}

func TestCPCallAndCPDoneClassOf(t *testing.T) {
	call := CPCall{Req: 9, Op: Lookup{TP: TP("t", 1), From: 7}}
	if call.ReqID() != 9 {
		t.Fatalf("ReqID = %d", call.ReqID())
	}
	done := CPDone{Req: 9, Outcome: NotApplied, Err: ErrUnavailable}
	if ClassOf(done) != Control || done.ReqID() != 9 {
		t.Fatalf("CPDone class/req broken")
	}
	offer := CPOffer{From: NodeRef{Node: "b-0"}, Req: 9, Op: call.Op}
	if ClassOf(offer) != Control {
		t.Fatalf("CPOffer class = %v", ClassOf(offer))
	}
	_ = CPReply{To: offer.From, Done: done}
}
