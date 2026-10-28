package core

import "testing"

type sliceQ []EventData

func (q *sliceQ) Len() int { return len(*q) }
func (q *sliceQ) Pop() (EventData, bool) {
	if len(*q) == 0 {
		return nil, false
	}
	b := (*q)[0]
	*q = (*q)[1:]
	return b, true
}

func TestControlFirst(t *testing.T) {
	c := &sliceQ{TimerFired{ID: 1}, TimerFired{ID: 2}}
	d := &sliceQ{ConnOpened{Conn: 1}}
	var p InboxPolicy = ControlFirst{}
	for _, want := range []EventData{TimerFired{ID: 1}, TimerFired{ID: 2}, ConnOpened{Conn: 1}} {
		got, ok := p.Next(c, d)
		if !ok || got != want {
			t.Fatalf("got %v,%v want %v", got, ok, want)
		}
	}
	if _, ok := p.Next(c, d); ok {
		t.Fatal("empty queues must give ok=false")
	}
}
