package session

import (
	"testing"

	"manifester/internal/core"
)

type wrap struct{ Client }

func (w *wrap) session() *Client { return &w.Client }

func TestRetryDelay(t *testing.T) {
	t.Parallel()
	if RetryWaitTime(1) != 100*core.Millisecond {
		t.Fatalf("RetryDelay(1) = %d", RetryWaitTime(1))
	}
	if RetryWaitTime(99) != RetryWaitTime(4) {
		t.Fatal("RetryDelay does not cap at 4")
	}
}

func TestLoopDialFailAndReady(t *testing.T) {
	t.Parallel()
	c := &wrap{Client: Client{ID: 0}}
	var ready, lost int
	l := Loop[*wrap]{
		Brokers:   []core.NodeID{"b0", "b1"},
		Clients:   []*wrap{c},
		Connected: func(c *wrap) bool { return c.State == Connected },
		Busy:      func(c *wrap) bool { return false },
		Ready: func(c *wrap) []core.Action {
			ready++
			return nil
		},
		Idle:  func(*wrap) []core.Action { return nil },
		Bytes: func(*wrap, []byte) []core.Action { return nil },
		Lost:  func(*wrap, string) { lost++ },
	}

	acts := l.Step(core.Event{Body: core.NodeStarted{}})
	if len(acts) != 1 {
		t.Fatalf("Started acts = %#v", acts)
	}
	d, ok := acts[0].(core.Connect)
	if !ok || c.State != Connecting {
		t.Fatalf("dial = %#v state %d", acts[0], c.State)
	}

	acts = l.Step(core.Event{Body: core.ConnectDone{Req: d.Req, Err: core.ErrUnavailable}})
	if c.State != WaitingToRetry || len(acts) != 1 {
		t.Fatalf("dial fail state=%d acts=%#v", c.State, acts)
	}

	acts = l.Step(core.Event{Body: core.TimerFired{ID: c.wakeID(), Gen: c.WakeGen}})
	d, ok = acts[0].(core.Connect)
	if !ok {
		t.Fatalf("redial = %#v", acts)
	}
	_ = l.Step(core.Event{Body: core.ConnectDone{Req: d.Req, Conn: 7}})
	if c.State != Connected || c.Conn != 7 || ready != 1 {
		t.Fatalf("ready state=%d conn=%d ready=%d", c.State, c.Conn, ready)
	}

	_ = l.Step(core.Event{Body: core.ConnClosed{Conn: 7}})
	if c.State != WaitingToRetry || lost != 1 {
		t.Fatalf("closed state=%d lost=%d", c.State, lost)
	}
}

func TestLoopRequestTimeout(t *testing.T) {
	t.Parallel()
	c := &wrap{Client: Client{ID: 1, State: Connected, Conn: 3}}
	busy := true
	l := Loop[*wrap]{
		Brokers:   []core.NodeID{"b0"},
		Clients:   []*wrap{c},
		Connected: func(*wrap) bool { return true },
		Busy:      func(*wrap) bool { return busy },
		Ready:     func(*wrap) []core.Action { return nil },
		Idle:      func(*wrap) []core.Action { return nil },
		Bytes:     func(*wrap, []byte) []core.Action { return nil },
		Lost:      func(*wrap, string) { busy = false },
	}
	acts := l.Request(c, []byte("x"), core.Second)
	if len(acts) != 2 {
		t.Fatalf("Request acts = %#v", acts)
	}
	tm := acts[1].(core.SetTimer)
	acts = l.Step(core.Event{Body: core.TimerFired{ID: tm.ID, Gen: tm.Gen}})
	if c.State != WaitingToRetry || len(acts) < 1 {
		t.Fatalf("timeout state=%d acts=%#v", c.State, acts)
	}
}

func TestLoopIdleAndBytes(t *testing.T) {
	t.Parallel()
	c := &wrap{Client: Client{ID: 2, State: Connected, Conn: 9}}
	var idle, bytes int
	l := Loop[*wrap]{
		Brokers:   []core.NodeID{"b0"},
		Clients:   []*wrap{c},
		Connected: func(c *wrap) bool { return c.State == Connected },
		Busy:      func(*wrap) bool { return false },
		Ready:     func(*wrap) []core.Action { return nil },
		Idle: func(*wrap) []core.Action {
			idle++
			return nil
		},
		Bytes: func(_ *wrap, data []byte) []core.Action {
			bytes += len(data)
			return nil
		},
		Lost: func(*wrap, string) {},
	}
	c.WakeGen = 1
	_ = l.Step(core.Event{Body: core.TimerFired{ID: c.wakeID(), Gen: 1}})
	if idle != 1 {
		t.Fatalf("idle = %d", idle)
	}
	_ = l.Step(core.Event{Body: core.BytesReceived{Conn: 9, Data: []byte("ab")}})
	if bytes != 2 {
		t.Fatalf("bytes = %d", bytes)
	}
}
