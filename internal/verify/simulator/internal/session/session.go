// Package session is the connection loop shared by the scenario clients: dial
// the brokers in rotation, back off on failure, time out the one outstanding
// request, and reconnect when the connection is lost. The protocol spoken over
// the connection belongs to the scenario, through the Loop hooks.
package session

import "mds/internal/core"

// State is a client's connection state. A scenario may define connected
// substates of its own at Connected and above.
type State uint8

// The connection states the loop drives.
const (
	WaitingToRetry State = iota // wake timer pending, then dial
	Connecting
	Connected
)

// Client is one client's connection state. A scenario embeds it in its client.
type Client struct {
	ID      int
	State   State
	Conn    core.ConnID
	DialReq core.RequestID
	SendReq core.RequestID
	Dials   int
	Fails   int // consecutive dial/connection failures
	WakeGen core.TimerVersion
	ReqGen  core.TimerVersion
}

func (c *Client) session() *Client { return c }

func (c *Client) wakeID() core.TimerID    { return core.TimerID(1 + c.ID*2) }
func (c *Client) requestID() core.TimerID { return core.TimerID(2 + c.ID*2) }

// Wake arms the client's wake timer, replacing any pending one.
func (c *Client) Wake(d core.Duration) core.Action {
	c.WakeGen++
	return core.SetTimer{ID: c.wakeID(), Gen: c.WakeGen, After: d}
}

// Answered settles the outstanding request, so its timeout no longer fires.
func (c *Client) Answered() core.Action {
	c.ReqGen++
	return core.CancelTimer{ID: c.requestID()}
}

func (c *Client) backoff() core.Action {
	c.Fails++
	c.State = WaitingToRetry
	return c.Wake(RetryWaitTime(c.Fails))
}

// RetryWaitTime is the backoff after n consecutive failures.
func RetryWaitTime(n int) core.Duration {
	if n > 4 {
		n = 4
	}
	return 50 * core.Millisecond << n
}

type member interface{ session() *Client }

// Loop drives Clients over Brokers. C is the scenario's client, which embeds
// Client. The hooks are the scenario's protocol and must all be set.
type Loop[C member] struct {
	Brokers []core.NodeID
	Clients []C
	NextReq core.RequestID

	// Connected reports that c's connection delivers its events to c.
	Connected func(c C) bool
	// Busy reports that c has a request outstanding.
	Busy func(c C) bool
	// Ready runs when c's dial succeeds, with Conn set and State Connected.
	Ready func(c C) []core.Action
	// Idle runs when c's wake timer fires while connected and not busy.
	Idle func(c C) []core.Action
	// Bytes handles data received on c's connection.
	Bytes func(c C, data []byte) []core.Action
	// Lost settles c's protocol state when its connection is lost, before
	// the loop schedules the reconnect.
	Lost func(c C, why string)
}

func (l *Loop[C]) nextRequestID() core.RequestID { l.NextReq++; return l.NextReq }

// Step handles the connection events of ev.
func (l *Loop[C]) Step(event core.Event) []core.Action {
	var acts []core.Action
	switch e := event.Body.(type) {
	case core.NodeStarted:
		for _, c := range l.Clients {
			acts = append(acts, l.Dial(c)...)
		}
	case core.TimerFired:
		acts = l.onTimer(e)
	case core.ConnectDone:
		for _, c := range l.Clients {
			if s := c.session(); s.State == Connecting && s.DialReq == e.Req {
				acts = l.onDial(c, e)
			}
		}
	case core.BytesReceived:
		if c, ok := l.byConn(e.Conn); ok {
			acts = l.Bytes(c, e.Data)
		}
	case core.ConnClosed:
		if c, ok := l.byConn(e.Conn); ok {
			acts = l.lost(c, "conn_closed", nil)
		}
	case core.SendDone:
		if c, ok := l.byConn(e.Conn); ok && e.Err != core.OK && c.session().SendReq == e.Req {
			acts = l.Abort(c, "send_failed")
		}
	}
	return acts
}

func (l *Loop[C]) byConn(id core.ConnID) (C, bool) {
	for _, c := range l.Clients {
		if c.session().Conn == id && id != 0 && l.Connected(c) {
			return c, true
		}
	}
	var none C
	return none, false
}

// Dial connects c to the next broker in its rotation.
func (l *Loop[C]) Dial(c C) []core.Action {
	s := c.session()
	s.State, s.DialReq = Connecting, l.nextRequestID()
	addr := string(l.Brokers[(s.ID+s.Dials)%len(l.Brokers)])
	s.Dials++
	return []core.Action{core.Connect{Req: s.DialReq, Addr: addr}}
}

func (l *Loop[C]) onDial(c C, e core.ConnectDone) []core.Action {
	s := c.session()
	s.DialReq = 0
	if e.Err != core.OK {
		return []core.Action{s.backoff()}
	}
	s.Conn, s.State = e.Conn, Connected
	return l.Ready(c)
}

func (l *Loop[C]) onTimer(e core.TimerFired) []core.Action {
	for _, c := range l.Clients {
		s := c.session()
		switch e.ID {
		case s.wakeID():
			if e.Gen != s.WakeGen {
				return nil
			}
			switch {
			case s.State == WaitingToRetry:
				return l.Dial(c)
			case l.Connected(c) && !l.Busy(c):
				return l.Idle(c)
			}
			return nil
		case s.requestID():
			if e.Gen != s.ReqGen || !l.Busy(c) {
				return nil
			}
			return l.Abort(c, "timeout")
		}
	}
	return nil
}

// Request sends data on c's connection and starts its timeout.
func (l *Loop[C]) Request(c C, data []byte, timeout core.Duration) []core.Action {
	s := c.session()
	s.SendReq = l.nextRequestID()
	s.ReqGen++
	return []core.Action{
		core.Send{Req: s.SendReq, Conn: s.Conn, Data: data},
		core.SetTimer{ID: s.requestID(), Gen: s.ReqGen, After: timeout},
	}
}

// Abort drops c's connection as unusable and schedules the reconnect.
func (l *Loop[C]) Abort(c C, why string) []core.Action {
	return l.lost(c, why, []core.Action{core.CloseConn{Conn: c.session().Conn, Abort: true}})
}

// lost handles a dead or unusable connection: whatever was outstanding has an
// unknown fate, and the client reconnects (to the next broker) with backoff.
func (l *Loop[C]) lost(c C, why string, acts []core.Action) []core.Action {
	l.Lost(c, why)
	s := c.session()
	s.Conn = 0
	s.ReqGen++
	return append(acts, core.CancelTimer{ID: s.requestID()}, s.backoff())
}
