package broker

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
)

// conn is one client connection. Kafka answers a connection's requests in
// request order even when they are pipelined, so each request owns a slot in q
// and only the head is sent, once it is ready.
type conn struct {
	id      core.ConnID
	fr      *kwire.Framer
	rx      hasher // running hash of every byte received (a partial frame is state)
	q       []*slot
	paused  bool // PauseReads is in force
	closing bool // no more requests are read; graceful close once q drains
	abort   bool // close with RST
	closed  bool // CloseConn issued or peer gone; answers are dropped
}

// slot is one request awaiting its response.
type slot struct {
	conn    *conn
	h       kwire.RequestHeader
	resp    core.Response
	pending int  // parts still outstanding, plus 1 while the handler runs
	ready   bool // resp is complete
	silent  bool // no response is sent (acks=0)
	facts   []core.Fact
	// fetchMax is the Fetch request's total byte limit (<= 0: none).
	fetchMax int32
}

func (b *Broker) findConn(id core.ConnID) *conn {
	for _, c := range b.conns {
		if c.id == id {
			return c
		}
	}
	return nil
}

func (b *Broker) dropConn(c *conn) {
	c.closed = true
	for i, x := range b.conns {
		if x == c {
			b.conns = append(b.conns[:i:i], b.conns[i+1:]...)
			return
		}
	}
}

func (b *Broker) onOpen(e core.ConnOpened) []core.Action {
	if b.stopping || b.findConn(e.Conn) != nil {
		return []core.Action{core.CloseConn{Conn: e.Conn, Abort: true}}
	}
	b.conns = append(b.conns, &conn{id: e.Conn, fr: kwire.NewFramer(b.cfg.MaxFrame)})
	return nil
}

func (b *Broker) onClosed(e core.ConnClosed) []core.Action {
	c := b.findConn(e.Conn)
	if c == nil {
		return nil
	}
	b.dropConn(c)
	if !e.Abort {
		return []core.Action{core.CloseConn{Conn: c.id}}
	}
	return nil
}

func (b *Broker) onBytes(e core.BytesReceived) []core.Action {
	c := b.findConn(e.Conn)
	if c == nil || c.closing {
		return nil
	}
	c.rx.bytes(e.Data)
	frames, err := c.fr.Push(e.Data)
	var acts []core.Action
	for _, f := range frames {
		if c.closing {
			break
		}
		acts = append(acts, b.request(c, f)...)
	}
	if err != nil {
		c.closing = true
	}
	b.dirty = append(b.dirty, c)
	return acts
}

// request decodes one frame and routes it.
func (b *Broker) request(c *conn, frame []byte) []core.Action {
	h, req, err := b.codec.DecodeRequest(frame)
	var ue *kwire.UnsupportedError
	switch {
	case errors.As(err, &ue) && h.APIKey == kwire.KeyApiVersions:
		s := b.newSlot(c, h)
		s.resp = kwire.ApiVersionsResponse(h)
		b.finishPart(s)
		return nil
	case err != nil:
		c.closing = true
		return nil
	}
	if req.Key() == kwire.KeyApiVersions {
		s := b.newSlot(c, h)
		s.resp = kwire.ApiVersionsResponse(h)
		b.finishPart(s)
		return nil
	}
	if b.stopping {
		c.closing = true
		return nil
	}
	s := b.newSlot(c, h)
	if !b.registered {
		b.parked = append(b.parked, parked{s, req})
		return nil
	}
	return b.handle(s, req)
}

// newSlot reserves the next response position on c. pending starts at 1, the
// guard released by handle (or finishPart) once the handler has queued all its
// parts.
func (b *Broker) newSlot(c *conn, h kwire.RequestHeader) *slot {
	s := &slot{conn: c, h: h, pending: 1}
	c.q = append(c.q, s)
	return s
}

// finishPart retires one pending part; at zero the response is ready.
func (b *Broker) finishPart(s *slot) {
	if s.pending--; s.pending > 0 {
		return
	}
	b.finalize(s)
	s.ready = true
	b.dirty = append(b.dirty, s.conn)
}

// finalize applies request-wide limits before the response is encoded.
func (b *Broker) finalize(s *slot) {
	r, ok := s.resp.(*kmsg.FetchResponse)
	if !ok || s.fetchMax <= 0 {
		return
	}
	total := 0
	for i := range r.Topics {
		for j := range r.Topics[i].Partitions {
			p := &r.Topics[i].Partitions[j]
			if total > 0 && total+len(p.RecordBatches) > int(s.fetchMax) {
				p.RecordBatches = nil
				continue
			}
			total += len(p.RecordBatches)
		}
	}
}

// drainDirty sends every ready head slot, in order per connection, and closes
// connections that are done.
func (b *Broker) drainDirty(acts []core.Action) []core.Action {
	for i := 0; i < len(b.dirty); i++ {
		acts = b.drain(b.dirty[i], acts)
	}
	b.dirty = b.dirty[:0]
	return acts
}

func (b *Broker) drain(c *conn, acts []core.Action) []core.Action {
	for len(c.q) > 0 && c.q[0].ready {
		s := c.q[0]
		c.q = c.q[1:]
		if c.closed || s.silent {
			continue
		}
		data, err := b.codec.EncodeResponse(s.h, s.resp)
		if err != nil {
			c.closing, c.abort = true, true
			continue
		}
		// Ack facts go immediately before the Send that carries them (decision 12).
		for _, f := range s.facts {
			acts = append(acts, core.Emit{Fact: f})
		}
		acts = append(acts, core.Send{Req: b.req(), Conn: c.id, Data: data})
	}
	if c.closing && !c.closed && (len(c.q) == 0 || c.abort) {
		b.dropConn(c)
		acts = append(acts, core.CloseConn{Conn: c.id, Abort: c.abort})
	}
	return acts
}
