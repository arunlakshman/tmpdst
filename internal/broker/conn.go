package broker

import (
	"errors"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
	"mds/internal/wire/kafkawire"
)

// connection is one client connection. Kafka answers a connection's requests in
// request order even when they are pipelined, so each request owns a slot in q
// and only the head is sent, once it is ready.
type connection struct {
	id      core.ConnID
	fr      *kafkawire.Framer
	rx      stateHasher // running hash of every byte received (a partial frame is state)
	q       []*slot
	paused  bool   // PauseReads is in force
	closing bool   // no more requests are read; graceful close once q drains
	abort   bool   // close with RST
	closed  bool   // CloseConn issued or peer gone; answers are dropped
	host    string // client host, from the peer address (JoinGroup's ClientHost)
}

// slot is one request awaiting its response.
type slot struct {
	conn    *connection
	h       kafkawire.RequestHeader
	resp    core.Response
	pending int  // parts still outstanding, plus 1 while the handler runs
	ready   bool // resp is complete
	silent  bool // no response is sent (acks=0)
	facts   []core.Fact
	// fetchMax is the Fetch request's total byte limit (<= 0: none).
	fetchMax int32
	// Fetch long poll (fetch.go): the Fetch waits for fetchMin bytes until
	// fetchUntil. fetchGot counts the bytes its ended parts returned.
	fetchMin   int32
	fetchUntil core.NodeTime
	fetchGot   int
}

func (b *Broker) findConn(id core.ConnID) *connection {
	for _, c := range b.conns {
		if c.id == id {
			return c
		}
	}
	return nil
}

func (b *Broker) dropConn(c *connection) {
	c.closed = true
	for i, x := range b.conns {
		if x == c {
			b.conns = append(b.conns[:i:i], b.conns[i+1:]...)
			return
		}
	}
}

func (b *Broker) addConn(c *connection) { b.conns = append(b.conns, c) }

func (b *Broker) onOpen(e core.ConnOpened) []core.Action {
	if b.stopping || b.findConn(e.Conn) != nil {
		return []core.Action{core.CloseConn{Conn: e.Conn, Abort: true}}
	}
	b.addConn(&connection{id: e.Conn, fr: kafkawire.NewFramer(b.cfg.MaxFrame), host: peerHost(e.Peer)})
	return nil
}

func (b *Broker) onClosed(e core.ConnClosed) []core.Action {
	c := b.findConn(e.Conn)
	if c == nil {
		return nil
	}
	b.dropConn(c)
	b.endParked(func(o *operation) bool { return o.s.conn == c })
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

// peerHost is the host part of a peer address "host:port". Kafka shows a
// client host with a leading slash, so it is returned that way.
func peerHost(peer string) string {
	if i := strings.LastIndexByte(peer, ':'); i >= 0 {
		peer = peer[:i]
	}
	return "/" + peer
}

// request decodes one frame and routes it.
func (b *Broker) request(c *connection, frame []byte) []core.Action {
	h, req, err := b.codec.DecodeRequest(frame)
	if resp, ok := apiVersionsReply(h, req, err); ok {
		s := b.newSlot(c, h)
		s.resp = resp
		b.finishPart(s)
		return nil
	}
	if err != nil || b.stopping {
		c.closing = true
		return nil
	}
	s := b.newSlot(c, h)
	return b.handle(s, req)
}

// apiVersionsReply is the one ApiVersions answer. An unsupported version of
// that key still gets the supported range. Every other decode error does not.
func apiVersionsReply(h kafkawire.RequestHeader, req core.Request, err error) (*kmsg.ApiVersionsResponse, bool) {
	if _, ok := errors.AsType[*kafkawire.UnsupportedError](err); ok && h.APIKey == kafkawire.KeyApiVersions {
		return kafkawire.ApiVersionsResponse(h), true
	}
	if err == nil && req.Key() == kafkawire.KeyApiVersions {
		return kafkawire.ApiVersionsResponse(h), true
	}
	return nil, false
}

// newSlot reserves the next response position on c. pending starts at 1, the
// guard released by handle (or finishPart) once the handler has queued all its
// parts.
func (b *Broker) newSlot(c *connection, h kafkawire.RequestHeader) *slot {
	s := &slot{conn: c, h: h, pending: 1}
	c.q = append(c.q, s)
	return s
}

// finishPart retires one pending part. The last part marks the slot ready.
func (b *Broker) finishPart(s *slot) {
	s.pending--
	if s.pending > 0 {
		return
	}
	b.readySlot(s)
}

func (b *Broker) readySlot(s *slot) {
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

func (b *Broker) drain(c *connection, acts []core.Action) []core.Action {
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
			acts = append(acts, core.RecordFact{Fact: f})
		}
		acts = append(acts, core.Send{Req: b.nextRequestID(), Conn: c.id, Data: data})
	}
	if c.closing && !c.closed && (len(c.q) == 0 || c.abort) {
		b.dropConn(c)
		acts = append(acts, core.CloseConn{Conn: c.id, Abort: c.abort})
	}
	return acts
}
