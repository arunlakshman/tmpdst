package prod

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"manifester/internal/core"
	"manifester/internal/sys"
)

// ErrNoConn reports a Write or Close on a connection that is gone.
var ErrNoConn = errors.New("prod: no such connection")

// NetConfig tunes Net. Zero fields take the defaults.
type NetConfig struct {
	ReadBuf      int           // read chunk size (64 KiB)
	WriteTimeout time.Duration // per queued write (30s)
	LingerRead   time.Duration // how long a gracefully closed connection drains the peer's bytes (2s)
}

func (c NetConfig) withDefaults() NetConfig {
	if c.ReadBuf <= 0 {
		c.ReadBuf = 64 << 10
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.LingerRead <= 0 {
		c.LingerRead = 2 * time.Second
	}
	return c
}

// Net is the TCP side of a broker: an accept loop that delivers ConnOpened,
// one reader goroutine per connection (BytesReceived / ConnClosed to the data
// inbox, parked by PauseReads) and one writer goroutine per connection
// (writes in call order). It implements sys.Connections.
type Net struct {
	cfg  NetConfig
	sink sys.InboxWriter
	ln   net.Listener
	next atomic.Uint64
	wg   sync.WaitGroup

	mu     sync.Mutex
	conns  map[core.ConnID]*conn
	closed bool
}

var _ sys.Connections = (*Net)(nil)

// NewNet starts accepting on ln. ConnIDs are unique per process and start at 1.
func NewNet(ln net.Listener, sink sys.InboxWriter, cfg NetConfig) *Net {
	n := &Net{cfg: cfg.withDefaults(), sink: sink, ln: ln, conns: map[core.ConnID]*conn{}}
	n.wg.Add(1)
	go n.accept()
	return n
}

// OpenConns is the number of accepted connections not yet closed.
func (n *Net) OpenConns() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.conns)
}

// Addr is the listening address.
func (n *Net) Addr() net.Addr { return n.ln.Addr() }

func (n *Net) accept() {
	defer n.wg.Done()
	for {
		nc, err := n.ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Error("accept", "err", err)
			}
			return
		}
		c := newConn(n, core.ConnID(n.next.Add(1)), nc)
		n.mu.Lock()
		if n.closed {
			n.mu.Unlock()
			_ = nc.Close() // best-effort teardown
			return
		}
		n.conns[c.id] = c
		n.mu.Unlock()
		select {
		case <-n.sink.Closed():
			c.abort(true)
			n.forget(c)
			continue
		default:
		}
		n.sink.DeliverData(core.ConnOpened{Conn: c.id, Peer: nc.RemoteAddr().String()})
		n.wg.Add(2)
		go c.readLoop()
		go c.writeLoop()
	}
}

func (n *Net) get(id core.ConnID) *conn {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.conns[id]
}

func (n *Net) forget(c *conn) {
	n.mu.Lock()
	delete(n.conns, c.id)
	n.mu.Unlock()
}

// Write queues data behind earlier writes on conn; it never blocks.
func (n *Net) Write(id core.ConnID, data []byte) error {
	return n.WriteAsync(id, data, nil)
}

// WriteAsync is Write with done called (once, from the writer goroutine) when
// the bytes were written or the connection failed first.
func (n *Net) WriteAsync(id core.ConnID, data []byte, done func(error)) error {
	c := n.get(id)
	if c == nil {
		return ErrNoConn
	}
	return c.enqueue(item{data: data, done: done})
}

// Close ends conn: graceful flushes queued writes and then sends FIN; abort
// drops them and resets the connection.
func (n *Net) Close(id core.ConnID, abort bool) error {
	c := n.get(id)
	if c == nil {
		return ErrNoConn
	}
	if abort {
		c.abort(true)
		return nil
	}
	return c.enqueue(item{fin: true})
}

// PauseReads stops reading from the connection until ResumeReads is called.
func (n *Net) PauseReads(id core.ConnID) {
	if c := n.get(id); c != nil {
		c.setPaused(true)
	}
}

// ResumeReads resumes reading from a connection paused by PauseReads.
func (n *Net) ResumeReads(id core.ConnID) {
	if c := n.get(id); c != nil {
		c.setPaused(false)
	}
}

// Shutdown stops accepting, lets gracefully closed connections flush for up
// to grace, then resets whatever is left. It returns when every goroutine is
// gone.
func (n *Net) Shutdown(grace time.Duration) {
	_ = n.ln.Close() // best-effort teardown
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	done := make(chan struct{})
	go func() { n.wg.Wait(); close(done) }()
	select {
	case <-done:
		return
	case <-time.After(grace):
	}
	n.mu.Lock()
	left := make([]*conn, 0, len(n.conns))
	for _, c := range n.conns {
		left = append(left, c)
	}
	n.mu.Unlock()
	for _, c := range left {
		c.abort(true)
	}
	<-done
}

type item struct {
	data []byte
	fin  bool
	done func(error)
}

type conn struct {
	n  *Net
	id core.ConnID
	nc net.Conn

	mu      sync.Mutex
	wake    chan struct{} // cap 1: queue changed
	resume  chan struct{} // closed while reads are allowed
	q       []item
	paused  bool
	fin     bool // FIN queued: no more writes accepted
	dead    bool // aborted or failed: nothing is delivered any more
	local   bool // the core closed it: no more events for the core
	told    bool // ConnClosed was delivered
	stopped chan struct{}
}

func newConn(n *Net, id core.ConnID, nc net.Conn) *conn {
	c := &conn{n: n, id: id, nc: nc, wake: make(chan struct{}, 1), resume: make(chan struct{}), stopped: make(chan struct{})}
	close(c.resume)
	return c
}

func (c *conn) enqueue(it item) error {
	c.mu.Lock()
	if c.dead || c.fin {
		c.mu.Unlock()
		return ErrNoConn
	}
	c.fin = it.fin
	if it.fin {
		c.local = true
	}
	c.q = append(c.q, it)
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}

func (c *conn) setPaused(p bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p == c.paused {
		return
	}
	c.paused = p
	if p {
		c.resume = make(chan struct{})
	} else {
		close(c.resume)
	}
}

// report tells the core the connection is gone, once, unless the core closed it.
func (c *conn) report(abort bool) {
	c.mu.Lock()
	skip := c.local || c.told
	c.told = true
	c.mu.Unlock()
	if !skip {
		c.n.sink.DeliverData(core.ConnClosed{Conn: c.id, Abort: abort})
	}
}

// abort resets the connection (RST) and fails queued writes; byCore marks a
// close the core asked for.
func (c *conn) abort(byCore bool) {
	c.mu.Lock()
	if byCore {
		c.local = true
	}
	if c.dead {
		c.mu.Unlock()
		return
	}
	c.dead = true
	q := c.q
	c.q = nil
	if c.paused {
		c.paused = false
		close(c.resume)
	}
	c.mu.Unlock()
	if tc, ok := c.nc.(*net.TCPConn); ok {
		_ = tc.SetLinger(0) // best-effort teardown
	}
	_ = c.nc.Close() // best-effort teardown
	for _, it := range q {
		if it.done != nil {
			it.done(net.ErrClosed)
		}
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *conn) readLoop() {
	defer c.n.wg.Done()
	defer c.finish()
	for {
		c.mu.Lock()
		r := c.resume
		c.mu.Unlock()
		select {
		case <-r:
		case <-c.stopped:
			return
		}
		buf := make([]byte, c.n.cfg.ReadBuf)
		k, err := c.nc.Read(buf)
		if k > 0 {
			c.mu.Lock()
			local := c.local
			c.mu.Unlock()
			if !local {
				c.n.sink.DeliverData(core.BytesReceived{Conn: c.id, Data: buf[:k:k]})
			}
		}
		if err != nil {
			c.report(!errors.Is(err, io.EOF))
			c.mu.Lock()
			c.dead = true
			c.mu.Unlock()
			c.signalWriter()
			return
		}
	}
}

func (c *conn) signalWriter() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// finish runs when the reader is done: the connection is released once the
// writer has stopped as well.
func (c *conn) finish() {
	<-c.stopped
	_ = c.nc.Close() // best-effort teardown
	c.n.forget(c)
}

func (c *conn) writeLoop() {
	defer c.n.wg.Done()
	defer close(c.stopped)
	for {
		c.mu.Lock()
		if len(c.q) == 0 {
			dead := c.dead
			c.mu.Unlock()
			if dead {
				return
			}
			<-c.wake
			continue
		}
		it := c.q[0]
		c.q[0] = item{}
		c.q = c.q[1:]
		c.mu.Unlock()
		if it.fin {
			c.closeGracefully()
			return
		}
		_ = c.nc.SetWriteDeadline(time.Now().Add(c.n.cfg.WriteTimeout)) // best-effort teardown
		_, err := c.nc.Write(it.data)
		if it.done != nil {
			it.done(err)
		}
		if err != nil {
			c.fail()
			return
		}
	}
}

// fail resets the connection after a write error and fails queued writes.
func (c *conn) fail() {
	c.report(true)
	c.abort(false)
}

// closeGracefully sends FIN, then drains the peer's bytes for a while so
// closing with unread input cannot turn into a reset that eats the answers.
func (c *conn) closeGracefully() {
	if tc, ok := c.nc.(*net.TCPConn); ok {
		_ = tc.CloseWrite() // best-effort teardown
	}
	_ = c.nc.SetReadDeadline(time.Now().Add(c.n.cfg.LingerRead)) // best-effort teardown
	c.mu.Lock()
	c.dead = true
	c.mu.Unlock()
}
