package prod

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"manifester/core"
	"manifester/sys"
)

func newTestNet(t *testing.T) (*Net, *sys.Inbox) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	in := sys.NewInbox(64)
	n := NewNet(ln, in, NetConfig{LingerRead: 200 * time.Millisecond})
	t.Cleanup(func() { n.Shutdown(time.Second); in.Close() })
	return n, in
}

// next pops the next data event, waiting up to a second.
func next(t *testing.T, in *sys.Inbox) core.EventBody {
	t.Helper()
	for end := time.Now().Add(time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		if b, ok := in.Data().Pop(); ok {
			return b
		}
	}
	t.Fatal("no event")
	return nil
}

func TestNetOpenReadWriteOrdered(t *testing.T) {
	n, in := newTestNet(t)
	c, err := net.Dial("tcp", n.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	op := next(t, in).(core.ConnOpened)
	_, _ = c.Write([]byte("hello"))
	var got []byte
	for len(got) < 5 {
		got = append(got, next(t, in).(core.BytesReceived).Data...)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
	var wg sync.WaitGroup
	var want bytes.Buffer
	for i := 0; i < 100; i++ {
		chunk := bytes.Repeat([]byte{byte(i)}, 1000)
		want.Write(chunk)
		wg.Add(1)
		if err := n.WriteAsync(op.Conn, chunk, func(err error) {
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}); err != nil {
			t.Fatal(err)
		}
	}
	back := make([]byte, want.Len())
	if _, err := io.ReadFull(c, back); err != nil || !bytes.Equal(back, want.Bytes()) {
		t.Fatalf("writes reordered or lost: %v", err)
	}
	wg.Wait()
	_ = c.Close()
	if cc, ok := next(t, in).(core.ConnClosed); !ok || cc.Conn != op.Conn || cc.Abort {
		t.Fatalf("want graceful ConnClosed, got %+v", cc)
	}
}

func TestNetPauseParksReader(t *testing.T) {
	n, in := newTestNet(t)
	c, _ := net.Dial("tcp", n.Addr().String())
	defer func() { _ = c.Close() }()
	op := next(t, in).(core.ConnOpened)
	_, _ = c.Write([]byte("a"))
	next(t, in)
	n.PauseReads(op.Conn)
	// The Read already blocked when we paused may return once more.
	_, _ = c.Write([]byte("b"))
	time.Sleep(100 * time.Millisecond)
	for in.Data().Len() > 0 {
		in.Data().Pop()
	}
	_, _ = c.Write([]byte("c"))
	time.Sleep(100 * time.Millisecond)
	if in.Data().Len() != 0 {
		t.Fatal("reader kept reading while paused")
	}
	n.ResumeReads(op.Conn)
	if r := next(t, in).(core.BytesReceived); string(r.Data) != "c" {
		t.Fatalf("got %q after resume", r.Data)
	}
}

func TestNetAbortResetsAndGracefulFlushes(t *testing.T) {
	n, in := newTestNet(t)
	c, _ := net.Dial("tcp", n.Addr().String())
	defer func() { _ = c.Close() }()
	op := next(t, in).(core.ConnOpened)
	_ = n.Write(op.Conn, []byte("bye"))
	if err := n.Close(op.Conn, false); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(c) // FIN after the queued bytes
	if err != nil || string(b) != "bye" {
		t.Fatalf("graceful close: %q %v", b, err)
	}
	if err := n.Write(op.Conn, []byte("late")); err == nil {
		t.Fatal("write after close succeeded")
	}

	c2, _ := net.Dial("tcp", n.Addr().String())
	defer func() { _ = c2.Close() }()
	op2 := next(t, in).(core.ConnOpened)
	if op2.Conn == op.Conn {
		t.Fatal("ConnID reused")
	}
	_ = n.Close(op2.Conn, true)
	_ = c2.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c2.Read(make([]byte, 1)); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("abort should reset, got %v", err)
	}
}
