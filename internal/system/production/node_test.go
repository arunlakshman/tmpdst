package production

import (
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"manifester/internal/broker"
	"manifester/internal/core"
	"manifester/internal/system"
	"manifester/internal/verify/speccheck/specsystem"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func startNode(t *testing.T, mod func(*NodeConfig)) (*Node, NodeConfig) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := NodeConfig{
		NodeID: "n1", Broker: broker.DefaultConfig(), Listener: ln,
		Objects: specsystem.NewObjectStore(), ObjClassify: MemoryErrorClassifier{},
		ControlPlane: specsystem.NewControlPlane(), CPClassify: MemoryErrorClassifier{},
		StopGrace: time.Second, FlushGrace: 50 * time.Millisecond,
	}
	if mod != nil {
		mod(&cfg)
	}
	n, err := Start(t.Context(), cfg)
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Kill(); <-n.Done() })
	return n, cfg
}

func waitDone(t *testing.T, n *Node) error {
	t.Helper()
	select {
	case <-n.Done():
		return n.Wait()
	case <-time.After(10 * time.Second):
		t.Fatal("node did not stop")
		return nil
	}
}

func TestStartRequiresPieces(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	full := NodeConfig{Listener: ln, Objects: specsystem.NewObjectStore(), ControlPlane: specsystem.NewControlPlane()}
	for name, mod := range map[string]func(*NodeConfig){
		"listener": func(c *NodeConfig) { c.Listener = nil },
		"objects":  func(c *NodeConfig) { c.Objects = nil },
		"cp":       func(c *NodeConfig) { c.ControlPlane = nil },
	} {
		c := full
		mod(&c)
		if _, err := Start(t.Context(), c); err == nil || !strings.Contains(err.Error(), "required") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestStartGracefulStop(t *testing.T) {
	var mu sync.Mutex
	var seen []core.IOCall
	n, _ := startNode(t, func(c *NodeConfig) {
		c.OnIO = func(a core.IOCall) { mu.Lock(); seen = append(seen, a); mu.Unlock() }
	})
	if n.Addr() == nil || !strings.HasPrefix(n.String(), "broker(127.0.0.1:") {
		t.Errorf("addr %v string %q", n.Addr(), n.String())
	}
	if n.OpenConns() != 0 {
		t.Errorf("open conns %d", n.OpenConns())
	}
	// A list-all-topics Metadata request makes the broker call the control
	// plane; the tap sees that action.
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", n.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	req := kmsg.NewPtrMetadataRequest()
	req.Version, req.Topics = 1, nil
	body := req.AppendTo(nil)
	frame := binary.BigEndian.AppendUint32(nil, uint32(8+2+len(body)))
	frame = binary.BigEndian.AppendUint16(frame, uint16(req.Key()))
	frame = binary.BigEndian.AppendUint16(frame, uint16(req.Version))
	frame = binary.BigEndian.AppendUint32(frame, 7)
	frame = binary.BigEndian.AppendUint16(frame, 0xffff) // null client id
	if _, err := conn.Write(append(frame, body...)); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		mu.Lock()
		got := len(seen)
		mu.Unlock()
		if got > 0 {
			break
		}
	}
	mu.Lock()
	if len(seen) == 0 {
		t.Error("OnIO saw no action")
	}
	mu.Unlock()
	n.Stop()
	n.Stop() // idempotent
	if err := waitDone(t, n); err != nil {
		t.Errorf("graceful stop: %v", err)
	}
}

func TestStartKill(t *testing.T) {
	n, _ := startNode(t, nil)
	n.Kill()
	if err := waitDone(t, n); !errors.Is(err, system.ErrStopped) && !errors.Is(err, system.ErrKilledByRuntime) {
		t.Errorf("kill: %v", err)
	}
}

func TestStartShutdownChannel(t *testing.T) {
	sd := make(chan struct{})
	n, _ := startNode(t, func(c *NodeConfig) { c.Shutdown = sd })
	close(sd)
	if err := waitDone(t, n); err != nil {
		t.Errorf("shutdown channel: %v", err)
	}
}

func TestAdvertisedAndDefaults(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	bc, err := advertised(broker.Config{}, ln)
	if err != nil || bc.Host != "127.0.0.1" || bc.Port != int32(ln.Addr().(*net.TCPAddr).Port) {
		t.Errorf("advertised %+v %v", bc, err)
	}
	bc, _ = advertised(broker.Config{Host: "h", Port: 9}, ln)
	if bc.Host != "h" || bc.Port != 9 {
		t.Errorf("explicit kept: %+v", bc)
	}
	if _, err := advertised(broker.Config{}, badAddrListener{ln}); err == nil {
		t.Error("bad listener address accepted")
	}
	d := withDefaults(NodeConfig{})
	if d.StopGrace != 10*time.Second || d.FlushGrace != 5*time.Second || d.DataInbox != 1024 {
		t.Errorf("defaults %+v", d)
	}
	d = withDefaults(NodeConfig{StopGrace: 1, FlushGrace: 2, DataInbox: 3})
	if d.StopGrace != 1 || d.FlushGrace != 2 || d.DataInbox != 3 {
		t.Errorf("explicit kept %+v", d)
	}
}

type badAddr struct{}

func (badAddr) Network() string { return "x" }
func (badAddr) String() string  { return "no-port" }

type badAddrListener struct{ net.Listener }

func (badAddrListener) Addr() net.Addr { return badAddr{} }
