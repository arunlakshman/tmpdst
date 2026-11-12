//go:build integration

// Package integration runs the real mdscontrolplane binary against real
// DynamoDB Local, PostgreSQL and S3 (SeaweedFS) from internal/testinfra.
//
// The binary is built once per test run. Each node is a child process with
// its own free port and Pebble directory. Its output goes into the test log,
// and the process is killed when the test ends.
package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/testinfra"
)

const binaryPackage = "github.com/manifesterio/mds/controlplane/cmd/mdscontrolplane"

// binDir holds the built binary. TestMain makes it and removes it at the end.
var binDir string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mds-integration-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binDir = dir
	code := m.Run()
	testinfra.Stop()
	_ = os.RemoveAll(dir) // the built binary only
	os.Exit(code)
}

// binary builds mdscontrolplane the first time a test asks for it. The go
// command runs in this package's directory, which is inside the module, so the
// import path finds the module root.
var binary = sync.OnceValues(func() (string, error) {
	path := filepath.Join(binDir, "mdscontrolplane")
	out, err := exec.CommandContext(context.Background(), "go", "build", "-o", path, binaryPackage).CombinedOutput() //nolint:gosec // G204: our own temp path
	if err != nil {
		return "", fmt.Errorf("go build %s: %w\n%s", binaryPackage, err, out)
	}
	return path, nil
})

// syncBuf collects the output of one process. The process writes from one
// goroutine and the test reads from another.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// node is one running mdscontrolplane process.
type node struct {
	name string
	addr string
	cmd  *exec.Cmd
	done chan struct{} // closed when the process has exited
	code int           // exit code, valid after done; -1 if killed by a signal
	logs *syncBuf
	api  cpv1.ControlPlaneClient // talks to this node only, no failover
}

// startNode runs the binary with args and waits until the node is ready. The
// listen address, no metrics port and a fresh Pebble directory are added here.
func startNode(t *testing.T, name string, args []string) *node {
	t.Helper()
	bin, err := binary()
	if err != nil {
		t.Fatal(err)
	}
	n := &node{name: name, addr: freeAddr(t), done: make(chan struct{}), logs: &syncBuf{}}
	args = append([]string{"-addr", n.addr, "-metrics-addr", "", "-node-name", name,
		"-pebble-dir", filepath.Join(t.TempDir(), "pebble")}, args...)
	n.cmd = exec.CommandContext(t.Context(), bin, args...) //nolint:gosec // G204: the binary this test built, with test flags
	// No real AWS account, profile or metadata service: the binary uses its
	// static test key when an endpoint is set and AWS_ACCESS_KEY_ID is empty.
	n.cmd.Env = append(os.Environ(), "AWS_EC2_METADATA_DISABLED=true", "AWS_ACCESS_KEY_ID=",
		"AWS_SECRET_ACCESS_KEY=", "AWS_SESSION_TOKEN=", "AWS_PROFILE=", "MDS_POSTGRES_DSN=")
	n.cmd.Stdout, n.cmd.Stderr = n.logs, n.logs
	if err := n.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		err := n.cmd.Wait()
		n.code = 0
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			n.code = ee.ExitCode()
		} else if err != nil {
			n.code = -1
		}
		close(n.done)
	}()
	conn, err := grpc.NewClient(n.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	n.api = cpv1.NewControlPlaneClient(conn)
	t.Cleanup(func() {
		_ = conn.Close()
		n.kill()
		t.Logf("--- output of %s (%s) ---\n%s", n.name, n.addr, n.logs.String())
	})
	n.waitReady(t)
	return n
}

// waitReady polls GetStatus. It answers only when the node is ready (its
// health is SERVING then too).
func (n *node) waitReady(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		select {
		case <-n.done:
			t.Fatalf("%s exited %d before it was ready\n%s", n.name, n.code, n.logs.String())
		default:
		}
		if n.role(t) != "down" {
			return
		}
	}
	t.Fatalf("%s never became ready\n%s", n.name, n.logs.String())
}

// role is the node's role: "active", "standby", or "down" when it does not
// answer (not ready, stopped or killed).
func (n *node) role(t *testing.T) string {
	st, err := n.status(t)
	if err != nil {
		return "down"
	}
	return st.GetRole()
}

func (n *node) status(t *testing.T) (*cpv1.GetStatusResponse, error) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	return n.api.GetStatus(ctx, &cpv1.GetStatusRequest{})
}

// waitRole waits until the node has the role. A node that is ready can still
// be a standby for a moment, and it refuses commits until it is active.
func (n *node) waitRole(t *testing.T, want string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if n.role(t) == want {
			return
		}
	}
	t.Fatalf("%s: role %q, want %q\n%s", n.name, n.role(t), want, n.logs.String())
}

// stop sends SIGTERM, as an operator would, and returns the exit code.
func (n *node) stop(t *testing.T) int {
	t.Helper()
	if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	select {
	case <-n.done:
		return n.code
	case <-time.After(20 * time.Second):
		t.Fatalf("%s did not stop\n%s", n.name, n.logs.String())
	}
	return -1
}

// kill ends the process at once with SIGKILL, as a crash would.
func (n *node) kill() {
	_ = n.cmd.Process.Kill()
	<-n.done
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

// rankArgs are the role flags of one node in a group: fast heartbeats and a
// short takeover, so a test sees a takeover in about a second.
func rankArgs(rank int) []string {
	return []string{"-rank", strconv.Itoa(rank), "-takeover-after", "600ms", "-heartbeat", "50ms"}
}
