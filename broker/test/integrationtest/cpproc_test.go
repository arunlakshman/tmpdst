//go:build integration

package integrationtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/controlplane/client"
)

const cpBinaryPackage = "github.com/manifesterio/mds/controlplane/cmd/mdscontrolplane"

// binDir holds the built mdscontrolplane binary. TestMain creates and removes it.
var binDir string

func prepareBinDir() error {
	dir, err := os.MkdirTemp("", "mds-broker-it-")
	if err != nil {
		return err
	}
	binDir = dir
	return nil
}

// cpBinary builds mdscontrolplane once per process.
var cpBinary = sync.OnceValues(func() (string, error) {
	if binDir == "" {
		return "", errors.New("integrationtest: binDir not set (TestMain must call prepareBinDir)")
	}
	path := filepath.Join(binDir, "mdscontrolplane")
	out, err := exec.CommandContext(context.Background(), "go", "build", "-o", path, cpBinaryPackage).CombinedOutput() //nolint:gosec // G204: our own temp path
	if err != nil {
		return "", fmt.Errorf("go build %s: %w\n%s", cpBinaryPackage, err, out)
	}
	return path, nil
})

// syncBuf collects one process's output for the test log.
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

// cpProc is one running mdscontrolplane child process.
type cpProc struct {
	addr string
	cmd  *exec.Cmd
	done chan struct{}
	code int
	logs *syncBuf
}

// startCP runs the binary with args (listen address, metrics off and a fresh
// Pebble dir are added here) and waits until GetStatus answers.
func startCP(t *testing.T, args []string) *cpProc {
	t.Helper()
	bin, err := cpBinary()
	if err != nil {
		t.Fatal(err)
	}
	n := &cpProc{addr: freeAddr(t), done: make(chan struct{}), logs: &syncBuf{}}
	args = append([]string{
		"-addr", n.addr,
		"-metrics-addr", "",
		"-node-name", "broker-it-cp",
		"-pebble-dir", filepath.Join(t.TempDir(), "pebble"),
	}, args...)
	n.cmd = exec.CommandContext(t.Context(), bin, args...) //nolint:gosec // G204: binary this test built
	n.cmd.Env = append(os.Environ(),
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_ACCESS_KEY_ID=",
		"AWS_SECRET_ACCESS_KEY=",
		"AWS_SESSION_TOKEN=",
		"AWS_PROFILE=",
		"MDS_POSTGRES_DSN=",
	)
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
	t.Cleanup(func() {
		n.kill()
		t.Logf("--- output of mdscontrolplane (%s) ---\n%s", n.addr, n.logs.String())
	})
	n.waitReady(t)
	return n
}

func (n *cpProc) waitReady(t *testing.T) {
	t.Helper()
	cp := client.New(n.addr, nil)
	t.Cleanup(func() { _ = cp.Close() })
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		select {
		case <-n.done:
			t.Fatalf("mdscontrolplane exited %d before ready\n%s", n.code, n.logs.String())
		default:
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		st, err := cp.GetStatus(ctx)
		cancel()
		if err == nil && st.GetRole() != "" && st.GetRole() != "down" {
			return
		}
	}
	t.Fatalf("mdscontrolplane never became ready\n%s", n.logs.String())
}

func (n *cpProc) kill() {
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
