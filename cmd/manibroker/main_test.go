package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "manifester/controlplane/api/v1"
	"manifester/controlplane/apiconv"
	"manifester/controlplane/exec"
	"manifester/controlplane/machine"
	"manifester/controlplane/server"
	"manifester/core"
	"manifester/sys/runinfo"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func waitDial(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
		if err == nil {
			_ = c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never came up", addr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunMemBrokerStartsAndStops(t *testing.T) {
	listen, maddr := freeAddr(t), freeAddr(t)
	manifest := filepath.Join(t.TempDir(), "m.json")
	stop := make(chan struct{})
	done := make(chan int, 1)
	var logs bytes.Buffer
	go func() {
		done <- run([]string{"-objects", "mem", "-listen", listen, "-metrics-addr", maddr, "-run-id", "bt",
			"-debug", "-manifest-out", manifest, "-stop-grace", "1s"}, &logs, stop)
	}()
	waitDial(t, listen)
	waitDial(t, maddr)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+maddr+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	_, _ = body.ReadFrom(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(body.String(), "manibroker_open_connections") {
		t.Error("open_connections gauge missing")
	}
	close(stop)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code %d\n%s", code, logs.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("did not stop")
	}
	for _, want := range []string{"manibroker starting", "manibroker listening", "manibroker tuning", "manifest written", "manibroker stopped"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
	b, err := os.ReadFile(manifest) //nolint:gosec // G304: path is a test temp file
	if err != nil || !strings.Contains(string(b), `"max_segment_batches"`) {
		t.Fatalf("%v %s", err, b)
	}
}

func TestRunErrors(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	var logs bytes.Buffer
	busy, _ := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	defer func() { _ = busy.Close() }()
	cases := map[string][]string{
		"bad level":       {"-log-level", "loud"},
		"bad tuning":      {"-segment-bytes", "x", "-objects", "mem", "-metrics-addr", ""},
		"unknown objects": {"-objects", "nope", "-metrics-addr", ""},
		"metrics busy":    {"-objects", "mem", "-metrics-addr", busy.Addr().String()},
		"listen busy":     {"-objects", "mem", "-metrics-addr", "", "-listen", busy.Addr().String()},
		"bad advertise":   {"-objects", "mem", "-metrics-addr", "", "-listen", "127.0.0.1:0", "-advertise", "nohostport"},
		"bad adv port":    {"-objects", "mem", "-metrics-addr", "", "-listen", "127.0.0.1:0", "-advertise", "h:xx"},
	}
	for name, args := range cases {
		logs.Reset()
		if code := run(args, &logs, stop); code != 1 {
			t.Errorf("%s: code %d\n%s", name, code, logs.String())
		}
	}
	if run([]string{"-bogus"}, &logs, stop) != 2 || run([]string{"-h"}, &logs, stop) != 0 {
		t.Fatal("flag errors")
	}
}

func TestOpenObjectsS3(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()
	stop := make(chan struct{})
	close(stop)
	var logs bytes.Buffer
	// s3 store with a stub endpoint and -s3-create-bucket: the node starts, then stops at once.
	code := run([]string{"-objects", "s3", "-s3-endpoint", srv.URL, "-s3-path-style", "-s3-create-bucket", "-metrics-addr", "",
		"-listen", freeAddr(t), "-stop-grace", "1s"}, &logs, stop)
	if code != 0 {
		t.Fatalf("code %d\n%s", code, logs.String())
	}
	if !strings.Contains(logs.String(), "manibroker starting") {
		t.Fatal(logs.String())
	}
}

type unavailCP struct {
	cpv1.UnimplementedControlPlaneServer
}

func (unavailCP) DescribeTopics(context.Context, *cpv1.DescribeTopicsRequest) (*cpv1.DescribeTopicsResponse, error) {
	detail := &cpv1.ErrorDetail{
		Code:    apiconv.FromCoreErr(core.ErrUnavailable),
		Outcome: apiconv.FromCoreOutcome(core.NotApplied),
	}
	s, err := status.New(codes.Unavailable, "standby").WithDetails(detail)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "standby")
	}
	return nil, s.Err()
}

func serveCP(t *testing.T, reg func(*grpc.Server)) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	reg(gs)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.Stop)
	return ln.Addr().String()
}

// -cp-url takes a list: a dead address and a standby (Unavailable) are skipped in one call.
func TestCPURLListFailsOver(t *testing.T) {
	deadLn, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := deadLn.Addr().String()
	_ = deadLn.Close()

	standby := serveCP(t, func(gs *grpc.Server) {
		cpv1.RegisterControlPlaneServer(gs, unavailCP{})
	})
	upNode := server.New(server.Config{
		Machine: machine.Config{K: 4, CatchUpEvery: core.Duration(time.Millisecond), Rank: 0, Heartbeat: core.Duration(time.Second)},
		Log:     exec.NewMemLog(),
		Objects: exec.NewMemObjects(),
		Self:    core.NodeRef{Node: "cp-up"},
	})
	if err := upNode.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upNode.Close() })
	up := serveCP(t, upNode.Register)

	f, _, err := parseFlags([]string{"-cp-url", dead + "," + standby + "," + up}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _, _, _, err := nodeConfig(f, slog.New(slog.NewTextHandler(io.Discard, nil)), runinfo.New("manibroker", "t", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.ControlPlane.Call(t.Context(), core.DescribeTopics{}); err != nil {
		t.Fatalf("call through a dead url and a standby: %v", err)
	}
}
