package prod

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"manifester/toys3/control"
)

// server is one process lifetime: a Loop on dir behind an httptest server.
type server struct {
	t      *testing.T
	http   *httptest.Server
	cancel context.CancelFunc
	done   chan error
}

func startServer(t *testing.T, dir string, exec Executor) *server {
	t.Helper()
	if err := PrepareDir(dir); err != nil {
		t.Fatal(err)
	}
	loop := NewLoop(control.Config{Dir: filepath.ToSlash(dir), MaxObjectSize: 1 << 20}, control.NewMachine, exec, 64)
	loop.Log = log.New(io.Discard, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	s := &server{t: t, http: httptest.NewServer(&Handler{Loop: loop, MaxBody: 1 << 20}), cancel: cancel, done: make(chan error, 1)}
	go func() { s.done <- loop.Run(ctx) }()
	return s
}

func (s *server) stop() error {
	s.http.Close()
	s.cancel()
	return <-s.done
}

func (s *server) do(method, path string, body []byte, hdr map[string]string) (int, []byte) {
	s.t.Helper()
	req, err := http.NewRequest(method, s.http.URL+path, bytes.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (s *server) expect(method, path string, body []byte, hdr map[string]string, wantCode int, wantBody string) {
	s.t.Helper()
	code, b := s.do(method, path, body, hdr)
	if code != wantCode || (wantBody != "" && string(b) != wantBody) {
		s.t.Fatalf("%s %s: got %d %q, want %d %q", method, path, code, b, wantCode, wantBody)
	}
}

func TestEndToEnd(t *testing.T) {
	dir := t.TempDir()
	s := startServer(t, dir, OSExecutor{})
	s.expect("PUT", "/b/k1", []byte("hello"), nil, 200, "")
	s.expect("GET", "/b/k1", nil, nil, 200, "hello")
	s.expect("PUT", "/b/k2", nil, map[string]string{CopySourceHeader: "/b/k1"}, 200, "")
	s.expect("GET", "/b/k2", nil, nil, 200, "hello")
	s.expect("DELETE", "/b/k1", nil, nil, 200, "")
	s.expect("GET", "/b/k1", nil, nil, 404, "")
	s.expect("DELETE", "/b/k1", nil, nil, 404, "")
	s.expect("PUT", "/b/k3", nil, map[string]string{CopySourceHeader: "b/nope"}, 404, "")
	s.expect("PUT", "/b/sp%20ace/x", []byte("nested"), nil, 200, "")
	s.expect("GET", "/b/sp%20ace/x", nil, nil, 200, "nested")
	s.expect("GET", "/onlybucket", nil, nil, 400, "")
	s.expect("POST", "/b/k1", nil, nil, 405, "")
	if err := s.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}

	// A crash mid-PUT leaves a temp file behind.
	leftover := filepath.Join(dir, "tmp", "00000000000000ff")
	if err := os.WriteFile(leftover, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	s = startServer(t, dir, OSExecutor{})
	defer s.stop()
	// Requests queue behind recovery, so this reply proves recovery finished.
	s.expect("GET", "/b/k2", nil, nil, 200, "hello")
	s.expect("GET", "/b/k1", nil, nil, 404, "")
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("leftover temp file survived recovery: %v", err)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "tmp"))
	if len(entries) != 0 {
		t.Fatalf("tmp/ not empty: %v", entries)
	}
}

// failFsync wraps OSExecutor and fails every file fsync with EIO.
type failFsync struct{ OSExecutor }

func (f failFsync) Execute(a control.Action, done func(control.EventBody)) {
	if a, ok := a.(control.FSFsync); ok {
		go done(control.FSFsyncDone{IO: a.IO, Err: control.ErrIO})
		return
	}
	f.OSExecutor.Execute(a, done)
}

func TestHaltStopsLoop(t *testing.T) {
	s := startServer(t, t.TempDir(), failFsync{})
	s.expect("PUT", "/b/k", []byte("v"), nil, 500, "")
	if err := <-s.done; !errors.Is(err, ErrHalted) {
		t.Fatalf("Run returned %v, want ErrHalted", err)
	}
	s.done <- nil // let stop() return
	s.expect("GET", "/b/k", nil, nil, 503, "")
	s.stop()
}
