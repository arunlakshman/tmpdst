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
	"sync"
	"testing"

	"manifester/toys3/control"
	"manifester/toys3/format"
	"manifester/toys3/veritas"
)

// disk is the storage one test runs on, kept across server restarts: an
// Executor plus the root its data lives under (obj/ and tmp/ already exist)
// and direct access for planting crash leftovers and inspecting the result.
type disk interface {
	Executor
	Root() string                   // slash-separated data root, for control.Config.Dir
	Plant(path string, data []byte) // write a file behind the server's back
	Names(dir string) []string      // base names in dir, sorted
}

// osDisk is a real directory under t.TempDir(), driven by OSExecutor.
type osDisk struct {
	OSExecutor
	t    *testing.T
	root string
}

func newOSDisk(t *testing.T) *osDisk {
	root := filepath.ToSlash(t.TempDir())
	if err := PrepareDir(root); err != nil {
		t.Fatal(err)
	}
	return &osDisk{t: t, root: root}
}

func (d *osDisk) Root() string { return d.root }

func (d *osDisk) Plant(path string, data []byte) {
	if err := os.WriteFile(osPath(path), data, 0o644); err != nil {
		d.t.Fatal(err)
	}
}

func (d *osDisk) Names(dir string) []string {
	entries, err := os.ReadDir(osPath(dir))
	if err != nil {
		d.t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

// veritasDisk is an Executor over veritas.FS: no disk at all. veritas.FS is
// sequential, so a mutex serializes Exec; the completion is delivered on a
// fresh goroutine because Execute is called from the loop goroutine and done
// posts to the loop's inbox, which may be full.
type veritasDisk struct {
	mu sync.Mutex
	fs *veritas.FS
}

const veritasRoot = "/d"

func newVeritasDisk() *veritasDisk {
	return &veritasDisk{fs: veritas.NewFS(veritasRoot, format.ObjDir(veritasRoot), format.TmpDir(veritasRoot))}
}

func (d *veritasDisk) Execute(a control.Actuation, done func(control.EventBody)) {
	d.mu.Lock()
	fb := d.fs.Exec(a)
	d.mu.Unlock()
	go done(fb)
}

func (d *veritasDisk) Root() string { return veritasRoot }

func (d *veritasDisk) Plant(path string, data []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fs.WriteFile(path, data)
}

func (d *veritasDisk) Names(dir string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	fb := d.fs.Exec(control.FSList{Dir: dir}).(control.FSListDone)
	return fb.Names
}

// disks runs f once on each storage backend: veritas.FS (no disk) and a real
// directory (the only end-to-end test of real I/O).
func disks(t *testing.T, f func(t *testing.T, d disk)) {
	t.Run("veritas", func(t *testing.T) { f(t, newVeritasDisk()) })
	t.Run("os", func(t *testing.T) { f(t, newOSDisk(t)) })
}

// server is one process lifetime: a Loop on root behind an httptest server.
type server struct {
	t      *testing.T
	http   *httptest.Server
	cancel context.CancelFunc
	done   chan error
}

func startServer(t *testing.T, root string, exec Executor) *server {
	t.Helper()
	loop := NewLoop(control.Config{Dir: root, MaxObjectSize: 1 << 20}, control.NewMachine, exec, 64)
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

func TestEndToEnd(t *testing.T) { disks(t, testEndToEnd) }

func testEndToEnd(t *testing.T, d disk) {
	s := startServer(t, d.Root(), d)
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
	d.Plant(format.TmpPath(d.Root(), 0xff), []byte("junk"))

	s = startServer(t, d.Root(), d) // the next process, on the same storage
	defer s.stop()
	// Requests queue behind recovery, so this reply proves recovery finished.
	s.expect("GET", "/b/k2", nil, nil, 200, "hello")
	s.expect("GET", "/b/k1", nil, nil, 404, "")
	if names := d.Names(format.TmpDir(d.Root())); len(names) != 0 {
		t.Fatalf("tmp/ not empty after recovery (leftover temp file survived?): %v", names)
	}
}

// failFsync wraps an Executor and fails every file fsync with EIO.
type failFsync struct{ Executor }

func (f failFsync) Execute(a control.Actuation, done func(control.EventBody)) {
	if a, ok := a.(control.FSFsync); ok {
		go done(control.FSFsyncDone{IO: a.IO, Err: control.ErrIO})
		return
	}
	f.Executor.Execute(a, done)
}

func TestTripStopsLoop(t *testing.T) {
	disks(t, func(t *testing.T, d disk) {
		s := startServer(t, d.Root(), failFsync{d})
		s.expect("PUT", "/b/k", []byte("v"), nil, 500, "")
		if err := <-s.done; !errors.Is(err, ErrTripped) {
			t.Fatalf("Run returned %v, want ErrTripped", err)
		}
		s.done <- nil // let stop() return
		s.expect("GET", "/b/k", nil, nil, 503, "")
		s.stop()
	})
}
