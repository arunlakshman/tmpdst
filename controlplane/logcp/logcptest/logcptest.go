// Package logcptest builds logcp services over in-memory parts for tests,
// the model and the conformance suite: an in-memory (or fakedb) log, an
// in-memory applier store and an in-memory object store.
package logcptest

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"manifester/controlplane/applier"
	"manifester/controlplane/logcp"
	"manifester/core"
	"manifester/sys"
)

// Objects is an in-memory sys.ObjectStore.
type Objects struct {
	mu sync.Mutex
	m  map[string][]byte
}

var _ sys.ObjectStore = (*Objects)(nil)

// NewObjects returns an empty store.
func NewObjects() *Objects { return &Objects{m: map[string][]byte{}} }

// Put implements sys.ObjectStore.
func (o *Objects) Put(_ context.Context, key string, data []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.m[key] = append([]byte(nil), data...)
	return nil
}

// Get implements sys.ObjectStore.
func (o *Objects) Get(_ context.Context, ref core.SegmentRef) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	d, ok := o.m[ref.Key]
	if !ok || ref.Off < 0 || ref.Off+ref.Len > int64(len(d)) {
		return nil, &notFoundError{ref.Key}
	}
	return append([]byte(nil), d[ref.Off:ref.Off+ref.Len]...), nil
}

// Delete implements sys.ObjectStore.
func (o *Objects) Delete(_ context.Context, key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.m, key)
	return nil
}

// List implements sys.ObjectStore.
func (o *Objects) List(_ context.Context, prefix string) ([]string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []string
	for k := range o.m {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

type notFoundError struct{ key string }

func (e *notFoundError) Error() string { return "logcptest: no such object " + e.key }

// FastConfig is a config with short intervals for tests: one node over log, an
// in-memory applier store, no DB.
func FastConfig(name string, rank int, log logcp.Log) logcp.Config {
	return logcp.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Name:   name, Rank: rank, Incarnation: "t", Log: log, KV: applier.NewMemKV(),
		HoleAfter: 30 * time.Millisecond, TailPoll: 2 * time.Millisecond, TailBatch: time.Millisecond,
		Heartbeat: 20 * time.Millisecond, CkptEvery: -1, MaxSlotRate: 5000,
	}
}

// Start builds and starts a service from cfg, and closes it with t.
func Start(t testing.TB, cfg logcp.Config) *logcp.Service {
	t.Helper()
	s, err := logcp.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// NewMem is one active node over a fresh in-memory log.
func NewMem(t testing.TB) *logcp.Service {
	t.Helper()
	return Start(t, FastConfig("n0", 0, logcp.NewMemLog()))
}

// Server serves s's handler over httptest and closes it with t.
func Server(t testing.TB, s *logcp.Service) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv
}
