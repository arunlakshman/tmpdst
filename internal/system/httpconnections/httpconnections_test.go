package httpconnections

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// doer is what both the plain and the AWS SDK clients offer.
type doer interface {
	Do(*http.Request) (*http.Response, error)
}

// wave sends n requests that are all in flight at once (the handler waits
// until n have arrived), so n connections are needed.
func wave(t *testing.T, d doer, url string, n int, arrived *atomic.Int64) {
	t.Helper()
	arrived.Store(0)
	ctx := t.Context()
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			request, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			response, err := d.Do(request)
			if err != nil {
				t.Error(err)
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		})
	}
	wg.Wait()
}

// 200 concurrent requests need 200 connections the first time and none the
// next time: the idle pool is as large as the peak concurrency, so a steady
// load dials nothing (with the old 16 or 10 it re-dialed about 185 each wave).
func TestConnectionsAreReused(t *testing.T) {
	const n = 200
	for name, mk := range map[string]func() doer{
		"plain": func() doer { return &http.Client{Transport: Transport()} },
		"aws":   func() doer { return ConfigureAWSClient() },
	} {
		t.Run(name, func(t *testing.T) {
			var arrived atomic.Int64
			var dials atomic.Int64
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Hold every request until all n are in flight (or 10 s, so a
				// failed dial fails the test instead of hanging it).
				arrived.Add(1)
				for dl := time.Now().Add(10 * time.Second); arrived.Load() < n && time.Now().Before(dl); {
					time.Sleep(time.Millisecond)
				}
				_, _ = w.Write([]byte("ok"))
			}))
			srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
				if s == http.StateNew {
					dials.Add(1)
				}
			}
			srv.Start()
			defer srv.Close()

			d := mk()
			wave(t, d, srv.URL, n, &arrived)
			first := dials.Load()
			if first < n || first > n+8 {
				t.Fatalf("first wave dialed %d connections, want about %d", first, n)
			}
			for range 3 {
				wave(t, d, srv.URL, n, &arrived)
			}
			if again := dials.Load() - first; again > 8 {
				t.Fatalf("three more waves of %d dialed %d new connections: connections are not reused", n, again)
			}
		})
	}
}
