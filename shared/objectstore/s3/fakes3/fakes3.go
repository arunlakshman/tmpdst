// Package fakes3 is a small fake of the S3 service for tests. It speaks the path-style
// requests that the S3 adapter needs, and runs in the process (net/http/httptest), so a
// test needs no network and no container. It is test support code, not production code.
package fakes3

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Server is the minimal path-style S3 the store needs: bucket create, object
// PUT/GET (with Range)/HEAD/DELETE and ListObjectsV2. Inject, if set, may
// answer a request with an error before it is served.
type Server struct {
	mu      sync.Mutex
	Objs    map[string][]byte // "bucket/key". Read it only when no request is running.
	buckets map[string]bool
	// Inject answers a request with an HTTP status and an S3 error code. Return status 0
	// to serve the request as usual. Set it only when no request is running.
	Inject func(r *http.Request) (status int, code string)
	// URL is the address of the server, for the Endpoint of the adapter.
	URL   string
	srv   *httptest.Server
	Calls int // the number of requests so far. Read it only when no request is running.
}

// New starts a fake S3 server. It stops when the test ends.
func New(t testing.TB) *Server {
	t.Helper()
	f := &Server{Objs: map[string][]byte{}, buckets: map[string]bool{}}
	f.srv = httptest.NewServer(f)
	f.URL = f.srv.URL
	t.Cleanup(f.srv.Close)
	return f
}

// Close stops the server before the test ends (a test of a dead endpoint).
func (f *Server) Close() { f.srv.Close() }

func s3Error(w http.ResponseWriter, r *http.Request, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
	}
}

func (f *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls++
	if f.Inject != nil {
		if st, code := f.Inject(r); st != 0 {
			_, _ = io.Copy(io.Discard, r.Body)
			s3Error(w, r, st, code)
			return
		}
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	switch {
	case key == "" && r.Method == http.MethodPut:
		if f.buckets[bucket] {
			s3Error(w, r, 409, "BucketAlreadyOwnedByYou")
			return
		}
		f.buckets[bucket] = true
	case key == "" && r.Method == http.MethodGet:
		f.list(w, r, bucket)
	case r.Method == http.MethodPut:
		data, _ := io.ReadAll(r.Body)
		f.Objs[path] = data
	case r.Method == http.MethodDelete:
		delete(f.Objs, path)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		f.get(w, r, path)
	default:
		s3Error(w, r, 405, "MethodNotAllowed")
	}
}

func (f *Server) get(w http.ResponseWriter, r *http.Request, path string) {
	data, ok := f.Objs[path]
	if !ok {
		s3Error(w, r, 404, "NoSuchKey")
		return
	}
	body, status := data, http.StatusOK
	if rg := r.Header.Get("Range"); rg != "" && r.Method == http.MethodGet {
		spec := strings.TrimPrefix(rg, "bytes=")
		lo, hi, _ := strings.Cut(spec, "-")
		start, _ := strconv.Atoi(lo)
		end := len(data) - 1
		if hi != "" {
			end, _ = strconv.Atoi(hi)
		}
		if start >= len(data) {
			s3Error(w, r, 416, "InvalidRange")
			return
		}
		end = min(end, len(data)-1)
		body, status = data[start:end+1], http.StatusPartialContent
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func (f *Server) list(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	var keys []string
	for p := range f.Objs {
		if k, ok := strings.CutPrefix(p, bucket+"/"); ok && strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	type content struct{ Key string }
	res := struct {
		XMLName     xml.Name `xml:"ListBucketResult"`
		IsTruncated bool
		KeyCount    int
		Contents    []content
	}{KeyCount: len(keys)}
	for _, k := range keys {
		res.Contents = append(res.Contents, content{k})
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(res)
}

// NewSlow is an endpoint that never answers until release is closed.
func NewSlow(release chan struct{}) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	return srv
}
