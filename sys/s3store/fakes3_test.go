package s3store

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeS3 is the minimal path-style S3 the store needs: bucket create, object
// PUT/GET (with Range)/HEAD/DELETE and ListObjectsV2. inject, if set, may
// answer a request with an error before it is served.
type fakeS3 struct {
	mu      sync.Mutex
	objs    map[string][]byte // "bucket/key"
	buckets map[string]bool
	inject  func(r *http.Request) (status int, code string)
	srv     *httptest.Server
	calls   int
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{objs: map[string][]byte{}, buckets: map[string]bool{}}
	f.srv = httptest.NewServer(f)
	t.Cleanup(f.srv.Close)
	return f
}

func s3Error(w http.ResponseWriter, r *http.Request, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
	}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.inject != nil {
		if st, code := f.inject(r); st != 0 {
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
		f.objs[path] = data
	case r.Method == http.MethodDelete:
		delete(f.objs, path)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		f.get(w, r, path)
	default:
		s3Error(w, r, 405, "MethodNotAllowed")
	}
}

func (f *fakeS3) get(w http.ResponseWriter, r *http.Request, path string) {
	data, ok := f.objs[path]
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

func (f *fakeS3) list(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	var keys []string
	for p := range f.objs {
		if k, ok := strings.CutPrefix(p, bucket+"/"); ok && strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
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

// newSlow is an endpoint that never answers until release is closed.
func newSlow(release chan struct{}) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	return srv
}
