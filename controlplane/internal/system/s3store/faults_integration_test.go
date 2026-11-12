//go:build integration

package s3store

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/testinfra"
)

// faultTransport wraps the transport of the HTTP client that the S3 SDK uses.
// Every request goes to SeaweedFS, unless the function given to set answers it.
type faultTransport struct {
	base   http.RoundTripper
	calls  atomic.Int64
	inject atomic.Pointer[func(*http.Request) (*http.Response, error)]
}

// RoundTrip counts the request, then lets the injected function answer it, or
// passes it to SeaweedFS when there is none or it returns nil, nil.
func (f *faultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.calls.Add(1)
	if p := f.inject.Load(); p != nil {
		if resp, err := (*p)(r); resp != nil || err != nil {
			if r.Body != nil {
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
			}
			return resp, err
		}
	}
	return f.base.RoundTrip(r)
}

func (f *faultTransport) set(fn func(*http.Request) (*http.Response, error)) { f.inject.Store(&fn) }

// s3ErrorResponse is what an S3 server sends for an error.
func s3ErrorResponse(r *http.Request, status int, code string) *http.Response {
	body := ""
	if r.Method != http.MethodHead {
		body = fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
	}
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:        http.Header{"Content-Type": {"application/xml"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)), Request: r,
	}
}

// newTestStore returns a store on a fresh SeaweedFS bucket. Its HTTP client goes
// through the returned faultTransport, which passes everything to SeaweedFS until
// a test calls set.
func newTestStore(t *testing.T, prefix string) (*Store, *faultTransport, testinfra.S3Info) {
	t.Helper()
	info := testinfra.S3(t)
	ft := &faultTransport{base: http.DefaultTransport}
	s, err := New(t.Context(), Config{
		Endpoint: info.Endpoint, Region: info.Region, Bucket: info.Bucket, Prefix: prefix,
		PathStyle: true, HTTP: &http.Client{Transport: ft},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, ft, info
}
