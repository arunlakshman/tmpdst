package prod

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"manifester/toys3/control"
)

// CopySourceHeader names the source of a CopyObject, as in S3:
// "x-amz-copy-source: /srcbucket/srckey" (URL-escaped; leading slash optional).
const CopySourceHeader = "X-Amz-Copy-Source"

// ErrMethodNotAllowed is returned by ParseRequest for unsupported methods.
var ErrMethodNotAllowed = errors.New("method not allowed")

// Handler maps the S3 REST subset onto control.Request events:
//
//	PUT    /{bucket}/{key}                          → control.Put{Key, Data: body}
//	PUT    /{bucket}/{key} + x-amz-copy-source      → control.Copy{Src, Dst}
//	GET    /{bucket}/{key}                          → control.Get
//	DELETE /{bucket}/{key}                          → control.Delete
//
// Buckets are not first-class: the object key is "bucket/key", so there is one
// flat namespace and no CreateBucket. Replies map Status straight to the HTTP
// status code; a 200 GET writes Body. S3 returns 204 for DELETE; toys3 keeps
// 200 for simplicity.
type Handler struct {
	Loop *Loop
	// MaxBody bounds request bodies (http.MaxBytesReader); 0 means no limit.
	MaxBody int64
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if r.Method == http.MethodPut && r.Header.Get(CopySourceHeader) == "" {
		rd := io.Reader(r.Body)
		if h.MaxBody > 0 {
			rd = http.MaxBytesReader(w, r.Body, h.MaxBody)
		}
		b, err := io.ReadAll(rd)
		if err != nil {
			http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
			return
		}
		body = b
	}
	op, err := ParseRequest(r, body)
	if errors.Is(err, ErrMethodNotAllowed) {
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, err.Error(), http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rep, err := h.Loop.Submit(r.Context(), op)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if rep.Status == control.StatusOK && rep.Body != nil {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(rep.Body)))
		w.WriteHeader(http.StatusOK)
		w.Write(rep.Body)
		return
	}
	w.WriteHeader(int(rep.Status))
}

// ParseRequest decodes an HTTP request (body already read) into a control.Op.
// Malformed requests (no key, bad copy source, unsupported method) return an
// error and become 400/405 without reaching the controller.
func ParseRequest(r *http.Request, body []byte) (control.Op, error) {
	key, err := splitKey(r.URL.Path)
	if err != nil {
		return nil, err
	}
	switch r.Method {
	case http.MethodGet:
		return control.Get{Key: key}, nil
	case http.MethodDelete:
		return control.Delete{Key: key}, nil
	case http.MethodPut:
		src := r.Header.Get(CopySourceHeader)
		if src == "" {
			return control.Put{Key: key, Data: body}, nil
		}
		unesc, err := url.PathUnescape(src)
		if err != nil {
			return nil, errors.New("bad " + CopySourceHeader + ": " + err.Error())
		}
		srcKey, err := splitKey(unesc)
		if err != nil {
			return nil, errors.New("bad " + CopySourceHeader + ": " + err.Error())
		}
		return control.Copy{Src: srcKey, Dst: key}, nil
	}
	return nil, ErrMethodNotAllowed
}

// splitKey turns "/bucket/key" (leading slash optional) into "bucket/key".
func splitKey(p string) (string, error) {
	p = strings.TrimPrefix(p, "/")
	bucket, key, ok := strings.Cut(p, "/")
	if !ok || bucket == "" || key == "" {
		return "", errors.New("path must be /{bucket}/{key}")
	}
	return p, nil
}

var _ http.Handler = (*Handler)(nil)
