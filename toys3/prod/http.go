package prod

import (
	"net/http"

	"manifester/toys3/core"
)

// CopySourceHeader names the source of a CopyObject, as in S3:
// "x-amz-copy-source: /srcbucket/srckey" (URL-escaped; leading slash optional).
const CopySourceHeader = "X-Amz-Copy-Source"

// Handler maps the S3 REST subset onto core.Request events:
//
//	PUT    /{bucket}/{key}                          → core.Put{Key, Data: body}
//	PUT    /{bucket}/{key} + x-amz-copy-source      → core.Copy{Src, Dst}
//	GET    /{bucket}/{key}                          → core.Get
//	DELETE /{bucket}/{key}                          → core.Delete
//
// Buckets are not first-class: the core key is "bucket/key", so there is one
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
	panic("toys3/prod: Handler.ServeHTTP unimplemented (phase 2)")
}

// ParseRequest decodes an HTTP request (body already read) into a core.Op.
// Malformed requests (no key, bad copy source, unsupported method) return an
// error and become 400/405 without reaching the core.
func ParseRequest(r *http.Request, body []byte) (core.Op, error) {
	panic("toys3/prod: ParseRequest unimplemented (phase 2)")
}

var _ http.Handler = (*Handler)(nil)
