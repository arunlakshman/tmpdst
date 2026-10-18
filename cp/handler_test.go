package cp_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/memstore"
)

// failStore answers every call with err (and code for business paths).
type failStore struct {
	*memstore.Store
	err  error
	code core.ErrCode
}

func (f failStore) Register(context.Context, core.Register) (core.RegisterResp, error) {
	return core.RegisterResp{}, f.err
}
func (f failStore) InitProducer(context.Context) (core.InitProducerResp, error) {
	return core.InitProducerResp{}, f.err
}
func (f failStore) CommitSegment(context.Context, core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	return core.CommitSegmentResp{}, f.code, f.err
}
func (f failStore) Lookup(context.Context, core.Lookup) (core.LookupResp, core.ErrCode, error) {
	return core.LookupResp{}, f.code, f.err
}
func (f failStore) DescribeTopics(context.Context, core.DescribeTopics) (core.DescribeTopicsResp, error) {
	return core.DescribeTopicsResp{}, f.err
}
func (f failStore) CreateTopic(context.Context, string, int32) (core.ErrCode, error) {
	return f.code, f.err
}

func postJSON(t *testing.T, h http.Handler, path, body string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	b, _ := io.ReadAll(rec.Body)
	return rec.Code, strings.TrimSpace(string(b))
}

const node = `{"node":"n1","inc":"00112233445566778899aabbccddeeff"}`

func TestHandlerHappyPath(t *testing.T) {
	h := cp.NewHandler(memstore.New())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != 200 || rec.Body.String() != "ok\n" {
		t.Fatalf("healthz %d %q", rec.Code, rec.Body)
	}
	steps := []struct{ path, body, want string }{
		{"/v1/create-topic", `{"topic":"t","partitions":2}`, `{}`},
		{"/v1/register", `{"self":` + node + `}`, `"epoch":`},
		{"/v1/init-producer", `{}`, `"producer":`},
		{"/v1/describe-topics", `{"topics":["t"]}`, `"name":"t"`},
		{"/v1/lookup", `{"topic":"t","partition":0,"from":0,"max_bytes":100}`, `"high_watermark":0`},
		{"/v1/commit-segment", `{"segment_key":"s","owner":` + node + `,"epoch":1,"batches":[{"topic":"t","partition":1,"records":3,"seg_key":"s","seg_off":0,"seg_len":9}]}`, `"assigned"`},
	}
	for _, s := range steps {
		code, body := postJSON(t, h, s.path, s.body)
		if code != 200 || !strings.Contains(body, s.want) {
			t.Errorf("%s: %d %s (want %s)", s.path, code, body, s.want)
		}
	}
	// the commit above registered epoch 1 only if register gave 1; either way
	// a lookup now reports the topic's watermark without error.
	if code, body := postJSON(t, h, "/v1/lookup", `{"topic":"t","partition":1,"from":0,"max_bytes":100}`); code != 200 {
		t.Errorf("lookup %d %s", code, body)
	}
}

func TestHandlerInvalidInput(t *testing.T) {
	h := cp.NewHandler(memstore.New())
	for _, path := range []string{"/v1/register", "/v1/commit-segment", "/v1/lookup"} {
		code, body := postJSON(t, h, path, `{not json`)
		if code != 200 || !strings.Contains(body, `"err":"ErrInvalid"`) {
			t.Errorf("%s malformed: %d %s", path, code, body)
		}
	}
	bad := `{"node":"n","inc":"zz"}`
	if _, body := postJSON(t, h, "/v1/register", `{"self":`+bad+`}`); !strings.Contains(body, "ErrInvalid") {
		t.Errorf("register bad node: %s", body)
	}
	if _, body := postJSON(t, h, "/v1/commit-segment", `{"owner":`+bad+`}`); !strings.Contains(body, "ErrInvalid") {
		t.Errorf("commit bad owner: %s", body)
	}
}

func TestHandlerBusinessCodes(t *testing.T) {
	h := cp.NewHandler(failStore{Store: memstore.New(), code: core.ErrFenced})
	for _, path := range []string{"/v1/commit-segment", "/v1/lookup", "/v1/create-topic"} {
		code, body := postJSON(t, h, path, `{"owner":`+node+`}`)
		if code != 200 || !strings.Contains(body, `"err":"ErrFenced"`) {
			t.Errorf("%s: %d %s", path, code, body)
		}
	}
}

func TestHandlerStoreErrors(t *testing.T) {
	paths := map[string]string{
		"/v1/register": `{"self":` + node + `}`, "/v1/init-producer": `{}`, "/v1/commit-segment": `{"owner":` + node + `}`,
		"/v1/lookup": `{}`, "/v1/describe-topics": `{}`, "/v1/create-topic": `{}`,
	}
	cases := []struct {
		err  error
		want int
	}{
		{cp.ErrUnavailable, 503}, {context.Canceled, 503}, {context.DeadlineExceeded, 503}, {errors.New("boom"), 500},
	}
	for _, c := range cases {
		h := cp.NewHandler(failStore{Store: memstore.New(), err: c.err})
		for path, body := range paths {
			if code, _ := postJSON(t, h, path, body); code != c.want {
				t.Errorf("%s with %v: %d, want %d", path, c.err, code, c.want)
			}
		}
	}
}
