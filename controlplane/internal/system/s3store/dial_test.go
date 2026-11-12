package s3store

import (
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// TestStoreDialAndHTTPClient needs no server: the endpoint is a port that
// nothing listens on.
func TestStoreDialAndHTTPClient(t *testing.T) {
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + l.Addr().String()
	_ = l.Close()
	s, err := New(t.Context(), Config{Endpoint: url, Bucket: "b", PathStyle: true, HTTP: &http.Client{}, Region: "eu-west-1"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Put(t.Context(), "k", nil)
	code, out := Classifier{}.Classify(runtime.ObjectPut{}, err)
	if code != runtime.ErrUnavailable || out != runtime.NotApplied {
		t.Errorf("dial: %v %v (%v)", code, out, err)
	}
	if err := s.EnsureBucket(t.Context()); err == nil {
		t.Error("EnsureBucket on a dead endpoint succeeded")
	}
	if !strings.HasPrefix(UniqueBucket(), "mds-") {
		t.Error("UniqueBucket")
	}
}
