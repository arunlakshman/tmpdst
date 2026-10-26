package runinfo

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRunIDPrecedence(t *testing.T) {
	t.Setenv(EnvRunID, "from-env")
	if i := New("svc", "", -1); i.RunID != "from-env" || i.RunIDFresh {
		t.Fatalf("%+v", i)
	}
	if i := New("svc", "flag", -1); i.RunID != "flag" {
		t.Fatalf("%+v", i)
	}
	t.Setenv(EnvRunID, "")
	if i := New("svc", "", -1); !strings.HasPrefix(i.RunID, "run-") || !i.RunIDFresh {
		t.Fatalf("%+v", i)
	}
}

func TestLoggerCarriesRunFields(t *testing.T) {
	var buf bytes.Buffer
	i := New("manibroker", "r1", 7)
	NewLogger(&buf, 0, i).Info("hello", "k", 1)
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{"run_id": "r1", "service": "manibroker", "broker_id": float64(7), "msg": "hello", "k": float64(1)} {
		if m[k] != want {
			t.Errorf("%s = %v, want %v", k, m[k], want)
		}
	}
	for _, k := range []string{"version", "git_sha", "time", "level"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing %s", k)
		}
	}
	buf.Reset()
	NewLogger(&buf, 0, New("manicp", "r1", -1)).Info("x")
	if strings.Contains(buf.String(), "broker_id") {
		t.Error("manicp line has broker_id")
	}
}

func TestParseLevel(t *testing.T) {
	if _, err := ParseLevel("warn"); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseLevel("loud"); err == nil {
		t.Fatal("want error")
	}
}

func TestRegister(t *testing.T) {
	reg := prometheus.NewRegistry()
	Register(reg, "x", New("svc", "r9", -1))
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
		if mf.GetName() == "x_build_info" {
			var lbl strings.Builder
			for _, l := range mf.Metric[0].Label {
				lbl.WriteString(l.GetName() + "=" + l.GetValue() + ",")
			}
			if !strings.Contains(lbl.String(), "run_id=r9") || !strings.Contains(lbl.String(), "go_version=go") {
				t.Errorf("labels %s", lbl.String())
			}
		}
	}
	for _, n := range []string{"x_build_info", "x_start_time_seconds", "x_uptime_seconds"} {
		if !got[n] {
			t.Errorf("missing %s", n)
		}
	}
}

func TestManifestFile(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.Int("a", 1, "")
	fs.String("b", "x", "")
	_ = fs.Parse([]string{"-b", "y"})
	i := New("svc", "r2", -1)
	m := NewManifest(i, fs, "eu-west-1", map[string]any{"segment_bytes": 5})
	dir := t.TempDir()
	dest, err := Write(t.Context(), m, WriteOptions{Dest: filepath.Join(dir, "sub") + "/"})
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(dest) != "svc-r2.json" {
		t.Fatal(dest)
	}
	b, _ := os.ReadFile(dest) //nolint:gosec // G304: path is a test temp file
	var got Manifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.RunID != "r2" || got.Flags["b"] != "y" || got.Flags["a"] != "1" || len(got.FlagsSet) != 1 || got.Region != "eu-west-1" {
		t.Fatalf("%+v", got)
	}
}

func TestManifestS3DestErrorsAreReturned(t *testing.T) {
	m := NewManifest(New("svc", "r", -1), flag.NewFlagSet("t", flag.ContinueOnError), "us-east-1", nil)
	if _, err := Write(t.Context(), m, WriteOptions{Dest: "s3://bucketonly"}); err == nil {
		t.Fatal("want error")
	}
}

func TestIMDS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			_, _ = w.Write([]byte("tok"))
		case r.Header.Get("X-aws-ec2-metadata-token") != "tok":
			w.WriteHeader(http.StatusUnauthorized)
		case strings.HasSuffix(r.URL.Path, "instance-type"):
			_, _ = w.Write([]byte("c7g.xlarge"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	got := FetchIMDS(t.Context(), srv.URL, time.Second)
	if got["instance_type"] != "c7g.xlarge" || len(got) != 1 {
		t.Fatalf("%v", got)
	}
	if FetchIMDS(t.Context(), "http://127.0.0.1:1", 50*time.Millisecond) != nil {
		t.Fatal("unreachable IMDS must give nil")
	}
}

func TestWriteEmptyDestAndIMDS(t *testing.T) {
	m := NewManifest(New("svc", "r3", -1), flag.NewFlagSet("t", flag.ContinueOnError), "", nil)
	if d, err := Write(t.Context(), m, WriteOptions{}); d != "" || err != nil {
		t.Fatal(d, err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = w.Write([]byte("tok"))
			return
		}
		_, _ = w.Write([]byte("i-1"))
	}))
	defer srv.Close()
	dest := filepath.Join(t.TempDir(), "m.json")
	if _, err := Write(t.Context(), m, WriteOptions{Dest: dest, IMDS: true, IMDSEndpoint: srv.URL}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(dest) //nolint:gosec // G304: path is a test temp file
	if !strings.Contains(string(b), "i-1") {
		t.Fatalf("no instance metadata: %s", b)
	}
}

func TestWriteFileFailure(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file")
	_ = os.WriteFile(f, nil, 0o600) // test setup; the test asserts on the failure path below
	m := NewManifest(New("svc", "r", -1), flag.NewFlagSet("t", flag.ContinueOnError), "", nil)
	if _, err := Write(t.Context(), m, WriteOptions{Dest: filepath.Join(f, "sub", "m.json")}); err == nil {
		t.Fatal("want error")
	}
}

func TestWriteS3(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	m := NewManifest(New("svc", "r4", -1), flag.NewFlagSet("t", flag.ContinueOnError), "us-east-1", nil)
	dest, err := Write(t.Context(), m, WriteOptions{Dest: "s3://bkt/dir/", Region: "us-east-1", S3Endpoint: srv.URL, S3PathStyle: true})
	if err != nil || dest != "s3://bkt/dir/svc-r4.json" {
		t.Fatal(dest, err)
	}
	if path != "/bkt/dir/svc-r4.json" {
		t.Fatal(path)
	}
}
