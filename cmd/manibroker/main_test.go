package main

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitDial(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never came up", addr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunMemBrokerStartsAndStops(t *testing.T) {
	listen, maddr := freeAddr(t), freeAddr(t)
	manifest := filepath.Join(t.TempDir(), "m.json")
	stop := make(chan struct{})
	done := make(chan int, 1)
	var logs bytes.Buffer
	go func() {
		done <- run([]string{"-objects", "mem", "-listen", listen, "-metrics-addr", maddr, "-run-id", "bt",
			"-debug", "-manifest-out", manifest, "-stop-grace", "1s"}, &logs, stop)
	}()
	waitDial(t, listen)
	waitDial(t, maddr)
	resp, err := http.Get("http://" + maddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	body.ReadFrom(resp.Body)
	resp.Body.Close()
	if !strings.Contains(body.String(), "manibroker_open_connections") {
		t.Error("open_connections gauge missing")
	}
	close(stop)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code %d\n%s", code, logs.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("did not stop")
	}
	for _, want := range []string{"manibroker starting", "manibroker listening", "manibroker tuning", "manifest written", "manibroker stopped"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
	b, err := os.ReadFile(manifest)
	if err != nil || !strings.Contains(string(b), `"max_segment_items"`) {
		t.Fatalf("%v %s", err, b)
	}
}

func TestRunErrors(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	var logs bytes.Buffer
	busy, _ := net.Listen("tcp", "127.0.0.1:0")
	defer busy.Close()
	cases := map[string][]string{
		"bad level":       {"-log-level", "loud"},
		"bad tuning":      {"-segment-bytes", "x", "-objects", "mem", "-metrics-addr", ""},
		"unknown objects": {"-objects", "nope", "-metrics-addr", ""},
		"metrics busy":    {"-objects", "mem", "-metrics-addr", busy.Addr().String()},
		"listen busy":     {"-objects", "mem", "-metrics-addr", "", "-listen", busy.Addr().String()},
		"bad advertise":   {"-objects", "mem", "-metrics-addr", "", "-listen", "127.0.0.1:0", "-advertise", "nohostport"},
		"bad adv port":    {"-objects", "mem", "-metrics-addr", "", "-listen", "127.0.0.1:0", "-advertise", "h:xx"},
	}
	for name, args := range cases {
		logs.Reset()
		if code := run(args, &logs, stop); code != 1 {
			t.Errorf("%s: code %d\n%s", name, code, logs.String())
		}
	}
	if run([]string{"-bogus"}, &logs, stop) != 2 || run([]string{"-h"}, &logs, stop) != 0 {
		t.Fatal("flag errors")
	}
}

func TestOpenObjectsS3(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	stop := make(chan struct{})
	close(stop)
	var logs bytes.Buffer
	// s3 store with a stub endpoint and -s3-create-bucket: the node starts, then stops at once.
	code := run([]string{"-objects", "s3", "-s3-endpoint", srv.URL, "-s3-path-style", "-s3-create-bucket", "-metrics-addr", "",
		"-listen", freeAddr(t), "-stop-grace", "1s"}, &logs, stop)
	if code != 0 {
		t.Fatalf("code %d\n%s", code, logs.String())
	}
	if !strings.Contains(logs.String(), "manibroker starting") {
		t.Fatal(logs.String())
	}
}
