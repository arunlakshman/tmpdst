package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"manifester/cp/cpobs"
	"manifester/cp/dynamo"
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

func TestRunServesMemStoreAndStops(t *testing.T) {
	addr, maddr := freeAddr(t), freeAddr(t)
	manifest := filepath.Join(t.TempDir(), "m.json")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-addr", addr, "-metrics-addr", maddr, "-run-id", "t1", "-manifest-out", manifest}, &logs)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + maddr + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("metrics port never came up: ", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cp port never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code %d\n%s", code, logs.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("did not stop")
	}
	for _, want := range []string{"manicp starting", "manicp listening", "manifest written", "manicp stopped"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing %q in logs", want)
		}
	}
	if _, err := os.Stat(manifest); err != nil {
		t.Fatal(err)
	}
}

func TestRunErrors(t *testing.T) {
	var logs bytes.Buffer
	cases := map[string][]string{
		"bad level":     {"-log-level", "loud"},
		"unknown store": {"-store", "nope", "-metrics-addr", ""},
		"bad metrics":   {"-metrics-addr", "256.0.0.1:x"},
		"bad addr":      {"-metrics-addr", "", "-addr", "256.0.0.1:x"},
	}
	for name, args := range cases {
		logs.Reset()
		if code := run(context.Background(), args, &logs); code != 1 {
			t.Errorf("%s: code %d", name, code)
		}
	}
	if code := run(context.Background(), []string{"-bogus"}, &logs); code != 2 {
		t.Fatal(code)
	}
	if code := run(context.Background(), []string{"-h"}, &logs); code != 0 {
		t.Fatal(code)
	}
}

func TestManifestFailureIsNotFatal(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file")
	os.WriteFile(f, nil, 0o644)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var logs bytes.Buffer
	code := run(ctx, []string{"-addr", freeAddr(t), "-metrics-addr", "", "-manifest-out", filepath.Join(f, "x", "m.json")}, &logs)
	if code != 0 || !strings.Contains(logs.String(), "manifest_failed") {
		t.Fatalf("code %d\n%s", code, logs.String())
	}
}

func TestDynamoStoreAgainstFakeEndpoint(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(400)
		w.Write([]byte(`{"__type":"com.amazon.coral.service#ValidationException","message":"fake"}`))
	}))
	defer srv.Close()
	var logs bytes.Buffer
	args := []string{"-store", "dynamo", "-dynamo-endpoint", srv.URL, "-metrics-addr", "", "-create-tables", "-addr", freeAddr(t)}
	if code := run(context.Background(), args, &logs); code != 1 {
		t.Fatalf("create-tables against a failing endpoint must exit 1, got %d\n%s", code, logs.String())
	}
	// Without -create-tables the store opens and the server runs until ctx ends.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	args = []string{"-store", "dynamo", "-dynamo-endpoint", srv.URL, "-metrics-addr", "", "-addr", freeAddr(t)}
	if code := run(ctx, args, &logs); code != 0 {
		t.Fatalf("code %d\n%s", code, logs.String())
	}
}

func TestOpenStoreSequencerFlag(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	m := cpobs.New()
	f, _, err := parseFlags([]string{"-store", "dynamo", "-dynamo-endpoint", "http://127.0.0.1:1"}, &bytes.Buffer{})
	if err != nil || !f.sequencer || f.seqPipeline != 4 || f.seqMaxItems != 100 || f.seqCache != 4096 {
		t.Fatalf("defaults: %+v %v", f, err)
	}
	st, closeStore, err := openStore(context.Background(), f, m)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.(*dynamo.Sequencer); !ok {
		t.Fatalf("default dynamo store is %T, want the sequencer", st)
	}
	closeStore()
	f.sequencer = false
	st, closeStore, err = openStore(context.Background(), f, m)
	if _, ok := st.(*dynamo.Store); !ok || err != nil {
		t.Fatalf("-sequencer=false gives %T %v", st, err)
	}
	closeStore()
}
