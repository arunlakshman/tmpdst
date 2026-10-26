package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProducer struct {
	n      atomic.Int64
	closed atomic.Bool
}

func (f *fakeProducer) Produce(ctx context.Context, r *kgo.Record, p func(*kgo.Record, error)) {
	f.n.Add(1)
	time.Sleep(time.Millisecond)
	p(r, nil)
}
func (f *fakeProducer) BufferedProduceRecords() int64   { return 3 }
func (f *fakeProducer) Flush(ctx context.Context) error { return nil }
func (f *fakeProducer) Close()                          { f.closed.Store(true) }

func TestParseArgs(t *testing.T) {
	var eb bytes.Buffer
	c, ok := parseArgs([]string{"-topic", "t", "-brokers", "a:1,b:2", "-partitions", "4", "-num-clients", "2"}, &eb)
	if !ok || c.topic != "t" || len(c.brokers) != 2 || c.parts != 4 || c.clients != 2 || c.buffered != 5000 {
		t.Fatalf("%+v %v", c, ok)
	}
	for _, bad := range [][]string{{}, {"-topic", "t", "-record-size", "4"}, {"-topic", "t", "-num-clients", "0"}, {"-nope"}} {
		if _, ok := parseArgs(bad, &eb); ok {
			t.Errorf("%v accepted", bad)
		}
	}
}

func TestMainRunUsage(t *testing.T) {
	var out, eb bytes.Buffer
	if mainRun(nil, &out, &eb, nil) != 2 || mainRun([]string{"other"}, &out, &eb, nil) != 2 {
		t.Fatal("usage")
	}
	if !strings.Contains(eb.String(), "usage: manibench") {
		t.Fatal(eb.String())
	}
	if mainRun([]string{"benchmark-producer"}, &out, &eb, nil) != 2 {
		t.Fatal("missing topic")
	}
}

func TestCreateTopic(t *testing.T) {
	var body string
	status := 200
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b bytes.Buffer
		_, _ = b.ReadFrom(r.Body)
		body = r.URL.Path + " " + b.String()
		w.WriteHeader(status)
	}))
	defer srv.Close()
	if err := createTopic(t.Context(), srv.URL+"/", "tt", 3); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, "/v1/create-topic") || !strings.Contains(body, `"partitions":3`) {
		t.Fatal(body)
	}
	status = 500
	if err := createTopic(t.Context(), srv.URL, "tt", 3); err == nil {
		t.Fatal("want status error")
	}
	if err := createTopic(t.Context(), "http://127.0.0.1:1", "tt", 3); err == nil {
		t.Fatal("want dial error")
	}

	var out, eb bytes.Buffer
	if mainRun([]string{"benchmark-producer", "-topic", "t", "-create-partitions", "2", "-cp-url", srv.URL}, &out, &eb, nil) != 1 {
		t.Fatal("create failure must exit 1")
	}
	if !strings.Contains(eb.String(), "create topic:") {
		t.Fatal(eb.String())
	}
}

func TestClientOpts(t *testing.T) {
	base := benchConfig{topic: "t", brokers: []string{"127.0.0.1:1"}, buffered: 10}
	n0 := len(clientOpts(base))
	base.inflight = 1
	base.parts = 3
	if len(clientOpts(base)) != n0+1 {
		t.Fatal("inflight adds an option")
	}
	cl, err := kgo.NewClient(clientOpts(base)...)
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
	p, err := newKgoClient(clientOpts(benchConfig{topic: "t", brokers: []string{"127.0.0.1:1"}, buffered: 5})...)
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	if _, err := newKgoClient(kgo.MaxBufferedRecords(-5)); err == nil {
		t.Fatal("want invalid option error")
	}
}

func TestMakeValue(t *testing.T) {
	v := makeValue(7, 9, 12)
	if len(v) != 12 || binary.BigEndian.Uint32(v) != 7 || binary.BigEndian.Uint32(v[4:]) != 9 || v[8] != byte('a'+8%26) {
		t.Fatalf("%v", v)
	}
}

func TestRunWithFakeClients(t *testing.T) {
	var fakes []*fakeProducer
	nc := func(opts ...kgo.Opt) (producer, error) {
		f := &fakeProducer{}
		fakes = append(fakes, f)
		return f, nil
	}
	var out, eb bytes.Buffer
	code := mainRun([]string{"benchmark-producer", "-topic", "t", "-num-clients", "2", "-partitions", "3",
		"-duration", "120ms", "-interval", "30ms", "-record-size", "16"}, &out, &eb, nc)
	if code != 0 {
		t.Fatalf("code %d %s", code, eb.String())
	}
	if len(fakes) != 2 || !fakes[0].closed.Load() || fakes[0].n.Load() == 0 || fakes[1].n.Load() == 0 {
		t.Fatalf("fakes %+v", fakes)
	}
	if !strings.Contains(out.String(), "\n") || out.Len() == 0 {
		t.Fatal("no report output")
	}
}

func TestRunClientError(t *testing.T) {
	nc := func(opts ...kgo.Opt) (producer, error) { return nil, errors.New("nope") }
	var out, eb bytes.Buffer
	if run(t.Context(), benchConfig{clients: 1, dur: time.Millisecond, every: time.Second}, &out, &eb, nc) != 1 || !strings.Contains(eb.String(), "new client: nope") {
		t.Fatal(eb.String())
	}
}

func TestCompressionFlag(t *testing.T) {
	var eb bytes.Buffer
	c, ok := parseArgs([]string{"-topic", "t"}, &eb)
	if !ok || c.compression != "none" {
		t.Fatalf("default %q %v", c.compression, ok)
	}
	for _, name := range []string{"none", "gzip", "snappy", "LZ4", "zstd", ""} {
		if _, known := codecByName(name); !known {
			t.Errorf("%q unknown", name)
		}
	}
	if _, ok := parseArgs([]string{"-topic", "t", "-compression", "lz5"}, &eb); ok {
		t.Fatal("unknown codec accepted")
	}
	if c, ok := parseArgs([]string{"-topic", "t", "-compression", "lz4"}, &eb); !ok || len(clientOpts(c)) == 0 {
		t.Fatal("lz4 rejected")
	}
}
