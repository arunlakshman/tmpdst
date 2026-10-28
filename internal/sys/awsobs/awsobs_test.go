package awsobs

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func family(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

// sum returns the counter total, or histogram sample count, of the series
// whose labels contain all of want.
func sum(mf *dto.MetricFamily, want map[string]string) float64 {
	if mf == nil {
		return 0
	}
	var total float64
next:
	for _, m := range mf.Metric {
		have := map[string]string{}
		for _, l := range m.Label {
			have[l.GetName()] = l.GetValue()
		}
		for k, v := range want {
			if have[k] != v {
				continue next
			}
		}
		if m.Counter != nil {
			total += m.Counter.GetValue()
		} else if m.Histogram != nil {
			total += float64(m.Histogram.GetSampleCount())
		}
	}
	return total
}

func TestDynamoRetriesAndThrottles(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"__type":"com.amazonaws.dynamodb.v20120810#ThrottlingException","message":"slow"}`))
			return
		}
		_, _ = w.Write([]byte(`{"Item":{}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	var logs bytes.Buffer
	rec := New(reg, "manicp", "dynamo", slog.New(slog.NewJSONHandler(&logs, nil)))
	c := dynamodb.New(dynamodb.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(srv.URL), Credentials: credentials.NewStaticCredentialsProvider("a", "b", ""),
		APIOptions: rec.APIOptions(), RetryMaxAttempts: 5,
	})
	if _, err := c.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("t"), Key: map[string]dbtypes.AttributeValue{"pk": &dbtypes.AttributeValueMemberS{Value: "x"}}}); err != nil {
		t.Fatal(err)
	}
	if n := sum(family(t, reg, "manicp_dynamo_attempt_seconds"), map[string]string{"op": "GetItem", "result": "http_400"}); n != 2 {
		t.Errorf("failed attempts = %v, want 2", n)
	}
	if n := sum(family(t, reg, "manicp_dynamo_attempt_seconds"), map[string]string{"op": "GetItem", "result": "ok"}); n != 1 {
		t.Errorf("ok attempts = %v, want 1", n)
	}
	if n := sum(family(t, reg, "manicp_dynamo_retries_total"), map[string]string{"op": "GetItem"}); n != 2 {
		t.Errorf("retries = %v, want 2", n)
	}
	if n := sum(family(t, reg, "manicp_dynamo_throttles_total"), map[string]string{"code": "ThrottlingException"}); n != 2 {
		t.Errorf("throttles = %v, want 2", n)
	}
	if n := sum(family(t, reg, "manicp_dynamo_operation_seconds"), map[string]string{"op": "GetItem", "outcome": "ok"}); n != 1 {
		t.Errorf("operations = %v, want 1", n)
	}
	if n := sum(family(t, reg, "manicp_dynamo_request_bytes"), map[string]string{"op": "GetItem"}); n != 3 {
		t.Errorf("request_bytes samples = %v, want 3", n)
	}
	if got := strings.Count(logs.String(), `"event":"aws_attempt_failed"`); got != 2 {
		t.Errorf("warn lines = %d, want 2:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), `"throttled":true`) {
		t.Errorf("no throttled flag: %s", logs.String())
	}
}

func TestS3Non2xxAndTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "slow") {
			time.Sleep(300 * time.Millisecond)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code><Message>x</Message></Error>`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	var logs bytes.Buffer
	rec := New(reg, "manibroker", "s3", slog.New(slog.NewJSONHandler(&logs, nil)))
	c := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(srv.URL), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("a", "b", ""),
		APIOptions:  rec.APIOptions(), Retryer: aws.NopRetryer{},
	})
	_, err := c.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("k"), Body: strings.NewReader("hello")})
	if err == nil {
		t.Fatal("want error")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("slow"), Body: strings.NewReader("hello")})
	if err == nil {
		t.Fatal("want timeout")
	}
	att := family(t, reg, "manibroker_s3_attempt_seconds")
	if sum(att, map[string]string{"op": "PutObject", "result": "http_503"}) != 1 {
		t.Errorf("no http_503 attempt")
	}
	if sum(att, map[string]string{"op": "PutObject", "result": "timeout"}) != 1 {
		t.Errorf("no timeout attempt: %v", att)
	}
	if sum(family(t, reg, "manibroker_s3_throttles_total"), map[string]string{"code": "SlowDown"}) != 1 {
		t.Error("SlowDown not counted as throttle")
	}
	if !strings.Contains(logs.String(), `"result":"timeout"`) || !strings.Contains(logs.String(), `"result":"http_503"`) {
		t.Errorf("logs: %s", logs.String())
	}
}

func TestQuietCodesLogAtDebug(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","message":"x"}`))
	}))
	defer srv.Close()
	var logs bytes.Buffer
	rec := New(prometheus.NewRegistry(), "manicp", "dynamo", slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	rec.QuietCodes = map[string]bool{"TransactionCanceledException": true}
	c := dynamodb.New(dynamodb.Options{Region: "us-east-1", BaseEndpoint: aws.String(srv.URL),
		Credentials: credentials.NewStaticCredentialsProvider("a", "b", ""), APIOptions: rec.APIOptions()})
	_, _ = c.TransactWriteItems(t.Context(), &dynamodb.TransactWriteItemsInput{})
	if logs.Len() != 0 {
		t.Errorf("quiet code logged at warn: %s", logs.String())
	}
}

func TestLimiter(t *testing.T) {
	l := limiter{perSec: 2}
	now := time.Unix(100, 0)
	for range 2 {
		if _, ok := l.allow(now); !ok {
			t.Fatal("under limit")
		}
	}
	if _, ok := l.allow(now); ok {
		t.Fatal("over limit")
	}
	if n, ok := l.allow(now.Add(time.Second)); !ok || n != 1 {
		t.Fatalf("n=%d ok=%v", n, ok)
	}
}

func TestNilRecorder(t *testing.T) {
	var r *Recorder
	if r.APIOptions() != nil {
		t.Fatal("nil recorder must add nothing")
	}
}
