package it

import (
	"context"
	"os"
	"testing"
	"time"

	"manifester/internal/sys/s3store"
)

// fullStack is brokers over the live machine/gRPC control plane (via newStack)
// with segment objects in SeaweedFS. Skipped unless MANIFESTER_S3_ENDPOINT is
// set (scripts/it-up.sh prints it). Dynamo is exercised by manicp -create and
// by unit tests of exec.CreateTable; this path's unique coverage is real S3.
func fullStack(t *testing.T) *stack {
	t.Helper()
	s3ep := os.Getenv("MANIFESTER_S3_ENDPOINT")
	if s3ep == "" {
		t.Skip("MANIFESTER_S3_ENDPOINT not set (scripts/it-up.sh)")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	objs, err := s3store.New(ctx, s3store.Config{Endpoint: s3ep, Bucket: s3store.UniqueBucket(), PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := objs.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	return newStack(t, nil, objs, s3store.Classifier{})
}

func TestFullStackRoundTrip(t *testing.T) {
	s := fullStack(t)
	s.addBrokers(2)
	create(s)
	runRoundTrip(t, s)
}

func TestFullStackKillBroker(t *testing.T) {
	s := fullStack(t)
	s.addBrokers(3)
	create(s)
	runKill(t, s)
}
