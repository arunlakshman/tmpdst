package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// fakeStore is an in-memory S3 for the checkpoint check. Keys are returned in
// pages of pageSize so the paginator is exercised.
type fakeStore struct {
	keys     []string
	bodies   map[string]io.ReadCloser
	listErr  error
	getErr   error
	pageSize int
}

func (f *fakeStore) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	var matching []string
	for _, k := range f.keys {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) {
			matching = append(matching, k)
		}
	}
	start := 0
	if in.ContinuationToken != nil {
		for i, k := range matching {
			if k == *in.ContinuationToken {
				start = i
			}
		}
	}
	end := len(matching)
	out := &s3.ListObjectsV2Output{}
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(matching[end])
	}
	for _, k := range matching[start:end] {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}
	return out, nil
}

func (f *fakeStore) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &s3.GetObjectOutput{Body: f.bodies[aws.ToString(in.Key)]}, nil
}

type badBody struct{}

func (badBody) Read([]byte) (int, error) { return 0, errors.New("connection reset") }
func (badBody) Close() error             { return nil }

func TestNewestKeyPicksTheLastKeyAcrossPages(t *testing.T) {
	store := &fakeStore{
		keys: []string{
			"cp/checkpoint/00000000000000000010",
			"cp/checkpoint/00000000000000000030",
			"cp/checkpoint/00000000000000000020",
			"cp/other/99999999999999999999",
		},
		pageSize: 1,
	}
	got, err := newestKey(t.Context(), store, "b", "cp/checkpoint/")
	if err != nil || got != "cp/checkpoint/00000000000000000030" {
		t.Fatalf("newestKey = %q, %v; want the slot-30 key", got, err)
	}
}

func TestNewestKeyFailsWhenThereIsNoCheckpoint(t *testing.T) {
	if _, err := newestKey(t.Context(), &fakeStore{keys: []string{"cp/log/1"}}, "b", "cp/checkpoint/"); err == nil {
		t.Fatal("want an error when no checkpoint object exists")
	}
	if _, err := newestKey(t.Context(), &fakeStore{listErr: errors.New("denied")}, "b", "cp/checkpoint/"); err == nil {
		t.Fatal("want the list error")
	}
}

func TestFetchCheckpointReadsTheNewestObject(t *testing.T) {
	store := &fakeStore{
		keys: []string{"c/00000000000000000001", "c/00000000000000000002"},
		bodies: map[string]io.ReadCloser{
			"c/00000000000000000001": io.NopCloser(strings.NewReader("old")),
			"c/00000000000000000002": io.NopCloser(strings.NewReader("new")),
		},
	}
	got := fetchCheckpoint(t.Context(), store, "b", "c/")
	if got.findErr != nil || got.readErr != nil || string(got.data) != "new" || got.key != "c/00000000000000000002" {
		t.Fatalf("fetchCheckpoint = %+v, want key ...02 with data new", got)
	}
}

func TestFetchCheckpointReportsEachFailure(t *testing.T) {
	missing := fetchCheckpoint(t.Context(), &fakeStore{}, "b", "c/")
	if missing.findErr == nil || missing.key != "c/" {
		t.Fatalf("empty store: %+v, want a find error that names the prefix", missing)
	}
	noGet := fetchCheckpoint(t.Context(), &fakeStore{keys: []string{"c/1"}, getErr: errors.New("denied")}, "b", "c/")
	if noGet.findErr == nil || noGet.key != "c/1" {
		t.Fatalf("get error: %+v, want a find error that names the key", noGet)
	}
	badRead := fetchCheckpoint(t.Context(), &fakeStore{keys: []string{"c/1"}, bodies: map[string]io.ReadCloser{"c/1": badBody{}}}, "b", "c/")
	if badRead.findErr != nil || badRead.readErr == nil {
		t.Fatalf("body error: %+v, want only a read error", badRead)
	}
}

func TestCheckCheckpointStopsWhenNoObject(t *testing.T) {
	c, out := captured()
	checkCheckpoint(t.Context(), c, &fakeStore{}, "b", "c/", nil, false, 0)
	if c.failed != 1 || !strings.Contains(out.String(), "checkpoint object exists in S3") {
		t.Fatalf("failed=%d\n%s", c.failed, out.String())
	}
}
