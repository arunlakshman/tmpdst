package main

import (
	"context"
	"flag"
	"testing"
	"time"
)

func TestRunServesUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)
	if err := run(ctx, []string{"-dir", t.TempDir(), "-addr", "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	if err := run(ctx, []string{"-h"}); err != flag.ErrHelp {
		t.Errorf("help: %v", err)
	}
	if err := run(ctx, []string{"-nope"}); err == nil {
		t.Error("bad flag accepted")
	}
	if err := run(ctx, []string{"-dir", "/dev/null/x"}); err == nil {
		t.Error("unusable dir accepted")
	}
	if err := run(ctx, []string{"-dir", t.TempDir(), "-addr", "bad:addr:x"}); err == nil {
		t.Error("bad address accepted")
	}
}
