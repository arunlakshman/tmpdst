package groupcoordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"manifester/internal/core"
)

// A join whose ctx ends while it is parked gets the ctx error, and the
// member is no longer waiting for a round.
func TestJoinCtxEndsWhileParked(t *testing.T) {
	h := newHarness(t, Config{})
	ctx, cancel := context.WithCancel(t.Context())
	id := "m1"
	done := make(chan joinResult, 1)
	go func() {
		resp, err := h.c.JoinGroup(ctx, h.joinReq("g", ""))
		done <- joinResult{resp, err}
	}()
	waitUntil(t, func() bool { return peek(h.c, "g").joined[id] })

	cancel()
	r := h.recvJoin(done)
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("canceled join: %+v %v, want context.Canceled", r.resp, r.err)
	}
	if peek(h.c, "g").joined[id] {
		t.Fatal("canceled join is still parked")
	}
}

// A round that ends after a parked join was canceled does not answer that
// join again. The member stays in the group, and the other join gets the answer.
func TestJoinCanceledThenRoundCompletes(t *testing.T) {
	h := newHarness(t, Config{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan joinResult, 1)
	go func() {
		resp, err := h.c.JoinGroup(ctx, h.joinReq("g", ""))
		done <- joinResult{resp, err}
	}()
	waitUntil(t, func() bool { return peek(h.c, "g").joined["m1"] })
	cancel()
	if r := h.recvJoin(done); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("canceled join: %v", r.err)
	}

	// The canceled member did not rejoin, so the round waits for it until the
	// rebalance timeout. Then the round ends and the parked join gets its answer.
	id, ch := h.join("g")
	h.clock.Advance(time.Duration(h.session) * time.Millisecond)
	r := h.recvJoin(ch)
	if r.err != nil || r.resp.Err != core.OK || r.resp.Generation != 1 || r.resp.MemberID != id {
		t.Fatalf("second join: %+v %v", r.resp, r.err)
	}
	if v := peek(h.c, "g"); v.joined[id] {
		t.Fatalf("after round: %+v", v)
	}
}
