package groupcoordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"mds/internal/core"
)

// A join whose ctx ends while it is parked gets the ctx error, and the
// member is no longer waiting for a round.
func TestJoinCtxEndsWhileParked(t *testing.T) {
	h := newHarness(t, Config{})
	ctx, cancel := context.WithCancel(t.Context())
	id := "m1"
	done := make(chan joinResult, 1)
	go func() {
		response, err := h.c.JoinGroup(ctx, h.joinReq("g", ""))
		done <- joinResult{response, err}
	}()
	waitUntil(t, func() bool { return peek(h.c, "g").joined[id] })

	cancel()
	r := h.recvJoin(done)
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("canceled join: %+v %v, want context.Canceled", r.response, r.err)
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
		response, err := h.c.JoinGroup(ctx, h.joinReq("g", ""))
		done <- joinResult{response, err}
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
	if r.err != nil || r.response.Err != core.OK || r.response.Generation != 1 || r.response.MemberID != id {
		t.Fatalf("second join: %+v %v", r.response, r.err)
	}
	if v := peek(h.c, "g"); v.joined[id] {
		t.Fatalf("after round: %+v", v)
	}
}
