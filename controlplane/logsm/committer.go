package logsm

import (
	"context"

	"manifester/core"
)

// Committer appends commands for one Replica with up to K appends in flight on
// consecutive slots. A slot lost to another writer is retried at a later slot:
// no offset was computed from the writer's view, because offsets come from
// applying the log, so a lost or late slot cannot poison a later one.
type Committer struct {
	r   *Replica
	sem chan struct{}
}

// NewCommitter returns a committer with k appends in flight (k >= 1).
func NewCommitter(r *Replica, k int) *Committer {
	return &Committer{r: r, sem: make(chan struct{}, max(k, 1))}
}

// Do appends e and returns the answer derived from this replica's applied
// state once the winning slot has been applied. e.ID is set here. An error is
// ctx's: the command may or may not be in the log (the caller retries under
// the same key, which is safe).
func (c *Committer) Do(ctx context.Context, e Entry) (core.CPDone, error) {
	if err := ctx.Err(); err != nil {
		return core.CPDone{}, err
	}
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return core.CPDone{}, ctx.Err()
	}
	defer func() { <-c.sem }()
	e.ID = c.r.newID()
	for {
		n := c.r.alloc()
		won, err := c.write(ctx, n, e)
		if err != nil {
			c.r.unwatch(n)
			return core.CPDone{}, err
		}
		if won {
			return c.r.Await(ctx, n)
		}
		c.r.unwatch(n)
		c.r.lostSlot()
		c.r.pullPaced(ctx) // learn the new tail (shared, paced) before retrying
	}
}

// write appends e at slot n. The append is conditional, so an unknown outcome
// is retried at the same slot: it either lands, or fails and shows what is
// there, which is ours iff the ID matches.
func (c *Committer) write(ctx context.Context, n core.LogSlot, e Entry) (bool, error) {
	for {
		cur, won, err := c.r.cfg.Log.Append(ctx, int64(n), e)
		if err == nil {
			c.r.observe(n, cur)
			return won || cur.ID == e.ID, nil
		}
		if err := c.r.retried(ctx); err != nil {
			return false, err
		}
	}
}

// lostSlot counts a slot lost to another writer.
func (r *Replica) lostSlot() {
	r.mu.Lock()
	r.st.Lost++
	r.mu.Unlock()
}

// retried counts a retry of an unknown outcome and pauses.
func (r *Replica) retried(ctx context.Context) error {
	r.mu.Lock()
	r.st.Retries++
	r.mu.Unlock()
	return r.cfg.Wait(ctx)
}
