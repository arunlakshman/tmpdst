package logcp

import "context"

// Roles are advisory (decision 51): the slot is the fence, so two nodes that
// both believe they are the appender cost retries, not correctness. The
// active node is the one brokers send commits to; the others are hot standbys
// that tail the log, serve fresh reads and refuse commits with a 503.
//
// The rule that keeps one active node in practice:
//   - rank 0 starts active, the others as standbys;
//   - the active node appends a no-op heartbeat when idle for Heartbeat;
//   - a standby becomes active when it has seen no slot of another writer for
//     TakeoverAfter while its own reads were succeeding (TakeoverAfter 0: only Promote);
//   - an active node steps down when a writer of a LOWER rank appends at a
//     slot written after it became active (the preferred node came back).

func (s *Service) isActive() bool { return s.active.Load() }

// IsActive reports the node's advisory role.
func (s *Service) IsActive() bool { return s.isActive() }

// Promote makes this node the active appender now (operator or test).
func (s *Service) Promote() { s.setActive(true) }

// Demote makes this node a standby; commands already queued still finish.
func (s *Service) Demote() { s.setActive(false) }

func (s *Service) setActive(on bool) {
	r := s.rep
	r.mu.Lock()
	if s.active.Swap(on) == on {
		r.mu.Unlock()
		return
	}
	if on {
		r.promoSlot, r.lowerSeen, r.lastOtherAt = max(r.high, r.applied), false, s.env.Now()
	}
	r.lowerSeen = false
	r.mu.Unlock()
	s.m.roleIs(on)
	s.cfg.Logger.Info("log: role changed", "event", "log_role", "active", on, "node", s.cfg.Name)
}

// roleTick applies the rules above; the run loop calls it.
func (s *Service) roleTick(ctx context.Context) {
	r := s.rep
	now := s.env.Now()
	r.mu.Lock()
	lower, other, pullOK, own, blocked := r.lowerSeen, r.lastOtherAt, r.lastPullOK, r.lastOwnAt, r.blocked
	r.mu.Unlock()
	take := int64(s.cfg.TakeoverAfter)
	switch {
	case s.isActive() && lower:
		s.setActive(false)
	case !s.isActive() && take > 0 && blocked == nil && now-other > take && now-pullOK < take:
		s.setActive(true)
	case s.isActive() && now-own > int64(s.cfg.Heartbeat) && s.q.depth() == 0:
		s.heartbeat(ctx)
	}
}

// heartbeat appends a no-op in the background (at most one at a time).
func (s *Service) heartbeat(ctx context.Context) {
	if !s.hb.CompareAndSwap(false, true) {
		return
	}
	s.env.Go(func() {
		defer s.hb.Store(false)
		s.appendGroup(ctx, s.newEntry(nil), nil)
	})
}
