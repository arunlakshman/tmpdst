package rebalance

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/fault"
)

// everyOther is a fault injector for tests: each site that is on fires on
// its first call, then on every other call.
type everyOther struct {
	on    map[string]bool
	calls map[string]int
	fired map[string]int
}

func newEveryOther(sites ...fault.NamedSite) *everyOther {
	f := &everyOther{on: map[string]bool{}, calls: map[string]int{}, fired: map[string]int{}}
	for _, s := range sites {
		f.on[s.Name()] = true
	}
	return f
}

func (f *everyOther) Maybe(site fault.NamedSite) bool {
	s := site.Name()
	if !f.on[s] {
		return false
	}
	f.calls[s]++
	if f.calls[s]%2 == 1 {
		f.fired[s]++
		return true
	}
	return false
}

func (f *everyOther) MaybeP(site fault.NamedSite, _ float64) bool { return f.Maybe(site) }
func (f *everyOther) Fault(fault.NamedSite, float64) bool         { return false }

// The sites that do not move time must not change any reply.
func TestSitesKeepTheReplies(t *testing.T) {
	f := newEveryOther(SiteStaleTimer, SiteDuplicateReply, SiteLateDrain, SiteLoadRetry)
	h := newBareHarness(t, Config{}, f)
	h.session = 5000
	h.step(RoleChanged{Active: true})
	h.check("loading", h.phase("g"), "loading")
	h.until(100 * msec)
	h.check("served after the retry", h.phase("g"), stateDead)

	a, b := h.stableTwo("g", sec, 1, 1)
	h.hold = true
	h.run([]step{
		{sec + delay, h.join(10, "g", a), nil},
		{sec + delay, hb(11, "g", b, 2), []string{"11 ErrRebalanceInProgress"}},
	})
	h.run([]step{{sec + delay, h.join(12, "g", b), nil}})
	h.check("install", heldOps(h), "[GroupInstall]")
	h.run([]step{
		{sec + delay, hb(13, "g", b, 2), nil},
		{sec + delay, leave(14, "g", "zz"), nil},
	})
	h.check("after install", h.answer(0, runtime.OK), []string{
		"10 OK gen=3 proto=range leader=m1 id=m1 members=[m1:meta-range m2:meta-range]",
		"12 OK gen=3 proto=range leader=m1 id=m2", "13 ErrIllegalGeneration", "14 OK [ErrUnknownMember]",
	})
	h.hold = false
	h.run([]step{
		{sec + delay, syncReq(15, "g", b, 3), nil},
		{sec + delay, syncReq(16, "g", a, 3, as(a, "a"), as(b, "b")), []string{"15 OK assign=b", "16 OK assign=a"}},
		{sec + delay + 5*sec, nil, nil}, // both sessions end: both are removed
	})
	h.check("expired", h.phase("g"), stateEmpty)
	for _, site := range []fault.NamedSite{SiteStaleTimer, SiteDuplicateReply, SiteLateDrain, SiteLoadRetry} {
		if f.fired[site.Name()] == 0 {
			t.Errorf("site %s never fired", site.Name())
		}
	}
}

// SiteTimerLate: a timer fires up to 50 ms late.
func TestSiteTimerLate(t *testing.T) {
	f := newEveryOther(SiteTimerLate)
	h := newHarnessFaults(t, Config{}, f)
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{delay, nil, nil},
		{delay + lateMax, nil, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
	})
	h.check("fired", f.fired[SiteTimerLate.Name()] > 0, true)
}
