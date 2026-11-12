package simulator

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// The buggify sites of the simulator. Each is a place where the fake log or
// the fake object store does something that the real one may do too. A site is
// switched on for some runs and off for others, and when on it fires now and
// then (dst fault.Injector.Maybe). The names are stable plain strings: a report
// and a replay refer to them.
var (
	// SiteLogSlowAnswer: the log applies the call, and the reply comes back late.
	SiteLogSlowAnswer = fault.NewSite("sim.log.slow-answer") //determinismcheck:ok catalog handle, set once at start
	// SiteLogShortPage: a LogRead returns fewer entries than it could (at least
	// one). LogRead promises "up to Limit", and the reader asks again from the
	// slot after the last entry it got.
	SiteLogShortPage = fault.NewSite("sim.log.short-read-page") //determinismcheck:ok catalog handle, set once at start
	// SiteObjectsSlowAnswer: the object store applies the call, and the reply
	// comes back late.
	SiteObjectsSlowAnswer = fault.NewSite("sim.objects.slow-answer") //determinismcheck:ok catalog handle, set once at start
)

// slowAnswer is the extra delay of the reply to call: zero, or (when the site
// for the log or the object store fires) the same range as the Slow fault. The
// call has taken effect already, so this only moves the reply later.
func (st *storage) slowAnswer(r *rng.Rand, call runtime.IOCall) machine.Duration {
	site := SiteObjectsSlowAnswer
	if isLogCall(call) {
		site = SiteLogSlowAnswer
	}
	if !st.s.env.inj.Maybe(fault.Site(site.Name())) {
		return 0
	}
	return machine.Duration(r.Between(int64(slowMin), int64(slowMax)))
}

// shortPage may cut a page of log entries to a shorter one that still has the
// first entry. The reader asks again from where the page ended.
func (st *storage) shortPage(ents []runtime.LogEntry) []runtime.LogEntry {
	if len(ents) < 2 || !st.s.env.inj.Maybe(fault.Site(SiteLogShortPage.Name())) {
		return ents
	}
	return ents[:1+st.s.rand.Intn(len(ents)-1)]
}

func isLogCall(call runtime.IOCall) bool {
	switch call.(type) {
	case runtime.LogAppend, runtime.LogRead, runtime.LogGetEnd, runtime.LogDeleteOld, runtime.LogClaimEpoch:
		return true
	}
	return false
}

// SiteNames returns the names of all buggify sites that a run can reach, in
// order: the sites of the simulator, then the sites of the rebalance machine
// (domain/rebalance sites.go), which gets the fault injector of the run.
func SiteNames() []string {
	return append([]string{SiteLogSlowAnswer.Name(), SiteLogShortPage.Name(), SiteObjectsSlowAnswer.Name()}, rebalance.SiteNames()...)
}
