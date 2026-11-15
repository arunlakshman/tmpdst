// Package fault is cooperative fault injection drawn from one rng stream.
package fault

import "github.com/manifesterio/mds/dst/rng"

// Default rates for Maybe activation and fire.
const (
	MaybeActivateRate = 0.5
	MaybeFireRate     = 0.05
)

// Injector is cooperative fault injection against one Rand stream.
// Invariant: Quiet suppresses Fault only. Maybe and MaybeP still draw.
// Invariant: Maybe and MaybeP latch per-site activation once per Injector lifetime (one run).
// Invariant: Fault sites are always On and never latch. They are counted like the others.
type Injector struct {
	rand  *rng.Rand
	quiet bool
	sites map[string]*SiteCount
	order []string // site names in first-seen order, so Sites never ranges over the map
}

// SiteCount is what happened at one site during a run.
// On is the latch: the site was switched on for the run. A Fault site is always On,
// because it has no latch. Calls is how often the site was asked. Fired is how often
// it answered true. A site that is off has Fired 0.
type SiteCount struct {
	On    bool
	Calls uint64
	Fired uint64
}

// New returns an injector that draws from r.
func New(r *rng.Rand) *Injector {
	return &Injector{rand: r, sites: map[string]*SiteCount{}}
}

// SetQuiet enables or disables the quiet tail.
func (i *Injector) SetQuiet(q bool) { i.quiet = q }

// Quiet reports whether Fault is suppressed.
func (i *Injector) Quiet() bool { return i.quiet }

// Maybe activates the site for the run with probability MaybeActivateRate on first
// sight, then fires with probability MaybeFireRate on each call when active.
func (i *Injector) Maybe(site Site) bool {
	return i.maybe(site, MaybeFireRate)
}

// MaybeP activates the site like Maybe, then fires with probability p when active.
func (i *Injector) MaybeP(site Site, p float64) bool {
	return i.maybe(site, p)
}

func (i *Injector) maybe(site Site, fire float64) bool {
	c := i.count(site.Name(), func() *SiteCount {
		return &SiteCount{On: i.rand.Chance(MaybeActivateRate)}
	})
	c.Calls++
	if c.On && i.rand.Chance(fire) {
		c.Fired++
		return true
	}
	return false
}

// count returns the counts for name. On first sight it stores the counts made by
// first. first is called only once per name, so it is the only place a draw can happen.
func (i *Injector) count(name string, first func() *SiteCount) *SiteCount {
	c, known := i.sites[name]
	if !known {
		c = first()
		i.sites[name] = c
		i.order = append(i.order, name)
	}
	return c
}

// Sites returns a copy of the counts of every site that Maybe, MaybeP or Fault has
// seen in this run.
func (i *Injector) Sites() map[string]SiteCount {
	out := make(map[string]SiteCount, len(i.sites))
	for _, name := range i.order {
		out[name] = *i.sites[name]
	}
	return out
}

// Fault fires with probability p. It counts the call on the site. It never fires
// and never draws while Quiet is set.
func (i *Injector) Fault(site Site, p float64) bool {
	c := i.count(site.Name(), func() *SiteCount {
		return &SiteCount{On: true}
	})
	c.Calls++
	if i.quiet {
		return false
	}
	if i.rand.Chance(p) {
		c.Fired++
		return true
	}
	return false
}
