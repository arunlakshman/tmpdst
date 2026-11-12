// Package fault is cooperative fault injection drawn from one rng stream.
package fault

import "github.com/manifesterio/mds/dst/rng"

// Site is a stable cooperative fault point id. Do not interpolate values into it.
type Site string

// Default rates for Maybe activation and fire.
const (
	MaybeActivateRate = 0.5
	MaybeFireRate     = 0.05
)

// Injector is cooperative fault injection against one Rand stream.
// Invariant: Quiet suppresses Fault only. Maybe and MaybeP still draw.
// Invariant: Maybe and MaybeP latch per-site activation once per Injector lifetime (one run).
type Injector struct {
	rand  *rng.Rand
	quiet bool
	sites map[string]*SiteCount
	order []string // site names in first-seen order, so Sites never ranges over the map
}

// SiteCount is what happened at one Maybe or MaybeP site during a run.
// On is the latch: the site was switched on for the run. Calls is how often the
// site was asked. Fired is how often it answered true. A site that is off has
// Fired 0.
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
	c, known := i.sites[string(site)]
	if !known {
		c = &SiteCount{On: i.rand.Chance(MaybeActivateRate)}
		i.sites[string(site)] = c
		i.order = append(i.order, string(site))
	}
	c.Calls++
	if c.On && i.rand.Chance(fire) {
		c.Fired++
		return true
	}
	return false
}

// Sites returns a copy of the counts of every site that Maybe or MaybeP has
// seen in this run. Fault sites are not counted.
func (i *Injector) Sites() map[string]SiteCount {
	out := make(map[string]SiteCount, len(i.sites))
	for _, name := range i.order {
		out[name] = *i.sites[name]
	}
	return out
}

// Fault fires with probability p. It never fires while Quiet is set.
func (i *Injector) Fault(_ Site, p float64) bool {
	return !i.quiet && i.rand.Chance(p)
}
