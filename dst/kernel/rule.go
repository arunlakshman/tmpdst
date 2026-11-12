package kernel

import "github.com/manifesterio/mds/dst/machine"

// Rule checks one property of a run. Make a fresh Rule for every run, so it
// needs no Reset. A rule reports a broken property through View.Assert.
type Rule interface {
	Name() string
	// Observe runs after every queue pop.
	Observe(v View)
	// AtEnd runs once, after the run (in Finish).
	AtEnd(v View)
}

// View is what a rule sees. It is an interface so that tests can stub it.
type View interface {
	Step() uint64
	Now() Time
	Phase() Phase
	Timeline() *Timeline
	Assert() machine.Assertions
}

// view is the View of a World.
type view struct{ w *World }

func (v view) Step() uint64               { return v.w.steps }
func (v view) Now() Time                  { return v.w.q.now }
func (v view) Phase() Phase               { return v.w.phase }
func (v view) Timeline() *Timeline        { return v.w.tl }
func (v view) Assert() machine.Assertions { return env(v) }
