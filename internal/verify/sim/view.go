package sim

import "manifester/internal/core"

// view is the View invariants get after each step. Ground truth comes from the
// engines by type assertion; with no such engine the truths are empty.
type view struct{ r *run }

func (v view) Step() uint64            { return v.r.steps }
func (v view) Now() Time               { return v.r.now() }
func (v view) Phase() Phase            { return v.r.phase }
func (v view) Timeline() Timeline      { return v.r.tl }
func (v view) History() History        { return v.r.hist }
func (v view) Assert() core.Assertions { return v.r.as }

func (v view) Alive(ref core.NodeInstance) bool {
	n := v.r.byName[ref.Node]
	return n != nil && n.alive && n.ref == ref
}

func (v view) Objects() ObjectTruth {
	for _, e := range v.r.engines {
		if t, ok := e.(ObjectTruth); ok {
			return t
		}
	}
	return noObjects{}
}

func (v view) CP() CPTruth {
	for _, e := range v.r.engines {
		if t, ok := e.(CPTruth); ok {
			return t
		}
	}
	return noCP{}
}

type noObjects struct{}

func (noObjects) Object(string) ([]byte, bool)    { return nil, false }
func (noObjects) EachObject(func(string, []byte)) {}
func (noObjects) Gen() uint64                     { return 0 }

type noCP struct{}

func (noCP) Committed(core.TopicPartition) []core.BatchRange { return nil }
func (noCP) HighWatermark(core.TopicPartition) core.Offset   { return 0 }
func (noCP) Gen() uint64                                     { return 0 }

var (
	_ View        = view{}
	_ NodeAlive   = view{}
	_ ObjectTruth = noObjects{}
	_ CPTruth     = noCP{}
)
