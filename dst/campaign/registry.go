package campaign

import (
	"fmt"
	"slices"
	"strings"
)

// AllScenarios is the scenario name that means every registered scenario.
const AllScenarios = "all"

// RunOptions tell a scenario how to run one seed.
type RunOptions struct {
	Log         bool // keep every step line in Result.Log
	HaltOnFirst bool // stop at the first failure (replay)
}

// Scenario is one named simulation. Run builds and runs one seed. The campaign
// calls Run from many goroutines at once, so Run must build its own world and
// must not share state with other calls.
type Scenario struct {
	Name   string
	Detail string // one line for the usage text
	Run    func(seed uint64, o RunOptions) Result
}

// Registry holds the scenarios of one product, in the order they were added.
type Registry struct {
	list []Scenario
}

// Add registers s. A scenario without a name or without a Run function, or a
// name that is added twice, is a programming error: Add panics, so the
// program stops at start.
func (r *Registry) Add(s Scenario) {
	if strings.TrimSpace(s.Name) == "" || s.Run == nil {
		panic("campaign: a scenario needs a name and a Run function") //nolint:forbidigo // a bad registration must stop the program at start
	}
	if _, dup := r.Get(s.Name); dup {
		panic("campaign: the scenario " + s.Name + " is added twice") //nolint:forbidigo // a bad registration must stop the program at start
	}
	r.list = append(r.list, s)
}

// Names returns the scenario names in the order they were added.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.list))
	for _, s := range r.list {
		out = append(out, s.Name)
	}
	return out
}

// Get returns the scenario with this name.
func (r *Registry) Get(name string) (Scenario, bool) {
	for _, s := range r.list {
		if s.Name == name {
			return s, true
		}
	}
	return Scenario{}, false
}

// Resolve turns the names of Options.Scenarios into scenario names. An empty
// list, or the name "all", means every registered scenario. A name that is
// not registered is an error.
func (r *Registry) Resolve(names []string) ([]string, error) {
	if len(names) == 0 || slices.Contains(names, AllScenarios) {
		return r.Names(), nil
	}
	for _, n := range names {
		if _, ok := r.Get(n); !ok {
			return nil, fmt.Errorf("unknown scenario %q (have %v)", n, r.Names())
		}
	}
	return names, nil
}
