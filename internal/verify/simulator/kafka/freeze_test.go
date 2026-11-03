package kafka

import (
	"testing"

	"github.com/manifesterio/mds/internal/verify/simulator"
)

// With real process freezes the log protocol keeps every property, and the
// campaign must reach the path where a frozen writer's put is refused at the
// floor of the log. (The campaign fails on a coverage gap, so a change that
// stops the freeze from reaching the floor is caught.)
func TestLogFreezeCampaign(t *testing.T) {
	n := 40
	if testing.Short() {
		n = 8
	}
	rep, err := simulator.NewRunner().Run(ConfigLogFreeze(seeds(1, n)))
	if err != nil {
		t.Fatal(err)
	}
	if testing.Short() { // coverage needs a full campaign
		rep.Coverage = nil
	}
	report(t, rep)
}
