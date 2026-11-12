package spec

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/broker/internal/verify/speccheck/checker"
)

// The spec is the correct subject, so the suite must report no failure.
func TestBrokerSpecVsSpec(t *testing.T) {
	RunSuite(t, NewSpecSubject, checker.Options{
		Seeds: 60,
		Seed:  -1,
		Replay: func(seed uint64) string {
			return fmt.Sprintf("go test ./spec -run '%s' -count=1 (seed %d)", t.Name(), seed)
		},
	})
}
