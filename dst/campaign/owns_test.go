package campaign

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/assert"
)

// TestOwnsLimitsTheExpectedNamesToTheScenariosThatRun: two scenarios share one
// catalog. A campaign of one scenario must not name the paths and the Always
// properties of the other as missing.
func TestOwnsLimitsTheExpectedNamesToTheScenariosThatRun(t *testing.T) {
	cat := catalog{asserts: []assert.Declared{
		{Name: "shared: path", Kind: assert.KindReachable},
		{Name: "a: path", Kind: assert.KindReachable},
		{Name: "a: maybe", Kind: assert.KindSometimes},
		{Name: "a: rule", Kind: assert.KindAlways},
		{Name: "b: path", Kind: assert.KindReachable},
		{Name: "b: rule", Kind: assert.KindAlways},
	}}
	owns := func(prefix string) func(string) bool {
		return func(name string) bool { return strings.HasPrefix(name, "shared: ") || strings.HasPrefix(name, prefix) }
	}
	a, b := pass("a"), pass("b")
	a.Owns, b.Owns = owns("a: "), owns("b: ")
	reg := registry(t, a, b)
	for _, tc := range []struct {
		scenarios                []string
		wantReached, wantChecked []string
		wantPaths                int
	}{
		{[]string{"a"}, []string{"a: maybe", "a: path", "shared: path"}, []string{"a: rule"}, 3},
		{[]string{"b"}, []string{"b: path", "shared: path"}, []string{"b: rule"}, 2},
		{[]string{"a", "b"}, []string{"a: maybe", "a: path", "b: path", "shared: path"}, []string{"a: rule", "b: rule"}, 4},
		{[]string{"all"}, []string{"a: maybe", "a: path", "b: path", "shared: path"}, []string{"a: rule", "b: rule"}, 4},
	} {
		rep, err := runCampaign(reg, Options{Scenarios: tc.scenarios, First: 1, Seeds: 1, Workers: 1}, cat, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(rep.NeverReached, tc.wantReached) || !slices.Equal(rep.NeverChecked, tc.wantChecked) || rep.expectedPaths != tc.wantPaths {
			t.Errorf("scenarios %v: never reached %v, never checked %v, expected paths %d",
				tc.scenarios, rep.NeverReached, rep.NeverChecked, rep.expectedPaths)
		}
	}
}

// TestScenarioWithoutOwnsOwnsEveryName: one scenario without an Owns function
// makes the whole catalog count.
func TestScenarioWithoutOwnsOwnsEveryName(t *testing.T) {
	cat := catalog{asserts: []assert.Declared{
		{Name: "a: path", Kind: assert.KindReachable},
		{Name: "b: path", Kind: assert.KindReachable},
	}}
	a := pass("a")
	a.Owns = func(name string) bool { return strings.HasPrefix(name, "a: ") }
	reg := registry(t, a, pass("b"))
	for scenarios, want := range map[string][]string{"a": {"a: path"}, "b": {"a: path", "b: path"}, "a,b": {"a: path", "b: path"}} {
		rep, err := runCampaign(reg, Options{Scenarios: strings.Split(scenarios, ","), First: 1, Seeds: 1, Workers: 1}, cat, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(rep.NeverReached, want) {
			t.Errorf("scenarios %q: never reached %v, want %v", scenarios, rep.NeverReached, want)
		}
	}
}
