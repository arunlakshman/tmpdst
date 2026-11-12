package fault

import (
	"slices"
	"strings"
)

// NamedSite is a declared buggify site: a named place in the code where a
// fault may be injected on purpose. Make it only with NewSite, in a
// package-level var block. The declaration puts the name in the catalog (see
// Sites), so a campaign can list the sites that never fired.
//
// The old string type Site stays until the products move to NamedSite (plan
// task T1.16 deletes it; NamedSite may then take the name Site).
type NamedSite struct{ name string }

// Name returns the name given to NewSite.
func (s NamedSite) Name() string { return s.name }

// sites is the catalog of every name made by NewSite. NewSite is called only
// from package-level var blocks, so the catalog is complete before main or a
// test starts, and it never changes after that.
//
//determinismcheck:ok written only while packages initialize; read-only after that
var sites = map[string]bool{}

// NewSite declares a buggify site. Call it only in a package-level var block.
// A name declared twice, or an empty name, panics at program start.
func NewSite(name string) NamedSite {
	if strings.TrimSpace(name) == "" {
		panic("fault: a site name must not be empty") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	if sites[name] {
		panic("fault: the site " + name + " is declared twice") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	sites[name] = true
	return NamedSite{name: name}
}

// Sites returns the name of every declared site, sorted.
func Sites() []string {
	out := make([]string, 0, len(sites))
	//determinismcheck:ok the names are sorted below
	for n := range sites {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}
