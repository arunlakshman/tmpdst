package fault

import (
	"slices"
	"strings"
)

// Site is a declared buggify site: a named place in the code where a
// fault may be injected on purpose. Make it only with NewSite, in a
// package-level var block. The declaration puts the name in the catalog (see
// Sites), so a campaign can list the sites that never fired.
type Site struct{ name string }

// Name returns the name given to NewSite.
func (s Site) Name() string { return s.name }

// sites is the catalog of every name made by NewSite or NewFaultSite. The value
// is true for a site that the injector counts (NewSite). NewSite is called only
// from package-level var blocks, so the catalog is complete before main or a
// test starts, and it never changes after that.
//
//determinismcheck:ok written only while packages initialize; read-only after that
var sites = map[string]bool{}

// NewSite declares a buggify site. Call it only in a package-level var block.
// A name declared twice, or an empty name, panics at program start.
func NewSite(name string) Site {
	return declareSite(name, true)
}

// NewFaultSite declares a site that the code asks with Injector.Fault. The
// injector does not count a Fault site (see Injector.Sites), so it can never
// show as fired. Sites leaves it out, and a campaign does not list it as
// "never fired". Call it only in a package-level var block.
func NewFaultSite(name string) Site {
	return declareSite(name, false)
}

func declareSite(name string, counted bool) Site {
	if strings.TrimSpace(name) == "" {
		panic("fault: a site name must not be empty") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	if _, dup := sites[name]; dup {
		panic("fault: the site " + name + " is declared twice") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	sites[name] = counted
	return Site{name: name}
}

// Sites returns the name of every declared site that the injector counts,
// sorted. A site made by NewFaultSite is not in the list.
func Sites() []string {
	out := make([]string, 0, len(sites))
	//determinismcheck:ok the names are sorted below
	for n, counted := range sites {
		if counted {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}
