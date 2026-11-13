package assert

import (
	"slices"
	"strings"
)

// Typed handles. Each kind of assertion has its own type, so a Sometimes
// property cannot be used where an Always is needed. Make them only with the
// New* functions, in package-level var blocks.

// Always names a property that must hold every time it is checked.
type Always struct{ name string }

// AlwaysOrUnreachable names a property that must hold every time it is
// checked; it is fine if the code never checks it.
type AlwaysOrUnreachable struct{ name string }

// Sometimes names a property that must hold at least once in a campaign.
type Sometimes struct{ name string }

// Reachable names a place in the code that a campaign must reach.
type Reachable struct{ name string }

// Unreachable names a place in the code that must never be reached.
type Unreachable struct{ name string }

// Name returns the name given to NewAlways.
func (p Always) Name() string { return p.name }

// Name returns the name given to NewAlwaysOrUnreachable.
func (p AlwaysOrUnreachable) Name() string { return p.name }

// Name returns the name given to NewSometimes.
func (p Sometimes) Name() string { return p.name }

// Name returns the name given to NewReachable.
func (p Reachable) Name() string { return p.name }

// Name returns the name given to NewUnreachable.
func (p Unreachable) Name() string { return p.name }

// Declared is one entry of the catalog.
type Declared struct {
	Name string
	Kind Kind
	// FailureOnly is set for an Always that the code records only when it
	// fails (a violation check). A campaign never checks it on a good run, so
	// the report does not list it as "never checked".
	FailureOnly bool
	// NotExpected is set for a Reachable that a campaign may miss. The report
	// does not list it as "never reached" and does not count it as expected.
	NotExpected bool
}

// declared is the catalog: every name made by a New* function, with its kind.
// The New* functions are called only from package-level var blocks, so the
// catalog is complete before main or a test starts, and it never changes after
// that.
//
//determinismcheck:ok written only while packages initialize; read-only after that
var declared = map[string]Declared{}

// declare adds name to the catalog. A name declared twice is a programming
// error, so it panics at program start.
func declare(d Declared) {
	name, k := d.Name, d.Kind
	if strings.TrimSpace(name) == "" {
		panic("assert: an assertion name must not be empty") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	if old, dup := declared[name]; dup {
		panic("assert: the name " + name + " is declared twice (" + string(old.Kind) + " and " + string(k) + ")") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	declared[name] = d
}

// NewAlways declares an Always property. Call it only in a package-level var
// block. A name declared twice panics at program start.
func NewAlways(name string) Always {
	declare(Declared{Name: name, Kind: KindAlways})
	return Always{name: name}
}

// NewAlwaysOnFailure declares an Always property that the code records only
// when it fails: a violation check, such as "run ends". It behaves as an
// Always, but a campaign does not list it as "never checked". Call it only in
// a package-level var block.
func NewAlwaysOnFailure(name string) Always {
	declare(Declared{Name: name, Kind: KindAlways, FailureOnly: true})
	return Always{name: name}
}

// NewAlwaysOrUnreachable declares an AlwaysOrUnreachable property. Call it
// only in a package-level var block.
func NewAlwaysOrUnreachable(name string) AlwaysOrUnreachable {
	declare(Declared{Name: name, Kind: KindAlwaysOrUnreachable})
	return AlwaysOrUnreachable{name: name}
}

// NewSometimes declares a Sometimes property. Call it only in a package-level
// var block.
func NewSometimes(name string) Sometimes {
	declare(Declared{Name: name, Kind: KindSometimes})
	return Sometimes{name: name}
}

// NewReachable declares a Reachable property. Call it only in a package-level
// var block.
func NewReachable(name string) Reachable {
	declare(Declared{Name: name, Kind: KindReachable})
	return Reachable{name: name}
}

// NewReachableNotExpected declares a Reachable property that a campaign may
// miss (a rare path that no scenario aims at). It behaves as a Reachable, but
// a campaign does not expect it: it is not listed as "never reached". Call it
// only in a package-level var block.
func NewReachableNotExpected(name string) Reachable {
	declare(Declared{Name: name, Kind: KindReachable, NotExpected: true})
	return Reachable{name: name}
}

// NewUnreachable declares an Unreachable property. Call it only in a
// package-level var block.
func NewUnreachable(name string) Unreachable {
	declare(Declared{Name: name, Kind: KindUnreachable})
	return Unreachable{name: name}
}

// Catalog returns every declared assertion, sorted by name. A campaign uses it
// to list properties that were never reached or never checked.
func Catalog() []Declared {
	names := make([]string, 0, len(declared))
	//determinismcheck:ok the names are sorted below
	for n := range declared {
		names = append(names, n)
	}
	slices.Sort(names)
	out := make([]Declared, 0, len(names))
	for _, n := range names {
		out = append(out, declared[n])
	}
	return out
}
