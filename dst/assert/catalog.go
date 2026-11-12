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
}

// declared is the catalog: every name made by a New* function, with its kind.
// The New* functions are called only from package-level var blocks, so the
// catalog is complete before main or a test starts, and it never changes after
// that.
//
//determinismcheck:ok written only while packages initialize; read-only after that
var declared = map[string]Kind{}

// declare adds name to the catalog. A name declared twice is a programming
// error, so it panics at program start.
func declare(name string, k Kind) {
	if strings.TrimSpace(name) == "" {
		panic("assert: an assertion name must not be empty") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	if old, dup := declared[name]; dup {
		panic("assert: the name " + name + " is declared twice (" + string(old) + " and " + string(k) + ")") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	declared[name] = k
}

// NewAlways declares an Always property. Call it only in a package-level var
// block. A name declared twice panics at program start.
func NewAlways(name string) Always {
	declare(name, KindAlways)
	return Always{name: name}
}

// NewAlwaysOrUnreachable declares an AlwaysOrUnreachable property. Call it
// only in a package-level var block.
func NewAlwaysOrUnreachable(name string) AlwaysOrUnreachable {
	declare(name, KindAlwaysOrUnreachable)
	return AlwaysOrUnreachable{name: name}
}

// NewSometimes declares a Sometimes property. Call it only in a package-level
// var block.
func NewSometimes(name string) Sometimes {
	declare(name, KindSometimes)
	return Sometimes{name: name}
}

// NewReachable declares a Reachable property. Call it only in a package-level
// var block.
func NewReachable(name string) Reachable {
	declare(name, KindReachable)
	return Reachable{name: name}
}

// NewUnreachable declares an Unreachable property. Call it only in a
// package-level var block.
func NewUnreachable(name string) Unreachable {
	declare(name, KindUnreachable)
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
		out = append(out, Declared{Name: n, Kind: declared[n]})
	}
	return out
}
