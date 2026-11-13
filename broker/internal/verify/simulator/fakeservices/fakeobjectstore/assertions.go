package fakeobjectstore

import "github.com/manifesterio/mds/dst/assert"

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/assert. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	objGetCorrupted = assert.NewReachable("obj get corrupted") //determinismcheck:ok catalog handle, set once at start
)
