package assert_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/assert"
)

var (
	catAlways      = assert.NewAlways("assert test: always")
	catOrUnreached = assert.NewAlwaysOrUnreachable("assert test: always or unreachable")
	catSometimes   = assert.NewSometimes("assert test: sometimes")
	catReachable   = assert.NewReachable("assert test: reachable")
	catUnreachable = assert.NewUnreachable("assert test: unreachable")
)

func TestCatalogListsEveryDeclaration(t *testing.T) {
	want := map[string]assert.Kind{
		catAlways.Name():      assert.KindAlways,
		catOrUnreached.Name(): assert.KindAlwaysOrUnreachable,
		catSometimes.Name():   assert.KindSometimes,
		catReachable.Name():   assert.KindReachable,
		catUnreachable.Name(): assert.KindUnreachable,
	}
	got := assert.Catalog()
	names := make([]string, 0, len(got))
	for _, d := range got {
		names = append(names, d.Name)
		if k, ok := want[d.Name]; ok && k != d.Kind {
			t.Fatalf("%s: kind %s, want %s", d.Name, d.Kind, k)
		}
	}
	if !slices.IsSorted(names) {
		t.Fatalf("catalog is not sorted: %v", names)
	}
	for n := range want {
		if !slices.Contains(names, n) {
			t.Fatalf("%s is missing from the catalog", n)
		}
	}
}

func TestDeclaringANameTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a second declaration of one name must panic")
		}
	}()
	assert.NewSometimes(catAlways.Name())
}

func TestEmptyNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an empty name must panic")
		}
	}()
	assert.NewReachable(" ")
}
