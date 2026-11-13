package fault_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/fault"
)

var (
	siteA = fault.NewSite("fault test: a")
	siteB = fault.NewSite("fault test: b")
	siteF = fault.NewFaultSite("fault test: fault only")
)

func TestSitesLeavesOutFaultSites(t *testing.T) {
	if slices.Contains(fault.Sites(), siteF.Name()) {
		t.Fatal("a Fault site must not be in Sites")
	}
}

func TestFaultSiteNameIsUnique(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a second declaration of a Fault site name must panic")
		}
	}()
	fault.NewSite(siteF.Name())
}

func TestSitesListsEveryDeclaration(t *testing.T) {
	got := fault.Sites()
	if !slices.IsSorted(got) {
		t.Fatalf("not sorted: %v", got)
	}
	if !slices.Contains(got, siteA.Name()) || !slices.Contains(got, siteB.Name()) {
		t.Fatalf("missing sites: %v", got)
	}
}

func TestDeclaringASiteTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a second declaration of one site must panic")
		}
	}()
	fault.NewSite(siteA.Name())
}

func TestEmptySiteNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an empty site name must panic")
		}
	}()
	fault.NewSite("")
}
