package injectedbug

import (
	"slices"
	"strings"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/conformance"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ObjectBug is a planted bug for an object store.
type ObjectBug = Bug[objectstore.Call, conformance.ObjectResult]

// NewObjectBug returns a named bug. Wrap is called once for each fresh subject,
// so the wrapper may keep state.
func NewObjectBug(name, doc string, wrap func(conformance.ObjectSubject) conformance.ObjectSubject) ObjectBug {
	return NewBug(name, doc, wrap)
}

// afterObject makes a bug that changes the result after the correct subject has
// answered. change gets the operation and the correct result.
func afterObject(name, doc string, change func(operation objectstore.Call, r conformance.ObjectResult) conformance.ObjectResult) ObjectBug {
	return after(name, doc, change)
}

// ObjectBugs returns every planted object store bug, in a fixed order.
func ObjectBugs() []ObjectBug {
	return []ObjectBug{
		PutKeepsCallerSlice(),
		GetRangeEndOffByOne(),
		GetOutOfRangeReturnsOK(),
		ListUnsorted(),
		ListPrefixAsSubstring(),
		DeleteMissingReturnsOK(),
		PutEmptyKeyAccepted(),
		PutOverwriteIgnored(),
		ObjectStateHashDiffers(),
	}
}

// PutKeepsCallerSlice keeps the slice that the caller passed to Put, so a later
// change of that slice changes the stored data. The suite writes over its copy
// of the data after each Put, which finds this bug on the next Get.
func PutKeepsCallerSlice() ObjectBug {
	return stateful("PutKeepsCallerSlice",
		"Put keeps the caller's slice instead of a copy, so a later change by the caller changes the stored data.",
		func(conformance.ObjectSubject) func(objectstore.Call, conformance.ObjectResult) conformance.ObjectResult {
			kept := map[string][]byte{}
			return func(operation objectstore.Call, r conformance.ObjectResult) conformance.ObjectResult {
				switch operation := operation.(type) {
				case objectstore.Put:
					if r.Code == core.OK {
						kept[operation.Key] = operation.Data
					}
				case objectstore.Delete:
					if r.Code == core.OK {
						delete(kept, operation.Key)
					}
				case objectstore.Get:
					data, ok := kept[operation.Ref.Key]
					if !ok || r.Code != core.OK {
						return r
					}
					end := int64(len(data))
					if operation.Ref.Len > 0 {
						end = operation.Ref.Off + operation.Ref.Len
					}
					if operation.Ref.Off >= 0 && operation.Ref.Off <= end && end <= int64(len(data)) {
						r.Data = slices.Clone(data[operation.Ref.Off:end])
					}
				case objectstore.List:
				}
				return r
			}
		})
}

// GetRangeEndOffByOne reads one byte too many when Len is set.
func GetRangeEndOffByOne() ObjectBug {
	return NewObjectBug("GetRangeEndOffByOne",
		"Get with a length reads one byte past the end of the range.",
		func(inner conformance.ObjectSubject) conformance.ObjectSubject {
			return wrap[objectstore.Call, conformance.ObjectResult](inner, func(operation objectstore.Call) (conformance.ObjectResult, error) {
				if get, ok := operation.(objectstore.Get); ok && get.Ref.Len > 0 {
					get.Ref.Len++
					operation = get
				}
				return inner.Apply(operation)
			}, nil)
		})
}

// GetOutOfRangeReturnsOK answers a range outside the object with OK and no data.
func GetOutOfRangeReturnsOK() ObjectBug {
	return afterObject("GetOutOfRangeReturnsOK",
		"Get with a range outside the object returns OK and no data instead of ErrInvalid.",
		func(operation objectstore.Call, r conformance.ObjectResult) conformance.ObjectResult {
			if _, ok := operation.(objectstore.Get); ok && r.Code == core.ErrInvalid {
				return conformance.ObjectResult{}
			}
			return r
		})
}

// ListUnsorted returns the keys in reverse order.
func ListUnsorted() ObjectBug {
	return afterObject("ListUnsorted",
		"List returns the keys in reverse order instead of sorted order.",
		func(operation objectstore.Call, r conformance.ObjectResult) conformance.ObjectResult {
			if _, ok := operation.(objectstore.List); ok {
				r.Keys = slices.Clone(r.Keys)
				slices.Reverse(r.Keys)
			}
			return r
		})
}

// ListPrefixAsSubstring lists a key when the prefix appears anywhere in it.
func ListPrefixAsSubstring() ObjectBug {
	return stateful("ListPrefixAsSubstring",
		"List matches the prefix anywhere in the key instead of only at the start.",
		func(conformance.ObjectSubject) func(objectstore.Call, conformance.ObjectResult) conformance.ObjectResult {
			keys := map[string]bool{}
			return func(operation objectstore.Call, r conformance.ObjectResult) conformance.ObjectResult {
				switch operation := operation.(type) {
				case objectstore.Put:
					if r.Code == core.OK {
						keys[operation.Key] = true
					}
				case objectstore.Delete:
					if r.Code == core.OK {
						delete(keys, operation.Key)
					}
				case objectstore.List:
					var found []string
					for k := range keys {
						if strings.Contains(k, operation.Prefix) {
							found = append(found, k)
						}
					}
					slices.Sort(found)
					r.Keys = found
				case objectstore.Get:
				}
				return r
			}
		})
}

// DeleteMissingReturnsOK hides ErrNotFound for a missing key.
func DeleteMissingReturnsOK() ObjectBug {
	return afterObject("DeleteMissingReturnsOK",
		"Delete of a missing key returns OK instead of ErrNotFound.",
		func(operation objectstore.Call, r conformance.ObjectResult) conformance.ObjectResult {
			if _, ok := operation.(objectstore.Delete); ok && r.Code == core.ErrNotFound {
				return conformance.ObjectResult{Outcome: core.Applied}
			}
			return r
		})
}

// PutEmptyKeyAccepted accepts the empty key.
func PutEmptyKeyAccepted() ObjectBug {
	return afterObject("PutEmptyKeyAccepted",
		"Put with the empty key returns OK instead of ErrInvalid.",
		func(operation objectstore.Call, r conformance.ObjectResult) conformance.ObjectResult {
			if put, ok := operation.(objectstore.Put); ok && put.Key == "" && r.Code == core.ErrInvalid {
				return conformance.ObjectResult{Outcome: core.Applied}
			}
			return r
		})
}

// PutOverwriteIgnored keeps the first value of a key and answers OK to later Puts.
func PutOverwriteIgnored() ObjectBug {
	return NewObjectBug("PutOverwriteIgnored",
		"Put on an existing key answers OK but keeps the old value.",
		func(inner conformance.ObjectSubject) conformance.ObjectSubject {
			exists := map[string]bool{}
			return wrap[objectstore.Call, conformance.ObjectResult](inner, func(operation objectstore.Call) (conformance.ObjectResult, error) {
				switch operation := operation.(type) {
				case objectstore.Put:
					if exists[operation.Key] {
						return conformance.ObjectResult{Outcome: core.Applied}, nil
					}
				case objectstore.Delete:
					delete(exists, operation.Key)
				case objectstore.Get, objectstore.List:
				}
				r, err := inner.Apply(operation)
				if put, ok := operation.(objectstore.Put); ok && err == nil && r.Code == core.OK {
					exists[put.Key] = true
				}
				return r, err
			}, nil)
		})
}

// ObjectStateHashDiffers reports a wrong state hash. It checks that the suite
// compares the hash at all. It has no effect on a subject with no hash.
func ObjectStateHashDiffers() ObjectBug {
	return stateHashDiffers[objectstore.Call, conformance.ObjectResult]()
}

// objectRunSeed runs the object store suite on one profile with one seed.
func objectRunSeed(p conformance.InputProfile, newSubject func() conformance.ObjectSubject, seed uint64) (string, int, bool) {
	return failureOf(conformance.ObjectStoreLockstep(p, newSubject).RunSeed(seed))
}

// ObjectBugsCaught runs every planted object store bug through the object
// store suite, on every profile, for the seeds 1 to budget. It fails t for each
// bug that no seed catches. newCorrect must return a fresh correct subject on
// each call. A subject with no spec hash (for example real S3) cannot show a
// wrong hash, so that one bug is skipped for such a subject and the log says
// so.
func ObjectBugsCaught(t testing.TB, newCorrect func() conformance.ObjectSubject, budget int) {
	t.Helper()
	bugsCaught(t, "object", ObjectBugs(), newCorrect, budget, objectRunSeed)
}

// ObjectSuiteIsClean runs the object store suite on newCorrect, with no bug,
// on every profile, for the seeds 1 to budget. It fails t on the first
// failure: a failure here is a false alarm.
func ObjectSuiteIsClean(t testing.TB, newCorrect func() conformance.ObjectSubject, budget int) {
	t.Helper()
	suiteIsClean(t, newCorrect, budget, objectRunSeed)
}
