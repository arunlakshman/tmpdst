package injectedbug

import (
	"slices"
	"strings"

	"manifester/internal/core"
	"manifester/internal/verify/speccheck/checker"
	"manifester/internal/verify/speccheck/checker/observation"
	"manifester/internal/verify/speccheck/checker/suites"
)

// ObjectBug is a planted bug for an object store. Wrap returns a subject that
// behaves like the correct one, except for the bug.
type ObjectBug struct {
	name, doc string
	wrap      func(correct suites.ObjectSubject) suites.ObjectSubject
}

// NewObjectBug returns a named bug. Wrap is called once for each fresh subject,
// so the wrapper may keep state.
func NewObjectBug(name, doc string, wrap func(suites.ObjectSubject) suites.ObjectSubject) ObjectBug {
	return ObjectBug{name: name, doc: doc, wrap: wrap}
}

// Name is the short name of the bug.
func (b ObjectBug) Name() string { return b.name }

// Doc is one sentence that says what the bug does wrong.
func (b ObjectBug) Doc() string { return b.doc }

// Wrap returns correct with the bug added.
func (b ObjectBug) Wrap(correct suites.ObjectSubject) suites.ObjectSubject { return b.wrap(correct) }

// objectSubject is a subject made from a function. do may call inner.
type objectSubject struct {
	inner suites.ObjectSubject
	do    func(op core.IOCall) (observation.ObjectResult, error)
	hash  func() (uint64, bool)
}

func (s *objectSubject) Do(op core.IOCall) (observation.ObjectResult, error) { return s.do(op) }

func (s *objectSubject) StateHash() (uint64, bool) {
	if s.hash != nil {
		return s.hash()
	}
	return s.inner.StateHash()
}

// afterObject makes a bug that changes the result after the correct subject has
// answered. change gets the operation and the correct result.
func afterObject(name, doc string, change func(op core.IOCall, r observation.ObjectResult) observation.ObjectResult) ObjectBug {
	return stateful(name, doc, func(suites.ObjectSubject) func(core.IOCall, observation.ObjectResult) observation.ObjectResult {
		return change
	})
}

// stateful makes a bug whose change function keeps state. newChange is called
// once for each fresh subject.
func stateful(name, doc string, newChange func(suites.ObjectSubject) func(core.IOCall, observation.ObjectResult) observation.ObjectResult) ObjectBug {
	return NewObjectBug(name, doc, func(inner suites.ObjectSubject) suites.ObjectSubject {
		change := newChange(inner)
		return &objectSubject{inner: inner, do: func(op core.IOCall) (observation.ObjectResult, error) {
			r, err := inner.Do(op)
			if err != nil {
				return r, err
			}
			return change(op, r), nil
		}}
	})
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
		func(suites.ObjectSubject) func(core.IOCall, observation.ObjectResult) observation.ObjectResult {
			kept := map[string][]byte{}
			return func(op core.IOCall, r observation.ObjectResult) observation.ObjectResult {
				switch op := op.(type) {
				case core.ObjectPut:
					if r.Code == core.OK {
						kept[op.Key] = op.Data
					}
				case core.ObjectDelete:
					if r.Code == core.OK {
						delete(kept, op.Key)
					}
				case core.ObjectGet:
					data, ok := kept[op.Ref.Key]
					if !ok || r.Code != core.OK {
						return r
					}
					end := int64(len(data))
					if op.Ref.Len > 0 {
						end = op.Ref.Off + op.Ref.Len
					}
					if op.Ref.Off >= 0 && op.Ref.Off <= end && end <= int64(len(data)) {
						r.Data = slices.Clone(data[op.Ref.Off:end])
					}
				}
				return r
			}
		})
}

// GetRangeEndOffByOne reads one byte too many when Len is set.
func GetRangeEndOffByOne() ObjectBug {
	return NewObjectBug("GetRangeEndOffByOne",
		"Get with a length reads one byte past the end of the range.",
		func(inner suites.ObjectSubject) suites.ObjectSubject {
			return &objectSubject{inner: inner, do: func(op core.IOCall) (observation.ObjectResult, error) {
				if get, ok := op.(core.ObjectGet); ok && get.Ref.Len > 0 {
					get.Ref.Len++
					op = get
				}
				return inner.Do(op)
			}}
		})
}

// GetOutOfRangeReturnsOK answers a range outside the object with OK and no data.
func GetOutOfRangeReturnsOK() ObjectBug {
	return afterObject("GetOutOfRangeReturnsOK",
		"Get with a range outside the object returns OK and no data instead of ErrInvalid.",
		func(op core.IOCall, r observation.ObjectResult) observation.ObjectResult {
			if _, ok := op.(core.ObjectGet); ok && r.Code == core.ErrInvalid {
				return observation.ObjectResult{}
			}
			return r
		})
}

// ListUnsorted returns the keys in reverse order.
func ListUnsorted() ObjectBug {
	return afterObject("ListUnsorted",
		"List returns the keys in reverse order instead of sorted order.",
		func(op core.IOCall, r observation.ObjectResult) observation.ObjectResult {
			if _, ok := op.(core.ObjectList); ok {
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
		func(suites.ObjectSubject) func(core.IOCall, observation.ObjectResult) observation.ObjectResult {
			keys := map[string]bool{}
			return func(op core.IOCall, r observation.ObjectResult) observation.ObjectResult {
				switch op := op.(type) {
				case core.ObjectPut:
					if r.Code == core.OK {
						keys[op.Key] = true
					}
				case core.ObjectDelete:
					if r.Code == core.OK {
						delete(keys, op.Key)
					}
				case core.ObjectList:
					var found []string
					for k := range keys {
						if strings.Contains(k, op.Prefix) {
							found = append(found, k)
						}
					}
					slices.Sort(found)
					r.Keys = found
				}
				return r
			}
		})
}

// DeleteMissingReturnsOK hides ErrNotFound for a missing key.
func DeleteMissingReturnsOK() ObjectBug {
	return afterObject("DeleteMissingReturnsOK",
		"Delete of a missing key returns OK instead of ErrNotFound.",
		func(op core.IOCall, r observation.ObjectResult) observation.ObjectResult {
			if _, ok := op.(core.ObjectDelete); ok && r.Code == core.ErrNotFound {
				return observation.ObjectResult{Outcome: core.Applied}
			}
			return r
		})
}

// PutEmptyKeyAccepted accepts the empty key.
func PutEmptyKeyAccepted() ObjectBug {
	return afterObject("PutEmptyKeyAccepted",
		"Put with the empty key returns OK instead of ErrInvalid.",
		func(op core.IOCall, r observation.ObjectResult) observation.ObjectResult {
			if put, ok := op.(core.ObjectPut); ok && put.Key == "" && r.Code == core.ErrInvalid {
				return observation.ObjectResult{Outcome: core.Applied}
			}
			return r
		})
}

// PutOverwriteIgnored keeps the first value of a key and answers OK to later Puts.
func PutOverwriteIgnored() ObjectBug {
	return NewObjectBug("PutOverwriteIgnored",
		"Put on an existing key answers OK but keeps the old value.",
		func(inner suites.ObjectSubject) suites.ObjectSubject {
			exists := map[string]bool{}
			return &objectSubject{inner: inner, do: func(op core.IOCall) (observation.ObjectResult, error) {
				switch op := op.(type) {
				case core.ObjectPut:
					if exists[op.Key] {
						return observation.ObjectResult{Outcome: core.Applied}, nil
					}
				case core.ObjectDelete:
					delete(exists, op.Key)
				}
				r, err := inner.Do(op)
				if put, ok := op.(core.ObjectPut); ok && err == nil && r.Code == core.OK {
					exists[put.Key] = true
				}
				return r, err
			}}
		})
}

// ObjectStateHashDiffers reports a wrong state hash. It checks that the suite
// compares the hash at all.
func ObjectStateHashDiffers() ObjectBug {
	return newHashBug(NewObjectBug, withObjectStateHash)
}

func withObjectStateHash(inner suites.ObjectSubject, hash func() (uint64, bool)) suites.ObjectSubject {
	return &objectSubject{inner: inner, do: inner.Do, hash: hash}
}

// FindObjectDetection runs the object store suite on a subject with bug, for every
// profile and the seeds 1 to seeds. It returns the first catch. newCorrect must
// return a fresh correct subject on each call.
func FindObjectDetection(bug ObjectBug, newCorrect func() suites.ObjectSubject, seeds int) (Detection, bool) {
	return findSubjectDetection(seeds, newCorrect, bug.Wrap, suites.ObjectStoreComparator)
}

// RequireAllObjectBugsDetected runs every planted object store bug through the
// object store suite. It reports an error for each bug that no profile finds
// within seeds seeds, and it logs the catch table. A subject with no spec hash
// (for example real S3) cannot show a wrong hash, so that one bug is skipped
// for such a subject and the log says so.
func RequireAllObjectBugsDetected(t checker.Reporter, newCorrect func() suites.ObjectSubject, seeds int) {
	requireAllDetected(t, "object store", newCorrect, seeds, ObjectBugs(), FindObjectDetection)
}
