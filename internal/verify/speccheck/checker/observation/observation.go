package observation

import (
	"reflect"
	"strconv"
	"strings"

	"manifester/internal/core"
)

// ObjectResult is what an object store call answered. Only the fields that
// the call can set are used; the others stay at their zero value.
type ObjectResult struct {
	Code    core.ErrCode
	Outcome core.Outcome
	Data    []byte
	Keys    []string
}

// ControlPlaneResult is what a control-plane call answered. Only the fields
// that the call can set are used; the others stay at their zero value.
type ControlPlaneResult struct {
	Code     core.ErrCode
	Outcome  core.Outcome
	Assigned []core.BatchRange
	Ranges   []core.BatchRange
	End      core.Offset
	Topics   []core.TopicInfo
}

// Normalize returns a copy where an empty slice is nil. A store may return
// either one for "nothing", and that difference is not a bug.
func (r ObjectResult) Normalize() ObjectResult {
	out := r
	out.Data = nil
	if len(r.Data) > 0 {
		out.Data = append([]byte(nil), r.Data...)
	}
	out.Keys = nil
	if len(r.Keys) > 0 {
		out.Keys = append([]string(nil), r.Keys...)
	}
	return out
}

// Normalize returns a copy where an empty slice is nil. A subject may return
// either one for "nothing", and that difference is not a bug.
func (r ControlPlaneResult) Normalize() ControlPlaneResult {
	out := r
	out.Assigned = copyRanges(r.Assigned)
	out.Ranges = copyRanges(r.Ranges)
	out.Topics = nil
	if len(r.Topics) > 0 {
		out.Topics = append([]core.TopicInfo(nil), r.Topics...)
	}
	return out
}

func copyRanges(in []core.BatchRange) []core.BatchRange {
	if len(in) == 0 {
		return nil
	}
	return append([]core.BatchRange(nil), in...)
}

// Equal reports whether both results are the same after Normalize.
func (r ObjectResult) Equal(other ObjectResult) bool {
	return reflect.DeepEqual(r.Normalize(), other.Normalize())
}

// Equal reports whether both results are the same after Normalize.
func (r ControlPlaneResult) Equal(other ControlPlaneResult) bool {
	return reflect.DeepEqual(r.Normalize(), other.Normalize())
}

// Describe returns one line with every field. Nil and empty slices print the
// same way, and every slice shows its length.
func (r ObjectResult) Describe() string {
	return "code " + r.Code.String() +
		", outcome " + outcomeText(r.Outcome) +
		", data " + dataText(r.Data) +
		", keys " + keysText(r.Keys)
}

// Describe returns one line with every field. Nil and empty slices print the
// same way, and every slice shows its length.
func (r ControlPlaneResult) Describe() string {
	return "code " + r.Code.String() +
		", outcome " + outcomeText(r.Outcome) +
		", assigned " + rangesText(r.Assigned) +
		", ranges " + rangesText(r.Ranges) +
		", end " + strconv.FormatInt(int64(r.End), 10) +
		", topics " + topicsText(r.Topics)
}

// Diff returns "" when both results are equal. Otherwise it returns one
// sentence that names the first field that differs, with both values.
func Diff(expected, actual ObjectResult) string {
	e, a := expected.Normalize(), actual.Normalize()
	switch {
	case e.Code != a.Code:
		return diffSentence("code", e.Code.String(), a.Code.String())
	case e.Outcome != a.Outcome:
		return diffSentence("outcome", outcomeText(e.Outcome), outcomeText(a.Outcome))
	case !reflect.DeepEqual(e.Data, a.Data):
		return diffSentence("data", dataText(e.Data), dataText(a.Data))
	case !reflect.DeepEqual(e.Keys, a.Keys):
		return diffSentence("keys", keysText(e.Keys), keysText(a.Keys))
	}
	return ""
}

// DiffControlPlane returns "" when both results are equal. Otherwise it
// returns one sentence that names the first field that differs, with both
// values. It has its own name because Go has no function overloading.
func DiffControlPlane(expected, actual ControlPlaneResult) string {
	e, a := expected.Normalize(), actual.Normalize()
	switch {
	case e.Code != a.Code:
		return diffSentence("code", e.Code.String(), a.Code.String())
	case e.Outcome != a.Outcome:
		return diffSentence("outcome", outcomeText(e.Outcome), outcomeText(a.Outcome))
	case !reflect.DeepEqual(e.Assigned, a.Assigned):
		return diffSentence("assigned", rangesText(e.Assigned), rangesText(a.Assigned))
	case !reflect.DeepEqual(e.Ranges, a.Ranges):
		return diffSentence("ranges", rangesText(e.Ranges), rangesText(a.Ranges))
	case e.End != a.End:
		return diffSentence("end", strconv.FormatInt(int64(e.End), 10), strconv.FormatInt(int64(a.End), 10))
	case !reflect.DeepEqual(e.Topics, a.Topics):
		return diffSentence("topics", topicsText(e.Topics), topicsText(a.Topics))
	}
	return ""
}

func diffSentence(field, expected, actual string) string {
	return "The field " + field + " differs: the expected result has " + expected +
		", but the actual result has " + actual + "."
}

// FromObjectCompletion converts an object store completion into a result. It
// panics on any other completion type, because that is a mistake in the test.
func FromObjectCompletion(c core.IOResult) ObjectResult {
	switch d := c.(type) {
	case core.ObjectPutDone:
		return ObjectResult{Code: d.Err, Outcome: d.Outcome}
	case core.ObjectGetDone:
		return ObjectResult{Code: d.Err, Data: d.Data}
	case core.ObjectDeleteDone:
		return ObjectResult{Code: d.Err, Outcome: d.Outcome}
	case core.ObjectListDone:
		return ObjectResult{Code: d.Err, Keys: d.Keys}
	}
	panic("observation: FromObjectCompletion needs an object store completion, but got " +
		reflect.TypeOf(c).String())
}

// FromControlPlaneDone converts a control-plane completion into a result. A
// nil Resp is allowed, because a failed call has no response. It panics on an
// unknown response type, because that is a mistake in the test.
func FromControlPlaneDone(d core.ControlPlaneDone) ControlPlaneResult {
	out := ControlPlaneResult{Code: d.Err, Outcome: d.Outcome}
	switch r := d.Resp.(type) {
	case nil:
	case core.CommitSegmentResp:
		out.Assigned = r.Assigned
	case core.FindSegmentsResp:
		out.Ranges = r.Ranges
		out.End = r.End
	case core.DescribeTopicsResp:
		out.Topics = r.Topics
	default:
		panic("observation: FromControlPlaneDone does not know the response type " +
			reflect.TypeOf(d.Resp).String())
	}
	return out
}

func outcomeText(o core.Outcome) string {
	switch o {
	case core.NotApplied:
		return "NotApplied"
	case core.MaybeApplied:
		return "MaybeApplied"
	case core.Applied:
		return "Applied"
	}
	return "Outcome(" + strconv.FormatUint(uint64(o), 10) + ")"
}

func dataText(b []byte) string {
	return "(len " + strconv.Itoa(len(b)) + ") " + strconv.Quote(string(b))
}

func keysText(keys []string) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = strconv.Quote(k)
	}
	return "(len " + strconv.Itoa(len(keys)) + ") [" + strings.Join(parts, ", ") + "]"
}

func rangesText(ranges []core.BatchRange) string {
	parts := make([]string, len(ranges))
	for i, r := range ranges {
		parts[i] = "{topic " + strconv.Quote(r.TopicPartition.Topic) +
			" partition " + strconv.FormatInt(int64(r.TopicPartition.Partition), 10) +
			" base " + strconv.FormatInt(int64(r.Base), 10) +
			" count " + strconv.FormatInt(r.Count, 10) +
			" segment " + strconv.Quote(r.Loc.Key) +
			" off " + strconv.FormatInt(r.Loc.Off, 10) +
			" len " + strconv.FormatInt(r.Loc.Len, 10) + "}"
	}
	return "(len " + strconv.Itoa(len(ranges)) + ") [" + strings.Join(parts, ", ") + "]"
}

func topicsText(topics []core.TopicInfo) string {
	parts := make([]string, len(topics))
	for i, t := range topics {
		parts[i] = "{name " + strconv.Quote(t.Name) +
			" partitions " + strconv.FormatInt(int64(t.Partitions), 10) + "}"
	}
	return "(len " + strconv.Itoa(len(topics)) + ") [" + strings.Join(parts, ", ") + "]"
}
