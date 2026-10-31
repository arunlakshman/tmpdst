package observation

import (
	"strings"
	"testing"

	"mds/internal/core"
)

var allCodes = []core.ErrCode{
	core.OK, core.ErrTimeout, core.ErrUnavailable, core.ErrNotFound, core.ErrConflict,
	core.ErrConnClosed, core.ErrTooLarge, core.ErrCorrupt, core.ErrInvalid, core.ErrIO,
}

var allOutcomes = []core.Outcome{core.NotApplied, core.MaybeApplied, core.Applied}

func TestNilAndEmptySlicesAreEqual(t *testing.T) {
	br := []core.BatchRange{{Base: 1, Count: 2}}
	tests := []struct {
		name  string
		left  ControlPlaneResult
		right ControlPlaneResult
		equal bool
	}{
		{"nil and empty assigned", ControlPlaneResult{}, ControlPlaneResult{Assigned: []core.BatchRange{}}, true},
		{"nil and empty ranges", ControlPlaneResult{}, ControlPlaneResult{Ranges: []core.BatchRange{}}, true},
		{"nil and empty topics", ControlPlaneResult{}, ControlPlaneResult{Topics: []core.TopicInfo{}}, true},
		{"same ranges", ControlPlaneResult{Ranges: br}, ControlPlaneResult{Ranges: []core.BatchRange{{Base: 1, Count: 2}}}, true},
		{"different count", ControlPlaneResult{Ranges: br}, ControlPlaneResult{Ranges: []core.BatchRange{{Base: 1, Count: 3}}}, false},
		{"different end", ControlPlaneResult{End: 1}, ControlPlaneResult{End: 2}, false},
	}
	for _, tc := range tests {
		if got := tc.left.Equal(tc.right); got != tc.equal {
			t.Errorf("%s: Equal returned %v, but %v was expected.", tc.name, got, tc.equal)
		}
	}
	objects := []struct {
		name  string
		left  ObjectResult
		right ObjectResult
		equal bool
	}{
		{"nil and empty data", ObjectResult{}, ObjectResult{Data: []byte{}}, true},
		{"nil and empty keys", ObjectResult{}, ObjectResult{Keys: []string{}}, true},
		{"different data", ObjectResult{Data: []byte("a")}, ObjectResult{Data: []byte("b")}, false},
		{"different keys", ObjectResult{Keys: []string{"a"}}, ObjectResult{Keys: []string{"a", "b"}}, false},
	}
	for _, tc := range objects {
		if got := tc.left.Equal(tc.right); got != tc.equal {
			t.Errorf("%s: Equal returned %v, but %v was expected.", tc.name, got, tc.equal)
		}
	}
}

func TestNormalizeUsesNilForEmpty(t *testing.T) {
	o := ObjectResult{Data: []byte{}, Keys: []string{}}.Normalize()
	if o.Data != nil || o.Keys != nil {
		t.Errorf("Normalize left an empty slice in an object result.")
	}
	c := ControlPlaneResult{Assigned: []core.BatchRange{}, Ranges: []core.BatchRange{}, Topics: []core.TopicInfo{}}.Normalize()
	if c.Assigned != nil || c.Ranges != nil || c.Topics != nil {
		t.Errorf("Normalize left an empty slice in a control-plane result.")
	}
}

func TestEveryCodeAndOutcomeIsCompared(t *testing.T) {
	for _, code := range allCodes {
		for _, outcome := range allOutcomes {
			base := ObjectResult{Code: code, Outcome: outcome}
			same := base
			if !base.Equal(same) {
				t.Errorf("Code %s with outcome %d is not equal to itself.", code, outcome)
			}
			for _, other := range allCodes {
				if other != code && base.Equal(ObjectResult{Code: other, Outcome: outcome}) {
					t.Errorf("Codes %s and %s compare as equal.", code, other)
				}
			}
			for _, other := range allOutcomes {
				if other != outcome && (ControlPlaneResult{Code: code, Outcome: outcome}).Equal(ControlPlaneResult{Code: code, Outcome: other}) {
					t.Errorf("Outcomes %d and %d compare as equal.", outcome, other)
				}
			}
		}
	}
}

func TestObjectConverters(t *testing.T) {
	tests := []struct {
		name string
		in   core.IOResult
		want ObjectResult
	}{
		{"put", core.ObjectPutDone{Err: core.ErrTimeout, Outcome: core.MaybeApplied}, ObjectResult{Code: core.ErrTimeout, Outcome: core.MaybeApplied}},
		{"get", core.ObjectGetDone{Data: []byte("x")}, ObjectResult{Data: []byte("x")}},
		{"get not found", core.ObjectGetDone{Err: core.ErrNotFound}, ObjectResult{Code: core.ErrNotFound}},
		{"delete", core.ObjectDeleteDone{Outcome: core.Applied}, ObjectResult{Outcome: core.Applied}},
		{"list", core.ObjectListDone{Keys: []string{"a", "b"}}, ObjectResult{Keys: []string{"a", "b"}}},
	}
	for _, tc := range tests {
		got := FromObjectCompletion(tc.in)
		if msg := Diff(tc.want, got); msg != "" {
			t.Errorf("%s: %s", tc.name, msg)
		}
	}
}

func TestObjectConverterPanicsOnOtherType(t *testing.T) {
	defer func() {
		r := recover()
		msg, _ := r.(string)
		if !strings.Contains(msg, "needs an object store completion") {
			t.Errorf("The panic message was %v, which does not explain the mistake.", r)
		}
	}()
	FromObjectCompletion(core.SendDone{})
}

func TestControlPlaneConverter(t *testing.T) {
	br := []core.BatchRange{{TopicPartition: core.TopicPartition{Topic: "t", Partition: 1}, Base: 5, Count: 2}}
	tests := []struct {
		name string
		in   core.ControlPlaneDone
		want ControlPlaneResult
	}{
		{"nil response", core.ControlPlaneDone{Err: core.ErrUnavailable}, ControlPlaneResult{Code: core.ErrUnavailable}},
		{"commit", core.ControlPlaneDone{Resp: core.CommitSegmentResp{Assigned: br}, Outcome: core.Applied}, ControlPlaneResult{Outcome: core.Applied, Assigned: br}},
		{"lookup", core.ControlPlaneDone{Resp: core.FindSegmentsResp{Ranges: br, End: 7}}, ControlPlaneResult{Ranges: br, End: 7}},
		{"describe", core.ControlPlaneDone{Resp: core.DescribeTopicsResp{Topics: []core.TopicInfo{{Name: "t", Partitions: 3}}}}, ControlPlaneResult{Topics: []core.TopicInfo{{Name: "t", Partitions: 3}}}},
	}
	for _, tc := range tests {
		got := FromControlPlaneDone(tc.in)
		if msg := DiffControlPlane(tc.want, got); msg != "" {
			t.Errorf("%s: %s", tc.name, msg)
		}
	}
}

func TestControlPlaneConverterPanicsOnUnknownResponse(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("FromControlPlaneDone did not panic on an unknown response type.")
		}
	}()
	FromControlPlaneDone(core.ControlPlaneDone{Resp: unknownResp{}})
}

type unknownResp struct{ core.ControlPlaneResponse }

func TestDiffMessages(t *testing.T) {
	if msg := Diff(ObjectResult{}, ObjectResult{Data: []byte{}}); msg != "" {
		t.Errorf("Diff reported %q for nil and empty data.", msg)
	}
	tests := []struct {
		name     string
		expected ObjectResult
		actual   ObjectResult
		want     string
	}{
		{"code", ObjectResult{}, ObjectResult{Code: core.ErrIO},
			"The field code differs: the expected result has OK, but the actual result has ErrIO."},
		{"outcome", ObjectResult{}, ObjectResult{Outcome: core.Applied},
			"The field outcome differs: the expected result has NotApplied, but the actual result has Applied."},
		{"data", ObjectResult{Data: []byte("a")}, ObjectResult{Data: []byte("b")},
			"The field data differs: the expected result has (len 1) \"a\", but the actual result has (len 1) \"b\"."},
		{"keys", ObjectResult{Keys: []string{"a"}}, ObjectResult{},
			"The field keys differs: the expected result has (len 1) [\"a\"], but the actual result has (len 0) []."},
	}
	for _, tc := range tests {
		if got := Diff(tc.expected, tc.actual); got != tc.want {
			t.Errorf("%s: Diff returned %q, but %q was expected.", tc.name, got, tc.want)
		}
	}
	cp := DiffControlPlane(ControlPlaneResult{End: 1}, ControlPlaneResult{End: 2})
	if want := "The field end differs: the expected result has 1, but the actual result has 2."; cp != want {
		t.Errorf("DiffControlPlane returned %q, but %q was expected.", cp, want)
	}
	cp = DiffControlPlane(ControlPlaneResult{}, ControlPlaneResult{Topics: []core.TopicInfo{{Name: "t", Partitions: 2}}})
	if !strings.HasPrefix(cp, "The field topics differs") {
		t.Errorf("DiffControlPlane returned %q for different topics.", cp)
	}
}

func TestDescribeTreatsNilAndEmptyTheSame(t *testing.T) {
	if a, b := (ObjectResult{}).Describe(), (ObjectResult{Data: []byte{}, Keys: []string{}}).Describe(); a != b {
		t.Errorf("Describe differs for nil and empty: %q and %q.", a, b)
	}
	if a, b := (ControlPlaneResult{}).Describe(), (ControlPlaneResult{Assigned: []core.BatchRange{}, Topics: []core.TopicInfo{}}).Describe(); a != b {
		t.Errorf("Describe differs for nil and empty: %q and %q.", a, b)
	}
	got := (ControlPlaneResult{End: 4, Ranges: []core.BatchRange{{Base: 1}}}).Describe()
	for _, part := range []string{"code OK", "outcome NotApplied", "assigned (len 0)", "ranges (len 1)", "end 4", "topics (len 0)"} {
		if !strings.Contains(got, part) {
			t.Errorf("Describe output %q does not contain %q.", got, part)
		}
	}
}
