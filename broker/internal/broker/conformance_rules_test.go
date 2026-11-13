package broker

import "testing"

// The old C1 test checked each rule below in its own if statement. This test
// builds a good answer and breaks it in one way at a time, to show that
// equalC1 still fails for each rule.
func TestC1ComparisonRules(t *testing.T) {
	want := c1result{Kind: "fetch", TopicPartition: c1TPs[0], From: 1, HWM: 3, LSO: 3, Log: []string{"a", "b", "c"}}
	good := func() c1result {
		return c1result{Kind: "fetch", TopicPartition: c1TPs[0], From: 1, HWM: 3, LSO: 3, Batches: 1,
			Records: []c1record{{0, "a"}, {1, "b"}, {2, "c"}}}
	}
	if err := equalC1(want, good()); err != nil {
		t.Fatalf("a good fetch answer was refused: %v", err)
	}
	breaks := map[string]func(*c1result){
		"wrong high watermark":                   func(r *c1result) { r.HWM = 4 },
		"error code where none is expected":      func(r *c1result) { r.Err = errOffsetOutOfRange },
		"wrong last stable offset":               func(r *c1result) { r.LSO = 2 },
		"log start is not 0":                     func(r *c1result) { r.LogStart = 1 },
		"batches are not well formed":            func(r *c1result) { r.Problem = "crc mismatch" },
		"no batch though the log has records":    func(r *c1result) { r.Batches, r.Records = 0, nil },
		"wrong value":                            func(r *c1result) { r.Records[1].Value = "x" },
		"record beyond the spec log":             func(r *c1result) { r.Records = append(r.Records, c1record{3, "d"}) },
		"stopped before the high watermark":      func(r *c1result) { r.Records = r.Records[:2] },
		"asked for another offset than the spec": func(r *c1result) { r.From = 2 },
		"wrong kind of answer":                   func(r *c1result) { r.Kind = "list" },
	}
	for name, mutate := range breaks {
		t.Run(name, func(t *testing.T) {
			got := good()
			mutate(&got)
			if equalC1(want, got) == nil {
				t.Fatalf("the answer %+v was accepted, but it breaks the rule %q", got, name)
			}
		})
	}
	small := want
	small.Small = true
	two := good()
	two.Batches = 2
	if equalC1(small, two) == nil {
		t.Fatal("a small fetch with two batches was accepted")
	}
	atHWM := c1result{Kind: "fetch", From: 3, HWM: 3, LSO: 3, Log: []string{"a", "b", "c"}}
	if err := equalC1(atHWM, c1result{Kind: "fetch", From: 3, HWM: 3, LSO: 3}); err != nil {
		t.Fatalf("an empty fetch at the high watermark was refused: %v", err)
	}
	if equalC1(atHWM, c1result{Kind: "fetch", From: 3, HWM: 3, LSO: 3, Batches: 1}) == nil {
		t.Fatal("a batch at the high watermark was accepted")
	}
	outOfRange := c1result{Kind: "fetch", From: 4, HWM: 3, Err: errOffsetOutOfRange}
	if err := equalC1(outOfRange, c1result{Kind: "fetch", From: 4, HWM: 3, Err: errOffsetOutOfRange}); err != nil {
		t.Fatalf("a right out-of-range answer was refused: %v", err)
	}
	if equalC1(outOfRange, c1result{Kind: "fetch", From: 4, HWM: 3}) == nil {
		t.Fatal("no error beyond the high watermark was accepted")
	}
	produce := c1result{Kind: "produce", Produced: []c1produced{{c1TPs[0], errNone, 5}}}
	for _, bad := range []c1result{
		{Kind: "produce", Produced: []c1produced{{c1TPs[0], errNone, 6}}},
		{Kind: "produce", Produced: []c1produced{{c1TPs[0], errInvalidRequest, 5}}},
		{Kind: "produce"},
	} {
		if equalC1(produce, bad) == nil {
			t.Fatalf("the produce answer %+v was accepted", bad)
		}
	}
	list := c1result{Kind: "list", TopicPartition: c1TPs[0], ListOffset: 4}
	if equalC1(list, c1result{Kind: "list", TopicPartition: c1TPs[0], ListOffset: 3}) == nil {
		t.Fatal("a wrong ListOffsets answer was accepted")
	}
	if equalC1(list, c1result{Kind: "list", TopicPartition: c1TPs[0], ListOffset: 4, Err: errUnknownTopicOrPart}) == nil {
		t.Fatal("a ListOffsets error was accepted")
	}
}
