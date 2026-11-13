package conformance

import (
	"bytes"
	"strings"
	"testing"

	brokerspec "github.com/manifesterio/mds/broker/spec"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/check/spectest"
)

// The spec is the correct subject, so the broker suite must report no failure.
// This tests the suite itself (its generator, its compare rules and its
// invariants).
func TestBrokerSpecVsSpec(t *testing.T) {
	l := BrokerLockstep(NewSpecBrokerSubject)
	l.Checks = 60
	l.Check(t)
}

// badLog is a log with one mistake. It wraps the correct log (the spec).
type badLog struct {
	BrokerSubject
	name string
}

func (b badLog) Append(batch [][]byte) contract.Offset {
	base := b.BrokerSubject.Append(batch)
	if b.name == "append base is off by one" {
		return base + 1
	}
	return base
}

func (b badLog) Read(from contract.Offset, max int) ([]brokerspec.StoredRecord, bool) {
	records, ok := b.BrokerSubject.Read(from, max)
	switch b.name {
	case "read accepts an offset past the end":
		return records, true
	case "read drops the last record":
		if len(records) > 0 {
			records = records[:len(records)-1]
		}
	case "read changes a value":
		if len(records) > 0 {
			records[0].Value = append([]byte("x"), records[0].Value...)
		}
	}
	return records, ok
}

func (b badLog) HighWatermark() contract.Offset {
	h := b.BrokerSubject.HighWatermark()
	if b.name == "high watermark is one too low" && h > 0 {
		return h - 1
	}
	return h
}

// aliasLog keeps the slices that the caller gives to Append, and gives out its
// own slices in Read. The suite writes over both after the call.
type aliasLog struct{ values [][]byte }

func (l *aliasLog) Append(batch [][]byte) contract.Offset {
	base := contract.Offset(len(l.values))
	l.values = append(l.values, batch...)
	return base
}

func (l *aliasLog) Read(from contract.Offset, max int) ([]brokerspec.StoredRecord, bool) {
	if from < 0 || int(from) > len(l.values) {
		return nil, false
	}
	var out []brokerspec.StoredRecord
	for o := int(from); o < len(l.values) && (max <= 0 || len(out) < max); o++ {
		out = append(out, brokerspec.StoredRecord{Offset: contract.Offset(o), Value: l.values[o]})
	}
	return out, true
}

func (l *aliasLog) HighWatermark() contract.Offset { return contract.Offset(len(l.values)) }

// aliasAppendLog keeps the slices that the caller gives to Append, but copies
// the values in Read. Only the write over the batch after Append finds it.
type aliasAppendLog struct{ aliasLog }

func (l *aliasAppendLog) Read(from contract.Offset, max int) ([]brokerspec.StoredRecord, bool) {
	records, ok := l.aliasLog.Read(from, max)
	for i := range records {
		records[i].Value = bytes.Clone(records[i].Value)
	}
	return records, ok
}

// Each of these logs breaks one rule of the broker spec. The suite must fail
// for each of them within the seed budget, and it must not fail for a log with
// no mistake.
func TestPlantedBrokerBugsAreCaught(t *testing.T) {
	logs := map[spectest.Bug]func() BrokerSubject{
		"keeps the slices of the caller":                      func() BrokerSubject { return &aliasLog{} },
		"keeps the slices of the caller, but copies them out": func() BrokerSubject { return &aliasAppendLog{} },
	}
	for _, name := range []string{
		"append base is off by one",
		"read accepts an offset past the end",
		"read drops the last record",
		"read changes a value",
		"high watermark is one too low",
	} {
		logs[spectest.Bug(name)] = func() BrokerSubject { return badLog{BrokerSubject: NewSpecBrokerSubject(), name: name} }
	}
	var bugs []spectest.Bug
	for bug := range logs {
		bugs = append(bugs, bug)
	}
	spectest.RequireCaught(t, bugs, 60, func(bug spectest.Bug, seed uint64) bool {
		return BrokerLockstep(logs[bug]).RunSeed(seed) != nil
	})
	spectest.RequireClean(t, 60, func(seed uint64) bool {
		return BrokerLockstep(func() BrokerSubject { return badLog{BrokerSubject: NewSpecBrokerSubject(), name: "no mistake"} }).RunSeed(seed) != nil
	})
}

// The broker suite writes over the batch it gave to Append and over the
// records it got from Read. The failure report must show the operations as the
// generator made them.
func TestBrokerFailureReportShowsTheOriginalBatch(t *testing.T) {
	l := BrokerLockstep(func() BrokerSubject {
		return badLog{BrokerSubject: NewSpecBrokerSubject(), name: "read accepts an offset past the end"}
	})
	var f *spectest.Failure[BrokerOp]
	for seed := uint64(1); f == nil && seed <= 60; seed++ {
		f = l.RunSeed(seed)
	}
	if f == nil {
		t.Fatal("the bad log was not caught")
	}
	if code := l.GoCode(f.Steps); !strings.Contains(code, "conformance.BrokerOp{") || strings.Contains(code, "!") {
		t.Fatalf("the report does not show the operations as the generator made them:\n%s", code)
	}
}
