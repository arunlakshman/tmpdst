package metadb

import (
	"errors"
	"strings"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
)

// brokenKV fails every transaction.
type brokenKV struct{}

func (*brokenKV) View(func(engine.Txn)) error { return errors.New("disk gone") }

func (*brokenKV) Update(func(engine.Txn)) error { return errors.New("disk gone") }

func (*brokenKV) Close() error { return nil }

func TestStoreFailureIsFailStop(t *testing.T) {
	a := New(&brokenKV{})
	if _, err := a.Write(0, contract.CommitSegment{SegmentKey: "k"}); err == nil {
		t.Error("Write must return the store error")
	}
	if _, err := a.Read(contract.DescribeTopics{}); err == nil {
		t.Error("Read must return the store error")
	}
	for name, f := range map[string]func(){
		"Apply":       func() { a.Apply(0, Entry{Kind: KindCommit, Op: contract.CommitSegment{SegmentKey: "k"}}) },
		"CreateTopic": func() { a.CreateTopic("t", 1) },
	} {
		func() {
			defer func() {
				if r := recover(); r == nil || !strings.Contains(r.(error).Error(), "store failed") {
					t.Errorf("%s: recover = %v", name, r)
				}
			}()
			f()
		}()
	}
}
