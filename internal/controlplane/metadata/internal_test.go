package metadata

import (
	"errors"
	"strings"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/core"
)

// brokenKV fails every transaction.
type brokenKV struct{}

func (*brokenKV) View(func(kv.Transaction)) error { return errors.New("disk gone") }

func (*brokenKV) Update(func(kv.Transaction)) error { return errors.New("disk gone") }

func (*brokenKV) Close() error { return nil }

func TestStoreFailureIsFailStop(t *testing.T) {
	a := New(&brokenKV{})
	if _, err := a.Commit(0, core.CommitSegment{SegmentKey: "k"}); err == nil {
		t.Error("Commit must return the store error")
	}
	for name, f := range map[string]func(){
		"Exec":        func() { a.Exec(core.ControlPlaneCall{Op: core.DescribeTopics{}}) },
		"CreateTopic": func() { a.CreateTopic("t", 1) },
		"End":         func() { a.End(core.NewTopicPartition("t", 0)) },
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
