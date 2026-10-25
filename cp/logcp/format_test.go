package logcp

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"manifester/core"
	"manifester/cp/logsm"
)

func commitCmd(key string, tps ...core.TopicPartition) logsm.Entry {
	c := core.CommitSegment{SegmentKey: key}
	for i, tp := range tps {
		c.Batches = append(c.Batches, core.PendingBatch{TP: tp, Records: int64(i + 1), Loc: core.SegmentRef{Key: key, Off: int64(i) * 10, Len: 10}})
	}
	return logsm.Entry{Kind: logsm.KindCommit, Commit: c}
}

func tpOf(t string, p int32) core.TopicPartition { return core.TP(t, p) }

func TestEntryRoundTrip(t *testing.T) {
	e := Entry{Writer: "n1", Rank: 2, ID: "n1/x/7", TS: 1234567, Cmds: []logsm.Entry{
		commitCmd("a", tpOf("t", 0), tpOf("t", 3)),
		{Kind: logsm.KindCreateTopic, Topic: "u", Parts: 5},
	}}
	got, err := UnmarshalEntry(MarshalEntry(e, Version))
	if err != nil || !reflect.DeepEqual(got, e) {
		t.Fatalf("%v\n got %+v\nwant %+v", err, got, e)
	}
	noop, err := UnmarshalEntry(MarshalEntry(Entry{Writer: "w", ID: "i"}, Version))
	if err != nil || len(noop.Cmds) != 0 || noop.Writer != "w" {
		t.Fatalf("noop: %v %+v", err, noop)
	}
}

func TestEntryRawCommandsAreReused(t *testing.T) {
	c := commitCmd("a", tpOf("t", 0))
	e := Entry{Writer: "w", ID: "i", Cmds: []logsm.Entry{c}}
	plain := MarshalEntry(e, Version)
	e.raw = [][]byte{logsm.Encode(c)}
	if string(MarshalEntry(e, Version)) != string(plain) {
		t.Fatal("pre-encoded commands change the bytes")
	}
}

func TestEntryVersionAndDamage(t *testing.T) {
	good := MarshalEntry(Entry{Writer: "w", ID: "i", Cmds: []logsm.Entry{commitCmd("a", tpOf("t", 0))}}, Version)
	newer := append([]byte{Version + 1}, good[1:]...)
	var un *UnsupportedError
	if _, err := UnmarshalEntry(newer); !errors.As(err, &un) || un.Version != Version+1 || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("newer format: %v", err)
	}
	for name, b := range map[string][]byte{
		"empty":     nil,
		"version 0": append([]byte{0}, good[1:]...),
		"truncated": good[:len(good)-3],
		"trailing":  append(append([]byte(nil), good...), 9),
		"bad count": append(append([]byte(nil), good[:5]...), 0xff, 0xff, 0xff, 0x7f),
	} {
		if _, err := UnmarshalEntry(b); err == nil || errors.As(err, &un) {
			t.Errorf("%s: accepted or misclassified: %v", name, err)
		}
	}
	for i := 1; i < len(good); i++ { // no prefix may decode
		if _, err := UnmarshalEntry(good[:i]); err == nil {
			t.Fatalf("prefix of %d bytes decoded", i)
		}
	}
}

func FuzzUnmarshalEntry(f *testing.F) {
	f.Add(MarshalEntry(Entry{Writer: "w", ID: "i", Cmds: []logsm.Entry{commitCmd("a", tpOf("t", 0))}}, Version))
	f.Fuzz(func(t *testing.T, b []byte) {
		if e, err := UnmarshalEntry(b); err == nil {
			if again, err := UnmarshalEntry(MarshalEntry(e, Version)); err != nil || !reflect.DeepEqual(again, e) {
				t.Fatalf("re-encode: %v", err)
			}
		}
	})
}
