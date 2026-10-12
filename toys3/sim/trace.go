package sim

import (
	"fmt"

	"manifester/toys3/control"
)

// Trace hashing: every event, action and fact is folded into the run's trace
// hash as stable numeric tags and field values, never as Go type names or
// log renderings, so renaming a type or changing how the log prints it can
// never change a TraceHash.

// hashOp folds a client op into h.
func hashOp(h uint64, op control.Op) uint64 {
	switch o := op.(type) {
	case control.Put:
		return Fold(Fold(Fold(h, 1), hashString(o.Key)), hashBytes(o.Data))
	case control.Get:
		return Fold(Fold(h, 2), hashString(o.Key))
	case control.Delete:
		return Fold(Fold(h, 3), hashString(o.Key))
	case control.Copy:
		return Fold(Fold(Fold(h, 4), hashString(o.Src)), hashString(o.Dst))
	}
	return Fold(h, 5)
}

// hashEvent folds an event body into h.
func hashEvent(h uint64, b control.EventBody) uint64 {
	switch e := b.(type) {
	case control.Started:
		return Fold(h, 10)
	case control.Request:
		return hashOp(Fold(Fold(h, 11), uint64(e.Client)), e.Op)
	case control.FSWriteDone:
		return Fold(Fold(Fold(h, 12), uint64(e.IO)), uint64(e.Err))
	case control.FSFsyncDone:
		return Fold(Fold(Fold(h, 13), uint64(e.IO)), uint64(e.Err))
	case control.FSRenameDone:
		return Fold(Fold(Fold(h, 14), uint64(e.IO)), uint64(e.Err))
	case control.FSFsyncDirDone:
		return Fold(Fold(Fold(h, 15), uint64(e.IO)), uint64(e.Err))
	case control.FSReadDone:
		return Fold(Fold(Fold(Fold(h, 16), uint64(e.IO)), uint64(e.Err)), hashBytes(e.Data))
	case control.FSUnlinkDone:
		return Fold(Fold(Fold(h, 17), uint64(e.IO)), uint64(e.Err))
	case control.FSListDone:
		h = Fold(Fold(Fold(h, 18), uint64(e.IO)), uint64(e.Err))
		for _, n := range e.Names {
			h = Fold(h, hashString(n))
		}
		return h
	}
	return Fold(h, 19)
}

// hashAction folds an action into h.
func hashAction(h uint64, a control.Action) uint64 {
	switch x := a.(type) {
	case control.Reply:
		return Fold(Fold(Fold(Fold(h, 30), uint64(x.Client)), uint64(x.Status)), hashBytes(x.Body))
	case control.FSWrite:
		return Fold(Fold(Fold(Fold(h, 31), uint64(x.IO)), hashString(x.Path)), hashBytes(x.Data))
	case control.FSFsync:
		return Fold(Fold(Fold(h, 32), uint64(x.IO)), hashString(x.Path))
	case control.FSRename:
		return Fold(Fold(Fold(Fold(h, 33), uint64(x.IO)), hashString(x.From)), hashString(x.To))
	case control.FSFsyncDir:
		return Fold(Fold(Fold(h, 34), uint64(x.IO)), hashString(x.Dir))
	case control.FSRead:
		return Fold(Fold(Fold(h, 35), uint64(x.IO)), hashString(x.Path))
	case control.FSUnlink:
		return Fold(Fold(Fold(h, 36), uint64(x.IO)), hashString(x.Path))
	case control.FSList:
		return Fold(Fold(Fold(h, 37), uint64(x.IO)), hashString(x.Dir))
	case control.Trip:
		return Fold(Fold(h, 38), hashString(x.Cause))
	case control.Emit:
		return Fold(Fold(h, 39), hashString(factHashKey(x.Fact)))
	}
	return Fold(h, 40)
}

// factHashKey is the FROZEN hash encoding of a fact (trace-hash epoch 1). It
// reproduces, byte for byte, how facts were rendered when the trace hash was
// defined, so renaming fact types or changing DescribeFact never changes a
// TraceHash. Changing this function is a deliberate re-baseline: regenerate
// testdata/golden-v1.txt and update every trace hash quoted in the docs in the
// same commit.
func factHashKey(f control.Fact) string {
	switch x := f.(type) {
	case control.Committed:
		if x.Deleted {
			return fmt.Sprintf("Committed{c%d delete %q}", x.Client, x.Key)
		}
		return fmt.Sprintf("Committed{c%d %q crc=%08x}", x.Client, x.Key, x.Sum)
	case control.Recovered:
		return fmt.Sprintf("Recovered{temps=%d}", x.TempsRemoved)
	case control.CorruptObject:
		return fmt.Sprintf("CorruptObject{%q}", x.Key) // legacy spelling, on purpose
	}
	return fmt.Sprintf("%T", f)
}
