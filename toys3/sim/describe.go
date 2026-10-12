package sim

import (
	"fmt"
	"strings"

	"manifester/toys3/core"
)

// DescribeOp renders a client operation compactly.
func DescribeOp(op core.Op) string {
	switch o := op.(type) {
	case core.Put:
		if w, ok := ValueWriter(o.Data); ok {
			return fmt.Sprintf("PUT %q value-of-%d", o.Key, w)
		}
		return fmt.Sprintf("PUT %q (%d bytes)", o.Key, len(o.Data))
	case core.Get:
		return fmt.Sprintf("GET %q", o.Key)
	case core.Delete:
		return fmt.Sprintf("DELETE %q", o.Key)
	case core.Copy:
		return fmt.Sprintf("COPY %q -> %q", o.Src, o.Dst)
	}
	return fmt.Sprintf("%T", op)
}

// describePayload names the writer of an object file or value, if it has one.
func describePayload(b []byte) string {
	if b == nil {
		return "nil"
	}
	if p, ok := core.DecodeObject(b); ok {
		if w, ok := ValueWriter(p); ok {
			return fmt.Sprintf("obj(value-of-%d)", w)
		}
		return fmt.Sprintf("obj(%d bytes)", len(p))
	}
	if w, ok := ValueWriter(b); ok {
		return fmt.Sprintf("value-of-%d", w)
	}
	return fmt.Sprintf("raw(%d bytes)", len(b))
}

// DescribeEvent renders an event body for the log.
func DescribeEvent(b core.EventBody) string {
	switch e := b.(type) {
	case core.Started:
		return "Started"
	case core.Request:
		return fmt.Sprintf("Request{c%d %s}", e.Client, DescribeOp(e.Op))
	case core.FSWriteDone:
		return fmt.Sprintf("FSWriteDone{io%d %v}", e.IO, e.Err)
	case core.FSFsyncDone:
		return fmt.Sprintf("FSFsyncDone{io%d %v}", e.IO, e.Err)
	case core.FSRenameDone:
		return fmt.Sprintf("FSRenameDone{io%d %v}", e.IO, e.Err)
	case core.FSFsyncDirDone:
		return fmt.Sprintf("FSFsyncDirDone{io%d %v}", e.IO, e.Err)
	case core.FSReadDone:
		return fmt.Sprintf("FSReadDone{io%d %v %s}", e.IO, e.Err, describePayload(e.Data))
	case core.FSUnlinkDone:
		return fmt.Sprintf("FSUnlinkDone{io%d %v}", e.IO, e.Err)
	case core.FSListDone:
		return fmt.Sprintf("FSListDone{io%d %v %v}", e.IO, e.Err, e.Names)
	}
	return fmt.Sprintf("%T", b)
}

// DescribeAction renders an action for the log.
func DescribeAction(a core.Action) string {
	switch x := a.(type) {
	case core.Reply:
		s := fmt.Sprintf("Reply{c%d %d", x.Client, x.Status)
		if x.Body != nil {
			s += " " + describePayload(x.Body)
		}
		return s + "}"
	case core.FSWrite:
		return fmt.Sprintf("FSWrite{io%d %s %s}", x.IO, x.Path, describePayload(x.Data))
	case core.FSFsync:
		return fmt.Sprintf("FSFsync{io%d %s}", x.IO, x.Path)
	case core.FSRename:
		return fmt.Sprintf("FSRename{io%d %s -> %s}", x.IO, x.From, x.To)
	case core.FSFsyncDir:
		return fmt.Sprintf("FSFsyncDir{io%d %s}", x.IO, x.Dir)
	case core.FSRead:
		return fmt.Sprintf("FSRead{io%d %s}", x.IO, x.Path)
	case core.FSUnlink:
		return fmt.Sprintf("FSUnlink{io%d %s}", x.IO, x.Path)
	case core.FSList:
		return fmt.Sprintf("FSList{io%d %s}", x.IO, x.Dir)
	case core.Halt:
		return fmt.Sprintf("Halt{%s}", x.Reason)
	case core.Emit:
		return "Emit{" + DescribeFact(x.Fact) + "}"
	}
	return fmt.Sprintf("%T", a)
}

// DescribeFact renders a fact.
func DescribeFact(f core.Fact) string {
	switch x := f.(type) {
	case core.Committed:
		if x.Deleted {
			return fmt.Sprintf("Committed{c%d delete %q}", x.Client, x.Key)
		}
		return fmt.Sprintf("Committed{c%d %q crc=%08x}", x.Client, x.Key, x.Sum)
	case core.Recovered:
		return fmt.Sprintf("Recovered{temps=%d}", x.TempsRemoved)
	case core.CorruptObject:
		return fmt.Sprintf("CorruptObject{%q}", x.Key)
	}
	return fmt.Sprintf("%T", f)
}

// String renders one log line.
func (e LogEntry) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "#%-6d t=%-12v inc=%d %-11s", e.Step, fmtTime(e.At), e.Inc, e.Kind)
	if e.Event != nil {
		b.WriteString(" ")
		b.WriteString(DescribeEvent(e.Event))
	}
	if len(e.Actions) > 0 {
		b.WriteString(" =>")
		for _, a := range e.Actions {
			b.WriteString(" ")
			b.WriteString(DescribeAction(a))
		}
	}
	if e.Note != "" {
		b.WriteString(" ")
		b.WriteString(e.Note)
	}
	return b.String()
}

// FormatLog renders log entries, one per line.
func FormatLog(log []LogEntry) string {
	var b strings.Builder
	for _, e := range log {
		b.WriteString(e.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// fmtTime renders virtual time as milliseconds with microsecond precision.
func fmtTime(t core.Time) string {
	return fmt.Sprintf("%d.%06dms", int64(t)/int64(core.Millisecond), int64(t)%int64(core.Millisecond))
}

// hashOp folds a client op into h.
func hashOp(h uint64, op core.Op) uint64 {
	switch o := op.(type) {
	case core.Put:
		return Fold(Fold(Fold(h, 1), hashString(o.Key)), hashBytes(o.Data))
	case core.Get:
		return Fold(Fold(h, 2), hashString(o.Key))
	case core.Delete:
		return Fold(Fold(h, 3), hashString(o.Key))
	case core.Copy:
		return Fold(Fold(Fold(h, 4), hashString(o.Src)), hashString(o.Dst))
	}
	return Fold(h, 5)
}

// hashEvent folds an event body into h.
func hashEvent(h uint64, b core.EventBody) uint64 {
	switch e := b.(type) {
	case core.Started:
		return Fold(h, 10)
	case core.Request:
		return hashOp(Fold(Fold(h, 11), uint64(e.Client)), e.Op)
	case core.FSWriteDone:
		return Fold(Fold(Fold(h, 12), uint64(e.IO)), uint64(e.Err))
	case core.FSFsyncDone:
		return Fold(Fold(Fold(h, 13), uint64(e.IO)), uint64(e.Err))
	case core.FSRenameDone:
		return Fold(Fold(Fold(h, 14), uint64(e.IO)), uint64(e.Err))
	case core.FSFsyncDirDone:
		return Fold(Fold(Fold(h, 15), uint64(e.IO)), uint64(e.Err))
	case core.FSReadDone:
		return Fold(Fold(Fold(Fold(h, 16), uint64(e.IO)), uint64(e.Err)), hashBytes(e.Data))
	case core.FSUnlinkDone:
		return Fold(Fold(Fold(h, 17), uint64(e.IO)), uint64(e.Err))
	case core.FSListDone:
		h = Fold(Fold(Fold(h, 18), uint64(e.IO)), uint64(e.Err))
		for _, n := range e.Names {
			h = Fold(h, hashString(n))
		}
		return h
	}
	return Fold(h, 19)
}

// hashAction folds an action into h.
func hashAction(h uint64, a core.Action) uint64 {
	switch x := a.(type) {
	case core.Reply:
		return Fold(Fold(Fold(Fold(h, 30), uint64(x.Client)), uint64(x.Status)), hashBytes(x.Body))
	case core.FSWrite:
		return Fold(Fold(Fold(Fold(h, 31), uint64(x.IO)), hashString(x.Path)), hashBytes(x.Data))
	case core.FSFsync:
		return Fold(Fold(Fold(h, 32), uint64(x.IO)), hashString(x.Path))
	case core.FSRename:
		return Fold(Fold(Fold(Fold(h, 33), uint64(x.IO)), hashString(x.From)), hashString(x.To))
	case core.FSFsyncDir:
		return Fold(Fold(Fold(h, 34), uint64(x.IO)), hashString(x.Dir))
	case core.FSRead:
		return Fold(Fold(Fold(h, 35), uint64(x.IO)), hashString(x.Path))
	case core.FSUnlink:
		return Fold(Fold(Fold(h, 36), uint64(x.IO)), hashString(x.Path))
	case core.FSList:
		return Fold(Fold(Fold(h, 37), uint64(x.IO)), hashString(x.Dir))
	case core.Halt:
		return Fold(Fold(h, 38), hashString(x.Reason))
	case core.Emit:
		return Fold(Fold(h, 39), hashString(DescribeFact(x.Fact)))
	}
	return Fold(h, 40)
}
