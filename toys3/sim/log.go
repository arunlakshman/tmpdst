package sim

import (
	"fmt"
	"strings"

	"manifester/toys3/core"
	"manifester/toys3/format"
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
	if p, ok := format.DecodeObject(b); ok {
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
