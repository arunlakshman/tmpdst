package sim

import (
	"fmt"
	"strings"

	"manifester/toys3/control"
	"manifester/toys3/format"
)

// DescribeOp renders a client operation compactly.
func DescribeOp(op control.Op) string {
	switch o := op.(type) {
	case control.Put:
		if w, ok := ValueWriter(o.Data); ok {
			return fmt.Sprintf("PUT %q value-of-%d", o.Key, w)
		}
		return fmt.Sprintf("PUT %q (%d bytes)", o.Key, len(o.Data))
	case control.Get:
		return fmt.Sprintf("GET %q", o.Key)
	case control.Delete:
		return fmt.Sprintf("DELETE %q", o.Key)
	case control.Copy:
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
func DescribeEvent(b control.EventBody) string {
	switch e := b.(type) {
	case control.Started:
		return "Started"
	case control.Request:
		return fmt.Sprintf("Request{c%d %s}", e.Client, DescribeOp(e.Op))
	case control.FSWriteDone:
		return fmt.Sprintf("FSWriteDone{io%d %v}", e.IO, e.Err)
	case control.FSFsyncDone:
		return fmt.Sprintf("FSFsyncDone{io%d %v}", e.IO, e.Err)
	case control.FSRenameDone:
		return fmt.Sprintf("FSRenameDone{io%d %v}", e.IO, e.Err)
	case control.FSFsyncDirDone:
		return fmt.Sprintf("FSFsyncDirDone{io%d %v}", e.IO, e.Err)
	case control.FSReadDone:
		return fmt.Sprintf("FSReadDone{io%d %v %s}", e.IO, e.Err, describePayload(e.Data))
	case control.FSUnlinkDone:
		return fmt.Sprintf("FSUnlinkDone{io%d %v}", e.IO, e.Err)
	case control.FSListDone:
		return fmt.Sprintf("FSListDone{io%d %v %v}", e.IO, e.Err, e.Names)
	}
	return fmt.Sprintf("%T", b)
}

// DescribeAction renders an action for the log.
func DescribeAction(a control.Action) string {
	switch x := a.(type) {
	case control.Reply:
		s := fmt.Sprintf("Reply{c%d %d", x.Client, x.Status)
		if x.Body != nil {
			s += " " + describePayload(x.Body)
		}
		return s + "}"
	case control.FSWrite:
		return fmt.Sprintf("FSWrite{io%d %s %s}", x.IO, x.Path, describePayload(x.Data))
	case control.FSFsync:
		return fmt.Sprintf("FSFsync{io%d %s}", x.IO, x.Path)
	case control.FSRename:
		return fmt.Sprintf("FSRename{io%d %s -> %s}", x.IO, x.From, x.To)
	case control.FSFsyncDir:
		return fmt.Sprintf("FSFsyncDir{io%d %s}", x.IO, x.Dir)
	case control.FSRead:
		return fmt.Sprintf("FSRead{io%d %s}", x.IO, x.Path)
	case control.FSUnlink:
		return fmt.Sprintf("FSUnlink{io%d %s}", x.IO, x.Path)
	case control.FSList:
		return fmt.Sprintf("FSList{io%d %s}", x.IO, x.Dir)
	case control.Trip:
		return fmt.Sprintf("Trip{%s}", x.Cause)
	case control.Emit:
		return "Emit{" + DescribeTelemetry(x.Telemetry) + "}"
	}
	return fmt.Sprintf("%T", a)
}

// DescribeTelemetry renders a telemetry value.
func DescribeTelemetry(f control.Telemetry) string {
	switch x := f.(type) {
	case control.Committed:
		if x.Deleted {
			return fmt.Sprintf("Committed{c%d delete %q}", x.Client, x.Key)
		}
		return fmt.Sprintf("Committed{c%d %q crc=%08x}", x.Client, x.Key, x.Sum)
	case control.Recovered:
		return fmt.Sprintf("Recovered{temps=%d}", x.TempsRemoved)
	case control.CorruptionDetected:
		return fmt.Sprintf("CorruptionDetected{%q}", x.Key)
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
func fmtTime(t control.Time) string {
	return fmt.Sprintf("%d.%06dms", int64(t)/int64(control.Millisecond), int64(t)%int64(control.Millisecond))
}
