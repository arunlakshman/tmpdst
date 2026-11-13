package kernel

import (
	"fmt"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
)

// env is the machine.Env of a World: the run stream, buggify and assertions.
// Every machine, service and rule of the run shares it.
type env struct{ w *World }

func (e env) Uint64() uint64               { return e.w.rand.Uint64() }
func (e env) Intn(n int) int               { return e.w.rand.Intn(n) }
func (e env) Between(lo, hi int64) int64   { return e.w.rand.Between(lo, hi) }
func (e env) Chance(p float64) bool        { return e.w.rand.Chance(p) }
func (e env) Maybe(s fault.Site) bool      { return e.w.faults.Maybe(s) }
func (e env) Reachable(p assert.Reachable) { e.w.asserts.Reachable(assert.Property(p.Name())) }
func (e env) Sometimes(c bool, p assert.Sometimes) {
	e.w.asserts.Sometimes(c, assert.Property(p.Name()))
}

// NewID draws two Uint64 values and puts them in the id, little-endian.
func (e env) NewID() machine.ID {
	var id machine.ID
	e.w.rand.Bytes(id[:])
	return id
}

func (e env) MaybeP(s fault.Site, p float64) bool {
	return e.w.faults.MaybeP(s, p)
}

func (e env) Fault(s fault.Site, p float64) bool {
	return e.w.faults.Fault(s, p)
}

func (e env) Always(c bool, p assert.Always, d ...assert.Pair) {
	e.w.asserts.Always(c, assert.Property(p.Name()), d...)
}

func (e env) AlwaysOrUnreachable(c bool, p assert.AlwaysOrUnreachable, d ...assert.Pair) {
	e.w.asserts.AlwaysOrUnreachable(c, assert.Property(p.Name()), d...)
}

func (e env) Unreachable(p assert.Unreachable, d ...assert.Pair) {
	e.w.asserts.Unreachable(assert.Property(p.Name()), d...)
}

// logLine is one step line. The message is kept and turned into text only
// when the Result is built. Log text is never hashed.
type logLine struct {
	step uint64
	at   Time
	text string
	msg  machine.Message
}

func (l logLine) entry() LogEntry {
	text := l.text
	if l.msg != nil {
		s := fmt.Sprintf("%T %+v", l.msg, l.msg)
		if len(s) > 160 {
			s = s[:160] + "..."
		}
		text += " " + s
	}
	return LogEntry{Step: l.step, At: l.at, Text: text}
}

// logMsg keeps a log line when Config.Log or Config.Tail asks for lines.
func (w *World) logMsg(text string, msg machine.Message) {
	if !w.cfg.Log && w.tail == nil {
		return
	}
	l := logLine{step: w.steps, at: w.q.now, text: text, msg: msg}
	if w.cfg.Log {
		w.lines = append(w.lines, l)
	}
	if w.tail != nil {
		w.tail.Add(l)
	}
}

var _ machine.Env = env{}
