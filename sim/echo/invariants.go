package echo

import (
	"strings"

	"manifester/core"
	"manifester/format"
	"manifester/sim"
)

func fieldOf(f core.Fact, key string) core.FieldValue {
	for _, x := range f.Fields {
		if x.Key == key {
			return x.Value
		}
	}
	return nil
}

type ackFact struct {
	off     int64
	key     string
	payload []byte
}

func parseAck(f core.Fact) (ackFact, bool) {
	o, ok1 := fieldOf(f, "offset").(core.I64)
	k, ok2 := fieldOf(f, "key").(core.Str)
	p, ok3 := fieldOf(f, "payload").(core.Bytes)
	return ackFact{int64(o), string(k), p}, ok1 && ok2 && ok3
}

// ackedIsCommitted: on the Step that acks, the offset is committed in the
// control plane to a segment that exists in the object store and decodes to
// the acked payload.
type ackedIsCommitted struct{ cur sim.Cursor }

func (*ackedIsCommitted) Name() string   { return "AckedIsCommitted" }
func (*ackedIsCommitted) AtEnd(sim.View) {}

func (i *ackedIsCommitted) Observe(v sim.View) {
	for _, tf := range v.Timeline().Since("echo_acked", &i.cur) {
		a, ok := parseAck(tf.Fact)
		v.Assert().Always(ok, "echo: ack fact is well formed")
		if !ok {
			continue
		}
		det := []core.Detail{{Key: "offset", Value: a.off}, {Key: "key", Value: a.key}}
		var loc core.SegmentRef
		found := false
		for _, r := range v.CP().Committed(tp()) {
			if int64(r.Base) == a.off && r.Count == 1 && r.Loc.Key == a.key {
				loc, found = r.Loc, true
			}
		}
		v.Assert().Always(found, "echo: acked offset is committed", det...)
		obj, have := v.Objects().Object(a.key)
		v.Assert().Always(have, "echo: acked segment exists in the object store", det...)
		if !found || !have {
			continue
		}
		same := false
		if seg, err := format.Decode(obj); err == nil {
			for j, e := range seg.Entries {
				if e.Off == loc.Off && e.Len == loc.Len {
					same = string(seg.Batches[j].Data) == string(a.payload)
				}
			}
		}
		v.Assert().Always(same, "echo: acked segment holds the acked payload", det...)
	}
}

// readYourAck: a GET of an acked offset returns the acked payload or an error,
// never a different payload.
type readYourAck struct {
	cur     sim.Cursor
	acked   map[int64][]byte
	checked []bool
}

func (*readYourAck) Name() string   { return "ReadYourAck" }
func (*readYourAck) AtEnd(sim.View) {}

func (i *readYourAck) Observe(v sim.View) {
	for _, tf := range v.Timeline().Since("echo_acked", &i.cur) {
		a, ok := parseAck(tf.Fact)
		if !ok {
			continue
		}
		if prev, dup := i.acked[a.off]; dup {
			v.Assert().Always(string(prev) == string(a.payload), "echo: an offset is acked with one payload", core.Detail{Key: "offset", Value: a.off})
		}
		i.acked[a.off] = a.payload
	}
	ops := v.History().Ops()
	for len(i.checked) < len(ops) {
		i.checked = append(i.checked, false)
	}
	for j, op := range ops {
		if i.checked[j] || op.Outcome == sim.OpPending {
			continue
		}
		i.checked[j] = true
		g, ok := op.Op.(GetOp)
		res, isStr := op.Result.(string)
		if !ok || !isStr {
			continue
		}
		if rest, has := strings.CutPrefix(res, "VAL "); has {
			want, acked := i.acked[g.Offset]
			v.Assert().Always(acked && rest == string(want), "echo: GET of an acked offset returns the acked payload",
				core.Detail{Key: "offset", Value: g.Offset}, core.Detail{Key: "got", Value: rest})
		}
	}
}

// livenessGrace is how long into Settling a request may still fail: faults
// planned in Chaos can land late (an engine op planned at the end of Chaos, a
// partition that outlives it), connections damaged in Chaos are only recycled
// at the phase change, and a node stopped at the end of Chaos is killed at its
// Stop deadline (5s of kernel grace) and then restarts.
const livenessGrace = 6 * core.Second

// liveness: at the end no request is pending, and every request invoked in
// Settling after livenessGrace succeeded. Settling is bounded by the kernel's
// "sim_phase" facts, not by configured times.
type liveness struct{}

func (*liveness) Name() string     { return "Liveness" }
func (*liveness) Observe(sim.View) {}

// phaseStart is when the kernel announced phase p, from its sim_phase fact.
func phaseStart(v sim.View, p sim.Phase) (sim.Time, bool) {
	for _, tf := range v.Timeline().Snapshot("sim_phase") {
		if ph, ok := fieldOf(tf.Fact, "phase").(core.U64); ok && sim.Phase(ph) == p {
			return tf.At, true
		}
	}
	return 0, false
}

func (l *liveness) AtEnd(v sim.View) {
	settling, ok1 := phaseStart(v, sim.PhaseSettling)
	settle, ok2 := phaseStart(v, sim.PhaseSettle)
	v.Assert().Always(ok1 && ok2, "echo: the kernel announced Settling and Settle")
	from, to := settling+sim.Time(livenessGrace), settle
	for _, op := range v.History().Ops() {
		v.Assert().Always(op.Outcome != sim.OpPending, "echo: no request pending at end", core.Detail{Key: "op", Value: op.Op})
		if !ok1 || !ok2 || op.Invoke < from || op.Invoke >= to {
			continue
		}
		res, _ := op.Result.(string)
		good := false
		switch op.Op.(type) {
		case PutOp:
			good = strings.HasPrefix(res, "OK ")
		case GetOp:
			good = strings.HasPrefix(res, "VAL ") || res == "ERR corrupt"
		}
		v.Assert().Always(good, "echo: request invoked in Settling succeeds",
			core.Detail{Key: "op", Value: op.Op}, core.Detail{Key: "result", Value: res}, core.Detail{Key: "invoke", Value: op.Invoke})
	}
	v.Assert().Sometimes(len(v.History().Ops()) > 0, "echo: workload issued requests")
}

// expectReachable is the campaign's coverage contract (Config.ExpectReachable):
// every fault family and recovery path must fire somewhere in the campaign.
// The engines and the broker declare these props where the event happens; the
// list lives here because only the campaign knows what it must see.
func expectReachable() []core.Prop {
	return []core.Prop{
		"obj 503 injected", "obj timeout injected", "obj lost response injected", "obj outage started", "obj get corrupted",
		"cp 503 injected", "cp timeout injected", "cp lost response injected", "cp outage started",
		"net: partition injected", "net: clog injected", "net: random_close injected", "net: black_hole injected",
		"echo: commit retried after MaybeApplied",
		"echo: corruption detected on read",
	}
}

// faultCoverage covers what only the kernel's facts show: a hard crash by
// attrition and a graceful stop. (Everything else is in expectReachable.)
type faultCoverage struct{}

func (*faultCoverage) Name() string     { return "FaultCoverage" }
func (*faultCoverage) Observe(sim.View) {}

func (*faultCoverage) AtEnd(v sim.View) {
	has := func(name, key, val string) bool {
		for _, tf := range v.Timeline().Snapshot(name) {
			if key == "" {
				return true
			}
			if s, ok := fieldOf(tf.Fact, key).(core.Str); ok && string(s) == val {
				return true
			}
		}
		return false
	}
	v.Assert().Sometimes(has("sim_crashed", "cause", "attrition"), "echo: fault fired: hard crash")
	v.Assert().Sometimes(has("sim_exited", "", ""), "echo: fault fired: graceful stop")
}
