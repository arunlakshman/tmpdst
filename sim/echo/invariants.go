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

// liveness: at the end no request is pending, and every request invoked in
// Settling after a grace period succeeded. The grace covers faults planned in
// Chaos landing late, connections damaged in Chaos and a node killed at the
// deadline of a Stop issued in Chaos.
type liveness struct{ ph sim.Phases }

const livenessGrace = 6 * core.Second

func (*liveness) Name() string     { return "Liveness" }
func (*liveness) Observe(sim.View) {}
func (l *liveness) AtEnd(v sim.View) {
	from := sim.Time(l.ph.Chaos + livenessGrace)
	to := sim.Time(l.ph.Chaos + l.ph.Settling)
	for _, op := range v.History().Ops() {
		v.Assert().Always(op.Outcome != sim.OpPending, "echo: no request pending at end", core.Detail{Key: "op", Value: op.Op})
		if op.Invoke < from || op.Invoke >= to {
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

// faultCoverage turns "this fault family fired" into Sometimes properties, so
// Report.Coverage is empty only when every family fired somewhere in the
// campaign.
type faultCoverage struct{}

func (*faultCoverage) Name() string     { return "FaultCoverage" }
func (*faultCoverage) Observe(sim.View) {}

func (*faultCoverage) AtEnd(v sim.View) {
	n := func(name, key, val string) bool {
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
	a := v.Assert()
	a.Sometimes(n("sim_fault", "kind", "obj_503"), "echo: fault fired: object store 503")
	a.Sometimes(n("sim_fault", "kind", "obj_timeout"), "echo: fault fired: object store timeout")
	a.Sometimes(n("sim_fault", "kind", "obj_lost_response"), "echo: fault fired: object store lost response")
	a.Sometimes(n("sim_fault", "kind", "obj_outage"), "echo: fault fired: object store outage")
	a.Sometimes(n("sim_fault", "kind", "obj_get_corrupt"), "echo: fault fired: object store corrupt read")
	a.Sometimes(n("sim_fault", "kind", "cp_503"), "echo: fault fired: control plane 503")
	a.Sometimes(n("sim_fault", "kind", "cp_timeout"), "echo: fault fired: control plane timeout")
	a.Sometimes(n("sim_fault", "kind", "cp_lost_response"), "echo: fault fired: control plane lost response")
	a.Sometimes(n("sim_fault", "kind", "cp_outage"), "echo: fault fired: control plane outage")
	a.Sometimes(n("sim_fault", "kind", "partition"), "echo: fault fired: partition")
	a.Sometimes(n("sim_fault", "kind", "clog"), "echo: fault fired: clog")
	a.Sometimes(n("sim_fault", "kind", "random_close"), "echo: fault fired: random close")
	a.Sometimes(n("sim_fault", "kind", "black_hole"), "echo: fault fired: black hole")
	a.Sometimes(n("sim_crashed", "cause", "attrition"), "echo: fault fired: hard crash")
	a.Sometimes(n("sim_exited", "", ""), "echo: fault fired: graceful stop")
	a.Sometimes(n("echo_commit_retried", "", ""), "echo: a commit was retried after MaybeApplied")
	a.Sometimes(n("corruption_detected", "", ""), "echo: corruption detected on read")
}
