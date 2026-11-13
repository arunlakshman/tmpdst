package echo

import (
	"strings"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/internal/preset"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/truth"
	"github.com/manifesterio/mds/broker/internal/wire/segmentformat"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

type ackFact struct {
	off     int64
	key     string
	payload []byte
}

func parseAck(f machine.Fact) (ackFact, bool) {
	o, ok1 := f.Field("offset").(machine.Int64Value)
	k, ok2 := f.Field("key").(machine.StringValue)
	p, ok3 := f.Field("payload").(machine.Bytes)
	return ackFact{int64(o), string(k), p}, ok1 && ok2 && ok3
}

// ackedMeansCommitted: on the Step that acks, the offset is committed in the
// control plane to a segment that exists in the object store and decodes to
// the acked payload.
type ackedMeansCommitted struct {
	cur  kernel.Cursor
	objs truth.ObjectStore
	cp   truth.ControlPlane
}

func (*ackedMeansCommitted) Name() string      { return "AckedIsCommitted" }
func (*ackedMeansCommitted) AtEnd(kernel.View) {}

func (i *ackedMeansCommitted) Observe(v kernel.View) {
	for _, tf := range v.Timeline().Since("echo_acked", &i.cur) {
		a, ok := parseAck(tf.Fact)
		v.Assert().Always(ok, ackFactWellFormed)
		if !ok {
			continue
		}
		det := []assert.Pair{{Key: "offset", Value: a.off}, {Key: "key", Value: a.key}}
		var loc core.SegmentRef
		found := false
		for _, r := range i.cp.Committed(topicPartition()) {
			if int64(r.Base) == a.off && r.Count == 1 && r.Loc.Key == a.key {
				loc, found = r.Loc, true
			}
		}
		v.Assert().Always(found, ackedOffsetCommitted, det...)
		obj, have := i.objs.Object(a.key)
		v.Assert().Always(have, ackedSegmentExistsObjectStore, det...)
		if !found || !have {
			continue
		}
		same := false
		if segment, err := segmentformat.Decode(obj); err == nil {
			for j, e := range segment.Entries {
				if e.Off == loc.Off && e.Len == loc.Len {
					same = string(segment.Batches[j].Data) == string(a.payload)
				}
			}
		}
		v.Assert().Always(same, ackedSegmentHoldsAckedPayload, det...)
	}
}

// readWhatWasAcked: a GET of an acked offset returns the acked payload or an error,
// never a different payload.
type readWhatWasAcked struct {
	hist    history.Reader[Op, string]
	cur     kernel.Cursor
	acked   map[int64][]byte
	checked []bool
}

func (*readWhatWasAcked) Name() string      { return "ReadYourAck" }
func (*readWhatWasAcked) AtEnd(kernel.View) {}

func (i *readWhatWasAcked) Observe(v kernel.View) {
	for _, tf := range v.Timeline().Since("echo_acked", &i.cur) {
		a, ok := parseAck(tf.Fact)
		if !ok {
			continue
		}
		if prev, dup := i.acked[a.off]; dup {
			v.Assert().Always(string(prev) == string(a.payload), offsetAckedOnePayload, assert.Pair{Key: "offset", Value: a.off})
		}
		i.acked[a.off] = a.payload
	}
	ops := i.hist.Ops()
	for len(i.checked) < len(ops) {
		i.checked = append(i.checked, false)
	}
	for j, operation := range ops {
		if i.checked[j] || operation.Outcome == history.Pending {
			continue
		}
		i.checked[j] = true
		g, ok := operation.Op.(GetOp)
		res := operation.Result
		if !ok {
			continue
		}
		if rest, has := strings.CutPrefix(res, "VAL "); has {
			want, acked := i.acked[g.Offset]
			v.Assert().Always(acked && rest == string(want), getReturnsAckedPayload,
				assert.Pair{Key: "offset", Value: g.Offset}, assert.Pair{Key: "got", Value: rest})
		}
	}
}

type liveness struct{ hist history.Reader[Op, string] }

func (*liveness) Name() string        { return "Liveness" }
func (*liveness) Observe(kernel.View) {}

func (l *liveness) AtEnd(v kernel.View) {
	settling, ok1 := preset.PhaseStart(v, kernel.PhaseRecovering)
	settle, ok2 := preset.PhaseStart(v, kernel.PhaseRecover)
	v.Assert().Always(ok1 && ok2, kernelAnnouncedSettlingAndSettle)
	from, to := settling+kernel.Time(preset.ProgressGraceTime), settle
	for _, operation := range l.hist.Ops() {
		v.Assert().Always(operation.Outcome != history.Pending, noRequestPendingAtEnd, assert.Pair{Key: "op", Value: operation.Op})
		if !ok1 || !ok2 || operation.Invoke < from || operation.Invoke >= to {
			continue
		}
		res := operation.Result
		good := false
		switch operation.Op.(type) {
		case PutOp:
			good = strings.HasPrefix(res, "OK ")
		case GetOp:
			good = strings.HasPrefix(res, "VAL ") || res == "ERR corrupt"
		}
		v.Assert().Always(good, requestInSettlingSucceeds,
			assert.Pair{Key: "op", Value: operation.Op}, assert.Pair{Key: "result", Value: res}, assert.Pair{Key: "invoke", Value: operation.Invoke})
	}
	v.Assert().Sometimes(len(l.hist.Ops()) > 0, workloadIssuedRequests)
}
