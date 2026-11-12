package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// describe is a Describe call whose reads are in flight: one LocalRead per
// name, one reply when all are back.
type describe struct {
	call CallID
	out  []GroupDescription
	left int
	reqs []runtime.RequestID
}

// describeRead marks a request id as the read of name i of a describe.
type describeRead struct {
	d *describe
	i int
}

// describe handles a Describe call (rule 32): the log's description of each
// group, with the state from memory.
func (m *Machine) describe(s *serving, in Describe) {
	d := &describe{call: in.Call, out: make([]GroupDescription, len(in.Groups)), left: len(in.Groups)}
	if d.left == 0 {
		m.reply(in.Call, runtime.OK, DescribeAnswer{Groups: d.out})
		return
	}
	for i, name := range in.Groups {
		d.out[i].Name = name
		req := m.read(describeOp(name), false)
		s.reqs[req] = describeRead{d: d, i: i}
		d.reqs = append(d.reqs, req)
	}
	s.describes = append(s.describes, d)
}

// describeDone takes one read. A failed read fails the call.
func (m *Machine) describeDone(s *serving, r describeRead, done runtime.ControlPlaneDone) {
	d := r.d
	if done.Err != runtime.OK {
		m.endDescribe(s, d)
		m.reply(d.call, runtime.ErrUnavailable, nil)
		return
	}
	d.out[r.i].DescribeGroupResp = runtime.ResponseAs[contract.DescribeGroupResp](done)
	if d.left--; d.left > 0 {
		return
	}
	m.endDescribe(s, d)
	for i := range d.out {
		d.out[i].State = stateOf(s, d.out[i].Name)
	}
	m.reply(d.call, runtime.OK, DescribeAnswer{Groups: d.out})
}

// abandonRead forgets the describe of call, if there is one.
func (m *Machine) abandonRead(s *serving, call CallID) bool {
	i := slices.IndexFunc(s.describes, func(d *describe) bool { return d.call == call })
	if i >= 0 {
		m.endDescribe(s, s.describes[i])
	}
	return i >= 0
}

func (m *Machine) endDescribe(s *serving, d *describe) {
	for _, req := range d.reqs {
		delete(s.reqs, req)
	}
	s.describes = slices.DeleteFunc(s.describes, func(o *describe) bool { return o == d })
}

// stateOf is the Kafka state of a group in memory, Dead when there is none.
func stateOf(s *serving, name string) string {
	if g := s.groups[name]; g != nil {
		return g.phase.state()
	}
	return stateDead
}

// list handles a List call (rule 32): every group in memory, by name.
func (m *Machine) list(s *serving, in List) {
	out := make([]contract.GroupSummary, 0, len(s.names))
	for _, name := range s.names {
		g := s.groups[name]
		out = append(out, contract.GroupSummary{Name: name, ProtocolType: g.protoType, State: g.phase.state()})
	}
	m.reply(in.Call, runtime.OK, ListAnswer{Groups: out})
}
