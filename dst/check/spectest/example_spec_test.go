package spectest

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"

	"pgregory.net/rapid"
)

// This file has a tiny example: a set of small numbers (0 to 7). The spec is
// a bit mask. The subject keeps a durable log and checkpoints, so it has
// reboots, and it can have planted bugs.

// setOp is one op on the set.
type setOp interface{ isSetOp() }

// Has asks if Key is in the set.
type Has struct{ Key int }

// Add puts Key in the set.
type Add struct{ Key int }

// Remove takes Key out of the set.
type Remove struct{ Key int }

func (Has) isSetOp()    {}
func (Add) isSetOp()    {}
func (Remove) isSetOp() {}

// setRes is the answer: was Key in the set before the op.
type setRes struct{ Was bool }

func equalRes(want, got setRes) error {
	if want != got {
		return errors.New("different answers")
	}
	return nil
}

// apply runs op on a bit mask and returns the new mask and the answer.
func apply(mask uint8, op setOp) (uint8, setRes) {
	switch o := op.(type) {
	case Has:
		return mask, setRes{Was: mask&(1<<o.Key) != 0}
	case Add:
		return mask | 1<<o.Key, setRes{Was: mask&(1<<o.Key) != 0}
	case Remove:
		return mask &^ (1 << o.Key), setRes{Was: mask&(1<<o.Key) != 0}
	}
	return mask, setRes{}
}

// setSpec is the spec.
type setSpec struct{ mask uint8 }

func (s *setSpec) Apply(op setOp) setRes {
	var r setRes
	s.mask, r = apply(s.mask, op)
	return r
}

func (s *setSpec) StateHash() uint64 { return uint64(s.mask) }

// The planted bugs of the subject.
const (
	bugRemoveLast  Bug = "remove does nothing when the set has one number"
	bugCrashNoLog  Bug = "a crash forgets the log after the newest checkpoint"
	bugNoAnswerAdd Bug = "an add loses its answer after a failure was injected"
)

// checkpoint is a snapshot of the mask after the first n log entries.
type checkpoint struct {
	mask uint8
	n    int
}

// setSubject keeps every op in a durable log, and the mask in memory.
type setSubject struct {
	bug         Bug
	mem         uint8
	log         []setOp // durable
	checkpoints []checkpoint
	injected    bool
}

func (s *setSubject) Apply(op setOp) (setRes, error) {
	if _, isRemove := op.(Remove); isRemove && s.bug == bugRemoveLast && bits.OnesCount8(s.mem) == 1 {
		return setRes{Was: true}, nil
	}
	var r setRes
	s.mem, r = apply(s.mem, op)
	s.log = append(s.log, op)
	if _, isAdd := op.(Add); isAdd && s.bug == bugNoAnswerAdd && s.injected {
		return setRes{}, errors.New("the add is done, but its answer is lost")
	}
	return r, nil
}

func (s *setSubject) StateHash() uint64 { return uint64(s.mem) }

// rebuild sets mem from the newest checkpoint and the log after it.
func (s *setSubject) rebuild(withLog bool) {
	var c checkpoint
	if len(s.checkpoints) > 0 {
		c = s.checkpoints[len(s.checkpoints)-1]
	}
	s.mem = c.mask
	if !withLog {
		s.log = s.log[:c.n]
		return
	}
	for _, op := range s.log[c.n:] {
		s.mem, _ = apply(s.mem, op)
	}
}

func (s *setSubject) reboots() Reboots {
	return Reboots{
		Reboot: func() { s.rebuild(true) },
		DirtyReboot: func(keep int) {
			s.checkpoints = s.checkpoints[:len(s.checkpoints)-min(keep, len(s.checkpoints))]
			s.rebuild(s.bug != bugCrashNoLog)
		},
		Checkpoint: func() { s.checkpoints = append(s.checkpoints, checkpoint{mask: s.mem, n: len(s.log)}) },
		MaxKeep:    2,
	}
}

// genSetOp draws ops simplest first: Has, then Add, then Remove.
func genSetOp() *rapid.Generator[setOp] {
	key := rapid.IntRange(0, 7)
	return SimplestFirst(
		rapid.Map(key, func(k int) setOp { return Has{Key: k} }),
		rapid.Map(key, func(k int) setOp { return Add{Key: k} }),
		rapid.Map(key, func(k int) setOp { return Remove{Key: k} }),
	)
}

// setLockstep builds the lockstep check of the example with bug turned on
// ("" for none).
func setLockstep(bug Bug) Lockstep[setOp, setRes, *setSubject] {
	return Lockstep[setOp, setRes, *setSubject]{
		NewSpec:    func() Spec[setOp, setRes] { return &setSpec{} },
		NewSubject: func(*rapid.T) *setSubject { return &setSubject{bug: bug} },
		Ops:        genSetOp(),
		Equal:      equalRes,
		Reboots:    (*setSubject).reboots,
		Rerun:      Rerun{EnvPrefix: "P"},
	}
}

// naiveSet is a naive model of the set: a sorted slice.
type naiveSet struct {
	keys []int
	bug  bool // when true, Remove forgets to remove
}

func (n *naiveSet) Apply(op setOp) setRes {
	switch o := op.(type) {
	case Has:
		return setRes{Was: slices.Contains(n.keys, o.Key)}
	case Add:
		i, found := slices.BinarySearch(n.keys, o.Key)
		if !found {
			n.keys = slices.Insert(n.keys, i, o.Key)
		}
		return setRes{Was: found}
	case Remove:
		i, found := slices.BinarySearch(n.keys, o.Key)
		if found && !n.bug {
			n.keys = slices.Delete(n.keys, i, i+1)
		}
		return setRes{Was: found}
	}
	panic(fmt.Sprintf("unknown op %T", op))
}

func (n *naiveSet) StateHash() uint64 { return uint64(len(n.keys)) }
