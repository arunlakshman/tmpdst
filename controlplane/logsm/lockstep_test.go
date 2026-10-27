package logsm

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	capplier "manifester/controlplane/applier"
	"manifester/controlplane/applier/pebblekv"
	"manifester/core"
	"manifester/veritas"
)

// Stage 1 (decision 49): the X2 harness with the production applier as every
// replica's state machine and veritas as the independent oracle. The same seeds
// and the same faults as TestX2Seeds; additionally every apply and every read is
// answered by both machines and compared, and at the end the applier and the
// oracle must agree on a digest taken through the public queries.

// lockMachine runs the applier and the oracle on the same inputs.
type lockMachine struct {
	app  *capplier.Applier
	orc  *veritas.ControlPlane
	errs []string
}

func newLock(kv capplier.KV) *lockMachine {
	return &lockMachine{app: capplier.New(kv), orc: veritas.NewControlPlane()}
}

func (m *lockMachine) note(format string, a ...any) {
	m.errs = append(m.errs, fmt.Sprintf(format, a...))
}

// Exec returns the applier's answer, so the replica, the committers and the
// model checks of X2 all see what production would; any difference from the
// oracle is recorded.
func (m *lockMachine) Exec(a core.IOAction) core.Completion {
	got, want := m.app.Exec(a), m.orc.Exec(a)
	if !reflect.DeepEqual(got, want) {
		m.note("Exec %+v: applier %+v, oracle %+v", a, got, want)
	}
	if c, ok := a.(core.CPCall); ok {
		if cs, ok := c.Op.(core.CommitSegment); ok {
			m.probe(cs)
		}
	}
	return got
}

func (m *lockMachine) CreateTopic(name string, parts int32) core.ErrCode {
	got, want := m.app.CreateTopic(name, parts), m.orc.CreateTopic(name, parts)
	if got != want {
		m.note("CreateTopic(%q,%d): applier %v, oracle %v", name, parts, got, want)
	}
	return got
}

func (m *lockMachine) StateHash() uint64 { return m.orc.StateHash() }

// probe compares Lookup answers (several From and MaxBytes) on the partitions
// a commit touched.
func (m *lockMachine) probe(cs core.CommitSegment) {
	for _, b := range cs.Batches {
		end := m.orc.End(b.TP)
		if got := m.app.End(b.TP); got != end {
			m.note("End(%v): applier %d, oracle %d", b.TP, got, end)
		}
		qs := []core.Lookup{{TP: b.TP}, {TP: b.TP, From: end / 2, MaxBytes: 60}, {TP: b.TP, From: end}, {TP: b.TP, From: end - 1, MaxBytes: 1}}
		// MaxBytes at, just below and just above every running total of Len
		// (the stop rule is a >= on exactly these).
		var sum int64
		for _, r := range m.orc.Exec(core.CPCall{Op: qs[0]}).(core.CPDone).Resp.(core.LookupResp).Ranges {
			sum += r.Loc.Len
			for d := int64(-1); d <= 1; d++ {
				if sum+d > 0 {
					qs = append(qs, core.Lookup{TP: b.TP, MaxBytes: int32(sum + d)})
				}
			}
		}
		for _, q := range qs {
			got, want := m.app.Exec(core.CPCall{Op: q}), m.orc.Exec(core.CPCall{Op: q})
			if !reflect.DeepEqual(got, want) {
				m.note("Lookup %+v: applier %+v, oracle %+v", q, got, want)
			}
		}
	}
}

// queryDigest hashes everything the public queries can say: DescribeTopics for
// all topics, then End and every run of every partition.
func queryDigest(exec func(core.CPOp) core.CPDone) uint64 {
	h := fnv.New64a()
	d := exec(core.DescribeTopics{}).Resp.(core.DescribeTopicsResp)
	for _, t := range d.Topics {
		_, _ = fmt.Fprintf(h, "T%s/%d;", t.Name, t.Partitions)
		for p := int32(0); p < t.Partitions; p++ {
			l := exec(core.Lookup{TP: core.TP(t.Name, p)}).Resp.(core.LookupResp)
			_, _ = fmt.Fprintf(h, "P%d/%d:%+v;", p, l.End, l.Ranges)
		}
	}
	return h.Sum64()
}

func (m *lockMachine) verify() error {
	if len(m.errs) > 0 {
		return fmt.Errorf("%d lockstep mismatches, first: %s", len(m.errs), m.errs[0])
	}
	a := queryDigest(func(op core.CPOp) core.CPDone { return m.app.Exec(core.CPCall{Op: op}).(core.CPDone) })
	o := queryDigest(func(op core.CPOp) core.CPDone { return m.orc.Exec(core.CPCall{Op: op}).(core.CPDone) })
	if a != o {
		return fmt.Errorf("query digest: applier %x, oracle %x", a, o)
	}
	return nil
}

// lockSeed runs one X2 seed with lockstep machines over kvs from mk.
func lockSeed(seed uint64, mk func() capplier.KV) (result, error) {
	var ms []*lockMachine
	res, err := runSeedWith(seed, func() Machine {
		m := newLock(mk())
		ms = append(ms, m)
		return m
	})
	if err != nil {
		return res, err
	}
	for i, m := range ms {
		defer func() { _ = m.app.Close() }()
		if err := m.verify(); err != nil {
			return res, fmt.Errorf("replica %d: %w", i, err)
		}
		if m.app.StateHash() != ms[0].app.StateHash() {
			return res, fmt.Errorf("replica %d applier state hash differs from replica 0", i)
		}
	}
	return res, nil
}

func lockSeeds(t *testing.T, n int, mk func() capplier.KV) {
	var failed, slots atomic.Int64
	var wg sync.WaitGroup
	next := make(chan uint64)
	for range 4 {
		wg.Go(func() {
			for seed := range next {
				res, err := lockSeed(seed, mk)
				slots.Add(res.slots)
				if err != nil {
					failed.Add(1)
					t.Errorf("seed %d: %v", seed, err)
				}
			}
		})
	}
	for seed := uint64(1); seed <= uint64(n); seed++ {
		next <- seed
	}
	close(next)
	wg.Wait()
	t.Logf("lockstep seeds=%d failed=%d slots=%d", n, failed.Load(), slots.Load())
}

// TestX2LockstepMem is the stage 1 gate: LOGSM_SEEDS=100000 for the full run.
func TestX2LockstepMem(t *testing.T) {
	lockSeeds(t, seedCount(), func() capplier.KV { return capplier.NewMemKV() })
}

// TestX2LockstepPebble uses real Pebble stores (slower): LOGSM_PEBBLE_SEEDS seeds.
func TestX2LockstepPebble(t *testing.T) {
	n := 20
	if v, err := strconv.Atoi(os.Getenv("LOGSM_PEBBLE_SEEDS")); err == nil && v > 0 {
		n = v
	}
	dir := t.TempDir()
	var id atomic.Int64
	lockSeeds(t, n, func() capplier.KV {
		kv, err := pebblekv.Open(filepath.Join(dir, strconv.FormatInt(id.Add(1), 10)), pebblekv.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		return kv
	})
}
