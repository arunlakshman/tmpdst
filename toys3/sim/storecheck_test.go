package sim

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"manifester/toys3/control"
	"manifester/toys3/format"
	"manifester/toys3/veritas"
	"manifester/toys3/veritas/veritastest"
)

// Conformance check C1 (VERITAS-PROPOSAL.md): the real controller, on
// fault-free storage, behaves exactly like veritas.Store. It lives in sim's
// tests because it needs both storage backends, veritas.FS and
// StorageModel, and the controller; sim already imports control and
// veritastest, and nothing imports sim's tests, so there is no cycle.
//
// One operation is one request (or a clean restart). The check delivers it,
// runs every storage command it causes to completion (oldest first, on the
// backend) until nothing is pending, and compares the single reply with the
// veritas. Then, at quiescence, it checks the storage: decoding every object
// under obj/ gives exactly the veritas's map, tmp/ is empty, and nothing
// tripped.

const (
	storeRoot    = "/d"
	storeMaxSize = 64 // small, so the 400 path runs
)

// Keys of the alphabet, simplest first: valid ones (lower case only: on a
// case-folding filesystem "A" and "a" alias, a known gap out of scope here;
// "a/b" exercises escaping), then invalid ones (empty; one byte too long).
var (
	storeKeys        = []string{"a", "b", "c", "a/b"}
	storeLongKey     = strings.Repeat("k", format.MaxNameLen+1)
	storeInvalidKeys = []string{"", storeLongKey}
	storeAllKeys     = append(slices.Clone(storeKeys), storeInvalidKeys...)
)

// storeKind is the kind of a C1 operation, simplest first.
type storeKind uint8

const (
	kGet storeKind = iota
	kPut
	kDelete
	kCopy
	kRestart
)

// storeOp is one C1 operation. A Put's payload is not stored: it is derived
// from the request's ClientReq and Size (storePayload), so a printed
// sequence stays short and every payload is unique.
type storeOp struct {
	Kind     storeKind
	Key, Src string // Key is Dst for a Copy
	Size     int    // Put only
}

// Constructors, used by Format and by pasted regression sequences.
func stGet(k string) storeOp         { return storeOp{Kind: kGet, Key: k} }
func stPut(k string, n int) storeOp  { return storeOp{Kind: kPut, Key: k, Size: n} }
func stDelete(k string) storeOp      { return storeOp{Kind: kDelete, Key: k} }
func stCopy(src, dst string) storeOp { return storeOp{Kind: kCopy, Src: src, Key: dst} }
func stRestart() storeOp             { return storeOp{Kind: kRestart} }

// storePayload is the payload of a Put by request c of n bytes: a
// self-describing value (MakeValue) when it fits, else a short pattern.
func storePayload(c control.ClientReq, n int) []byte {
	if n >= ValueHeader {
		return MakeValue(c, n)
	}
	return veritastest.Pattern(n, 'a'+byte(c%26))
}

// storeBackend is fault-free storage for C1: the Storage port, run
// synchronously, plus its contents. Durable is the view that survives a
// crash (the same as Current for veritas.FS).
type storeBackend interface {
	veritastest.FSUnderTest
	Current() Snapshot
	Durable() Snapshot
}

func (s syncStorage) Current() Snapshot { return s.fs.Current() }
func (s syncStorage) Durable() Snapshot { return s.fs.Durable() }

func newModelBackend() storeBackend {
	return syncStorage{NewStorageModel(NewRand(1), Disturbances{}, format.ObjDir(storeRoot), format.TmpDir(storeRoot))}
}

// veritasBackend is veritas.FS as a storeBackend.
type veritasBackend struct{ *veritas.FS }

func newVeritasBackend() storeBackend {
	return veritasBackend{veritas.NewFS(format.ObjDir(storeRoot), format.TmpDir(storeRoot))}
}

func (v veritasBackend) Current() Snapshot {
	var snap Snapshot
	for _, p := range v.Paths() {
		data, _ := v.ReadFile(p)
		snap.Files = append(snap.Files, FileState{Path: p, Data: data})
	}
	return snap
}

func (v veritasBackend) Durable() Snapshot { return v.Current() }

// storeEnv is a fixed control.Env: the controller draws no randomness today,
// and buggify is off.
type storeEnv struct{}

func (storeEnv) Uint64() uint64          { return 4 }
func (storeEnv) Intn(n int) int          { return 0 }
func (storeEnv) Maybe(control.Site) bool { return false }

// storePair is the controller on its storage, and the veritas.
type storePair struct {
	factory control.Factory
	backend storeBackend
	ctl     control.Machine
	ver     *veritas.Store
	now     control.Time
	next    control.ClientReq // last ClientReq issued; unique across restarts
	oracle  *Oracle           // C4 only: records the history if set
}

func (p *storePair) cfg() control.Config {
	return control.Config{Dir: storeRoot, MaxObjectSize: storeMaxSize}
}

// boot starts a new controller incarnation on the same storage and runs its
// recovery to completion.
func (p *storePair) boot() error {
	p.ctl = p.factory(storeEnv{}, p.cfg())
	out, err := p.deliver(control.Started{})
	if err != nil {
		return fmt.Errorf("boot: %w", err)
	}
	if len(out) > 0 {
		return fmt.Errorf("boot: unexpected %#v", out[0])
	}
	return nil
}

// deliver steps b, then completes the storage commands it causes, oldest
// first, until none is pending. It returns the replies, and an error on a
// Trip or a panic.
func (p *storePair) deliver(b control.EventBody) (replies []control.Reply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("controller panicked: %v", r)
		}
	}()
	var pending []control.Actuation
	for {
		p.now++
		for _, a := range p.ctl.Step(control.Event{At: p.now, Body: b}) {
			switch a := a.(type) {
			case control.Actuation:
				pending = append(pending, a)
			case control.Reply:
				replies = append(replies, a)
			case control.Trip:
				return replies, fmt.Errorf("tripped: %s", a.Cause)
			}
		}
		if len(pending) == 0 {
			return replies, nil
		}
		b = p.backend.Exec(pending[0])
		pending = pending[1:]
	}
}

// request returns op as the control.Op that request c sends.
func (op storeOp) request(c control.ClientReq) control.Op {
	switch op.Kind {
	case kGet:
		return control.Get{Key: op.Key}
	case kPut:
		return control.Put{Key: op.Key, Data: storePayload(c, op.Size)}
	case kDelete:
		return control.Delete{Key: op.Key}
	case kCopy:
		return control.Copy{Src: op.Src, Dst: op.Key}
	}
	panic("storeOp.request: not a request")
}

func (p *storePair) step(op storeOp) error {
	if op.Kind == kRestart {
		return p.boot() // the veritas has no restarts
	}
	p.next++
	c := p.next
	req := op.request(c)
	wantStatus, wantBody := p.ver.Apply(req)
	if p.oracle != nil {
		p.oracle.Invoke(p.now, 0, control.Request{Client: c, Op: req})
	}
	replies, err := p.deliver(control.Request{Client: c, Op: req})
	if err != nil {
		return err
	}
	if len(replies) != 1 || replies[0].Client != c {
		return fmt.Errorf("want one reply to c%d, got %#v", c, replies)
	}
	got := replies[0]
	if p.oracle != nil {
		p.oracle.Return(p.now, got)
	}
	if got.Status != wantStatus || !bytes.Equal(got.Body, wantBody) {
		return fmt.Errorf("replied %d %s, want %d %s", got.Status, describeBody(got.Body), wantStatus, describeBody(wantBody))
	}
	return nil
}

// describeBody names a body's writer when it is a self-describing value.
func describeBody(b []byte) string {
	if w, ok := ValueWriter(b); ok {
		return fmt.Sprintf("value-of-c%d (%d bytes)", w, len(b))
	}
	if len(b) > 24 {
		return fmt.Sprintf("%d bytes %q...", len(b), b[:24])
	}
	return fmt.Sprintf("%q", b)
}

// invariant checks the storage at quiescence against the veritas.
func (p *storePair) invariant() error {
	cur := p.backend.Current()
	for _, f := range cur.Files {
		if strings.HasPrefix(f.Path, format.TmpDir(storeRoot)+"/") {
			return fmt.Errorf("tmp/ not empty at quiescence: %s", f.Path)
		}
	}
	if err := p.objMatches("current", cur); err != nil {
		return err
	}
	return p.objMatches("durable", p.backend.Durable())
}

// objMatches checks that decoding every file under obj/ in snap gives
// exactly the veritas's map.
func (p *storePair) objMatches(view string, snap Snapshot) error {
	prefix := format.ObjDir(storeRoot) + "/"
	var keys []string
	for _, f := range snap.Files {
		name, ok := strings.CutPrefix(f.Path, prefix)
		if !ok {
			continue
		}
		key, ok := format.UnescapeKey(name)
		if !ok {
			return fmt.Errorf("%s obj/: %q is not an escaped key", view, name)
		}
		payload, ok := format.DecodeObject(f.Data)
		if !ok {
			return fmt.Errorf("%s obj/: %q is torn or corrupt", view, key)
		}
		want, ok := p.ver.Value(key)
		if !ok {
			return fmt.Errorf("%s obj/: holds %q, which the veritas does not", view, key)
		}
		if !bytes.Equal(payload, want) {
			return fmt.Errorf("%s obj/%s: %s, veritas %s", view, key, describeBody(payload), describeBody(want))
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	if want := p.ver.Keys(); !slices.Equal(keys, want) {
		return fmt.Errorf("%s obj/ holds keys %q, veritas %q", view, keys, want)
	}
	return nil
}

// storeCheck is C1 for the controller factory builds, on storage from
// newBackend. Put sizes are at least minSize.
func storeCheck(factory control.Factory, newBackend func() storeBackend, minSize int) veritastest.Check[*storePair, storeOp] {
	var written []string // valid keys Put so far in the sequence being generated
	return veritastest.Check[*storePair, storeOp]{
		Name: "storeOp",
		New: func() *storePair {
			p := &storePair{factory: factory, backend: newBackend(), ver: veritas.NewStore(storeMaxSize)}
			if err := p.boot(); err != nil {
				panic(err)
			}
			return p
		},
		Gen: func(r *veritastest.Rand, i int) storeOp {
			if i == 0 {
				written = nil
			}
			return genStoreOp(r, &written, minSize)
		},
		Step:      func(p *storePair, op storeOp) error { return p.step(op) },
		Invariant: func(p *storePair) error { return p.invariant() },
		Shrink:    func(op storeOp) []storeOp { return shrinkStoreOp(op, minSize) },
		ShrinkAll: shrinkStoreAll,
		Format:    formatStoreOp,
		Ops:       100,
	}
}

// genStoreOp returns a random operation. Get, Delete and Copy mostly target
// keys already Put (written); sizes cluster at 0, storeMaxSize and
// storeMaxSize+1; a Copy is sometimes onto its own source; one key in 20 is
// invalid.
func genStoreOp(r *veritastest.Rand, written *[]string, minSize int) storeOp {
	key := func() string {
		switch k := r.Intn(20); {
		case k == 0:
			return storeInvalidKeys[r.Intn(len(storeInvalidKeys))]
		case k < 16 && len(*written) > 0:
			return (*written)[r.Intn(len(*written))]
		}
		return storeKeys[r.Intn(len(storeKeys))]
	}
	switch k := r.Intn(100); {
	case k < 20:
		return stGet(key())
	case k < 55:
		var n int
		switch r.Intn(6) {
		case 0:
			n = 0
		case 1:
			n = storeMaxSize
		case 2:
			n = storeMaxSize + 1
		case 3:
			n = ValueHeader
		default:
			n = r.Intn(storeMaxSize + 2)
		}
		k := key()
		if format.ValidKey(k) && !slices.Contains(*written, k) {
			*written = append(*written, k)
		}
		return stPut(k, max(n, minSize))
	case k < 70:
		return stDelete(key())
	case k < 95:
		src := key()
		if r.Intn(4) == 0 {
			return stCopy(src, src)
		}
		return stCopy(src, key())
	default:
		return stRestart()
	}
}

// shrinkStoreOp returns simpler versions of op: smaller sizes (not below
// minSize), then earlier keys.
func shrinkStoreOp(op storeOp, minSize int) []storeOp {
	var out []storeOp
	switch op.Kind {
	case kPut:
		for _, n := range []int{minSize, (op.Size + minSize) / 2, op.Size - 1} {
			if n >= minSize && n < op.Size && !slices.ContainsFunc(out, func(o storeOp) bool { return o.Size == n }) {
				out = append(out, stPut(op.Key, n))
			}
		}
		for _, k := range earlierKey(op.Key) {
			out = append(out, stPut(k, op.Size))
		}
	case kGet, kDelete:
		for _, k := range earlierKey(op.Key) {
			o := op
			o.Key = k
			out = append(out, o)
		}
	case kCopy:
		for _, k := range earlierKey(op.Src) {
			out = append(out, stCopy(k, op.Key))
		}
		for _, k := range earlierKey(op.Key) {
			out = append(out, stCopy(op.Src, k))
		}
	}
	return out
}

func earlierKey(k string) []string {
	if i := slices.Index(storeAllKeys, k); i > 0 {
		return storeAllKeys[:i]
	}
	return nil
}

// shrinkStoreAll returns ops with one key renamed to an earlier one
// everywhere, latest key first.
func shrinkStoreAll(ops []storeOp) [][]storeOp {
	var out [][]storeOp
	for i := len(storeAllKeys) - 1; i > 0; i-- {
		for _, to := range storeAllKeys[:i] {
			from, changed := storeAllKeys[i], false
			cand := make([]storeOp, len(ops))
			for j, op := range ops {
				if op.Key == from && op.Kind != kRestart {
					op.Key, changed = to, true
				}
				if op.Src == from && op.Kind == kCopy {
					op.Src, changed = to, true
				}
				cand[j] = op
			}
			if changed {
				out = append(out, cand)
			}
		}
	}
	return out
}

func formatStoreOp(op storeOp) string {
	q := func(k string) string {
		if k == storeLongKey {
			return "storeLongKey"
		}
		return fmt.Sprintf("%q", k)
	}
	switch op.Kind {
	case kGet:
		return fmt.Sprintf("stGet(%s)", q(op.Key))
	case kPut:
		return fmt.Sprintf("stPut(%s, %d)", q(op.Key), op.Size)
	case kDelete:
		return fmt.Sprintf("stDelete(%s)", q(op.Key))
	case kCopy:
		return fmt.Sprintf("stCopy(%s, %s)", q(op.Src), q(op.Key))
	case kRestart:
		return "stRestart()"
	}
	return fmt.Sprintf("%#v", op)
}

// storeBackends are C1's two storage backends.
var storeBackends = []struct {
	name string
	new  func() storeBackend
}{
	{"veritasFS", newVeritasBackend},
	{"StorageModel", newModelBackend},
}

// conformanceOptions maps the -seeds, -seed and -duration flags onto
// veritastest.Options; run is the -run pattern of the test.
func conformanceOptions(run string) veritastest.Options {
	o := veritastest.Options{
		Seeds: *flagSeeds,
		Seed:  *flagSeed,
		Replay: func(seed uint64) string {
			return fmt.Sprintf("go test ./toys3/sim -run '%s' -seed=%d -v", run, seed)
		},
	}
	if *flagDuration > 0 {
		start := time.Now()
		o.Stop = func() bool { return time.Since(start) >= *flagDuration }
	}
	return o
}

// TestC1ControllerMatchesVeritasStore is conformance check C1: the
// controller, on each fault-free storage backend, replies exactly as
// veritas.Store does, and its storage holds exactly the veritas's objects.
// Flags: -seeds N, -seed S (replay one seed), -duration D (per backend), as
// for TestSeeds.
func TestC1ControllerMatchesVeritasStore(t *testing.T) {
	for _, b := range storeBackends {
		t.Run(b.name, func(t *testing.T) {
			c := storeCheck(control.NewMachine, b.new, 0)
			veritastest.Run(t, c, conformanceOptions("TestC1ControllerMatchesVeritasStore/"+b.name+"$"))
		})
	}
}

// TestC1Sequences pins sequences worth keeping: the edges of the size limit,
// Copy onto itself, invalid keys, and restarts between mutations.
func TestC1Sequences(t *testing.T) {
	seqs := [][]storeOp{
		{stPut("a", 0), stGet("a"), stPut("a", storeMaxSize), stPut("a", storeMaxSize+1), stGet("a")},
		{stCopy("a", "a"), stPut("a", 20), stCopy("a", "a"), stCopy("a", "b"), stDelete("a"), stGet("b")},
		{stPut("", 1), stPut(storeLongKey, 1), stGet(""), stDelete(storeLongKey), stCopy("a", ""), stCopy("", "a")},
		{stPut("a/b", 30), stRestart(), stGet("a/b"), stDelete("a/b"), stRestart(), stRestart(), stGet("a/b")},
	}
	for _, b := range storeBackends {
		c := storeCheck(control.NewMachine, b.new, 0)
		for i, ops := range seqs {
			if j, err := c.Replay(ops); err != nil {
				t.Errorf("%s: sequence %d, operation %d (%s): %v", b.name, i, j+1, formatStoreOp(ops[j]), err)
			}
		}
	}
}
