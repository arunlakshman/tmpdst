package control

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"manifester/toys3/format"
)

const root = "/d"

// testEnv is a fixed control.Env (the server does not draw randomness today).
type testEnv struct{}

func (testEnv) Uint64() uint64  { return 4 }
func (testEnv) Intn(n int) int  { return 0 }
func (testEnv) Maybe(Site) bool { return false }

// harness drives a Server by hand. FS actions are answered from an in-memory
// map (no crash semantics: every change is immediately "durable"). Issued FS
// actions wait in pending until the test completes them.
type harness struct {
	t       *testing.T
	s       *Server
	files   map[string][]byte // full path → raw file content
	pending []Action          // issued FS actions, not yet completed
	// fail, if set, decides the error an FS action completes with (OK = run it).
	fail func(Action) ErrCode
}

func newHarness(t *testing.T) *harness {
	return &harness{t: t, s: New(testEnv{}, Config{Dir: root, MaxObjectSize: 64}), files: map[string][]byte{}}
}

// boot delivers Started and runs recovery to completion.
func (h *harness) boot() {
	h.t.Helper()
	acts := h.step(Started{})
	if got := describe(acts); got != "list /d/tmp" {
		h.t.Fatalf("Started: got %q", got)
	}
	h.drain()
}

// step delivers one event and records the FS actions it issued.
func (h *harness) step(b EventBody) []Action {
	acts := h.s.Step(Event{Body: b})
	for _, a := range acts {
		if _, ok := ioOfAction(a); ok {
			h.pending = append(h.pending, a)
		}
	}
	return acts
}

// completeNext completes the oldest pending FS action and returns what the
// server did in response.
func (h *harness) completeNext() []Action {
	h.t.Helper()
	if len(h.pending) == 0 {
		h.t.Fatal("no pending FS action")
	}
	a := h.pending[0]
	h.pending = h.pending[1:]
	return h.step(h.exec(a))
}

// drain completes FS actions until none are pending and returns every action
// the server emitted along the way, in order.
func (h *harness) drain() []Action {
	var all []Action
	for len(h.pending) > 0 {
		all = append(all, h.completeNext()...)
	}
	return all
}

// do delivers a request and drains; it returns every resulting action.
func (h *harness) do(c ClientReq, op Op) []Action {
	acts := h.step(Request{Client: c, Op: op})
	return append(acts, h.drain()...)
}

// exec runs an FS action against the map and returns its completion.
func (h *harness) exec(a Action) EventBody {
	errc := OK
	if h.fail != nil {
		errc = h.fail(a)
	}
	switch a := a.(type) {
	case FSWrite:
		if errc == OK {
			h.files[a.Path] = slices.Clone(a.Data)
		}
		return FSWriteDone{IO: a.IO, Err: errc}
	case FSFsync:
		return FSFsyncDone{IO: a.IO, Err: errc}
	case FSFsyncDir:
		return FSFsyncDirDone{IO: a.IO, Err: errc}
	case FSRename:
		if errc == OK {
			d, ok := h.files[a.From]
			if !ok {
				return FSRenameDone{IO: a.IO, Err: ErrNotFound}
			}
			h.files[a.To] = d
			delete(h.files, a.From)
		}
		return FSRenameDone{IO: a.IO, Err: errc}
	case FSRead:
		if errc != OK {
			return FSReadDone{IO: a.IO, Err: errc}
		}
		d, ok := h.files[a.Path]
		if !ok {
			return FSReadDone{IO: a.IO, Err: ErrNotFound}
		}
		return FSReadDone{IO: a.IO, Data: d}
	case FSUnlink:
		if errc != OK {
			return FSUnlinkDone{IO: a.IO, Err: errc}
		}
		if _, ok := h.files[a.Path]; !ok {
			return FSUnlinkDone{IO: a.IO, Err: ErrNotFound}
		}
		delete(h.files, a.Path)
		return FSUnlinkDone{IO: a.IO}
	case FSList:
		if errc != OK {
			return FSListDone{IO: a.IO, Err: errc}
		}
		var names []string
		for p := range h.files {
			if rest, ok := strings.CutPrefix(p, a.Dir+"/"); ok {
				names = append(names, rest)
			}
		}
		slices.Sort(names)
		return FSListDone{IO: a.IO, Names: names}
	}
	h.t.Fatalf("exec: not an FS action: %#v", a)
	return nil
}

func ioOfAction(a Action) (IOID, bool) {
	if x, ok := a.(Actuation); ok {
		return x.ID(), true
	}
	return 0, false
}

// describe renders actions compactly (temp names as "tmp/*") for asserting
// exact sequences.
func describe(acts []Action) string {
	var out []string
	for _, a := range acts {
		var s string
		switch a := a.(type) {
		case FSWrite:
			s = "write " + tmpGlob(a.Path)
		case FSFsync:
			s = "fsync " + tmpGlob(a.Path)
		case FSRename:
			s = "rename " + tmpGlob(a.From) + " " + a.To
		case FSFsyncDir:
			s = "fsyncdir " + a.Dir
		case FSRead:
			s = "read " + a.Path
		case FSUnlink:
			s = "unlink " + tmpGlob(a.Path)
		case FSList:
			s = "list " + a.Dir
		case Halt:
			s = "halt"
		case Emit:
			switch f := a.Fact.(type) {
			case Committed:
				s = fmt.Sprintf("committed c%d %s del=%v", f.Client, f.Key, f.Deleted)
			case Recovered:
				s = fmt.Sprintf("recovered %d", f.TempsRemoved)
			case CorruptObject:
				s = "corrupt " + f.Key
			}
		case Reply:
			s = fmt.Sprintf("reply c%d %d", a.Client, a.Status)
			if a.Body != nil {
				s += " " + string(a.Body)
			}
		}
		out = append(out, s)
	}
	return strings.Join(out, "; ")
}

func tmpGlob(p string) string {
	if strings.HasPrefix(p, root+"/tmp/") {
		return "tmp/*"
	}
	return p
}

func expect(t *testing.T, what string, acts []Action, want string) {
	t.Helper()
	if got := describe(acts); got != want {
		t.Fatalf("%s:\n got  %s\n want %s", what, got, want)
	}
}

func TestHappyPath(t *testing.T) {
	h := newHarness(t)
	h.boot()

	expect(t, "PUT", h.do(1, Put{Key: "b/k", Data: []byte("hello")}),
		"write tmp/*; fsync tmp/*; rename tmp/* /d/obj/b%2Fk; fsyncdir /d/obj; committed c1 b/k del=false; reply c1 200")
	expect(t, "GET", h.do(2, Get{Key: "b/k"}),
		"read /d/obj/b%2Fk; reply c2 200 hello")
	expect(t, "COPY", h.do(3, Copy{Src: "b/k", Dst: "b/k2"}),
		"read /d/obj/b%2Fk; write tmp/*; fsync tmp/*; rename tmp/* /d/obj/b%2Fk2; fsyncdir /d/obj; committed c3 b/k2 del=false; reply c3 200")
	expect(t, "GET copy", h.do(4, Get{Key: "b/k2"}), "read /d/obj/b%2Fk2; reply c4 200 hello")
	expect(t, "DELETE", h.do(5, Delete{Key: "b/k"}),
		"unlink /d/obj/b%2Fk; fsyncdir /d/obj; committed c5 b/k del=true; reply c5 200")
	expect(t, "GET deleted", h.do(6, Get{Key: "b/k"}), "read /d/obj/b%2Fk; reply c6 404")
}

func TestCommittedSum(t *testing.T) {
	h := newHarness(t)
	h.boot()
	for _, a := range h.do(1, Put{Key: "k", Data: []byte("abc")}) {
		if e, ok := a.(Emit); ok {
			if c := e.Fact.(Committed); c.Sum != format.Checksum([]byte("abc")) {
				t.Fatalf("Committed.Sum = %x", c.Sum)
			}
			return
		}
	}
	t.Fatal("no Committed")
}

func TestNotFound(t *testing.T) {
	h := newHarness(t)
	h.boot()
	expect(t, "GET", h.do(1, Get{Key: "x"}), "read /d/obj/x; reply c1 404")
	expect(t, "DELETE", h.do(2, Delete{Key: "x"}), "unlink /d/obj/x; reply c2 404")
	expect(t, "COPY", h.do(3, Copy{Src: "x", Dst: "y"}), "read /d/obj/x; reply c3 404")
	if _, ok := h.files[format.ObjPath(root, "y")]; ok {
		t.Fatal("COPY of absent src created dst")
	}
}

func TestBadRequest(t *testing.T) {
	h := newHarness(t)
	h.boot()
	expect(t, "empty key", h.step(Request{Client: 1, Op: Get{Key: ""}}), "reply c1 400")
	expect(t, "long key", h.step(Request{Client: 2, Op: Put{Key: strings.Repeat("/", 100)}}), "reply c2 400")
	expect(t, "oversize", h.step(Request{Client: 3, Op: Put{Key: "k", Data: make([]byte, 65)}}), "reply c3 400")
	expect(t, "copy bad dst", h.step(Request{Client: 4, Op: Copy{Src: "a", Dst: ""}}), "reply c4 400")
	expect(t, "nil op", h.step(Request{Client: 5}), "reply c5 400")
}

func TestSameKeySerialized(t *testing.T) {
	h := newHarness(t)
	h.boot()
	expect(t, "PUT 1", h.step(Request{Client: 1, Op: Put{Key: "k", Data: []byte("one")}}), "write tmp/*")
	expect(t, "PUT 2 queued", h.step(Request{Client: 2, Op: Put{Key: "k", Data: []byte("two")}}), "")
	expect(t, "GET queued", h.step(Request{Client: 3, Op: Get{Key: "k"}}), "")
	expect(t, "other key runs", h.step(Request{Client: 4, Op: Get{Key: "other"}}), "read /d/obj/other")

	expect(t, "1 write done", h.completeNext(), "fsync tmp/*")
	expect(t, "other done", h.completeNext(), "reply c4 404")
	expect(t, "1 fsync done", h.completeNext(), "rename tmp/* /d/obj/k")
	expect(t, "1 rename done", h.completeNext(), "fsyncdir /d/obj")
	// PUT 2's write is issued only in the step that replies to PUT 1, after it.
	expect(t, "1 fsyncdir done", h.completeNext(), "committed c1 k del=false; reply c1 200; write tmp/*")
	expect(t, "rest", h.drain(),
		"fsync tmp/*; rename tmp/* /d/obj/k; fsyncdir /d/obj; committed c2 k del=false; reply c2 200; read /d/obj/k; reply c3 200 two")
}

func TestDifferentKeysConcurrent(t *testing.T) {
	h := newHarness(t)
	h.boot()
	expect(t, "a", h.step(Request{Client: 1, Op: Put{Key: "a", Data: []byte("1")}}), "write tmp/*")
	expect(t, "b", h.step(Request{Client: 2, Op: Put{Key: "b", Data: []byte("2")}}), "write tmp/*")
}

func TestCopyHoldsBothKeys(t *testing.T) {
	h := newHarness(t)
	h.boot()
	h.do(1, Put{Key: "a", Data: []byte("v")})
	expect(t, "copy", h.step(Request{Client: 2, Op: Copy{Src: "a", Dst: "b"}}), "read /d/obj/a")
	expect(t, "put src queued", h.step(Request{Client: 3, Op: Put{Key: "a", Data: []byte("w")}}), "")
	expect(t, "get dst queued", h.step(Request{Client: 4, Op: Get{Key: "b"}}), "")
	got := describe(h.drain())
	want := "write tmp/*; fsync tmp/*; rename tmp/* /d/obj/b; fsyncdir /d/obj; committed c2 b del=false; reply c2 200; write tmp/*; read /d/obj/b; " +
		"fsync tmp/*; reply c4 200 v; rename tmp/* /d/obj/a; fsyncdir /d/obj; committed c3 a del=false; reply c3 200"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestCopySameKey(t *testing.T) {
	h := newHarness(t)
	h.boot()
	h.do(1, Put{Key: "a", Data: []byte("v")})
	expect(t, "copy a→a", h.do(2, Copy{Src: "a", Dst: "a"}),
		"read /d/obj/a; write tmp/*; fsync tmp/*; rename tmp/* /d/obj/a; fsyncdir /d/obj; committed c2 a del=false; reply c2 200")
	expect(t, "get", h.do(3, Get{Key: "a"}), "read /d/obj/a; reply c3 200 v")
}

func TestRecovery(t *testing.T) {
	h := newHarness(t)
	h.files[root+"/tmp/0000000000000007"] = []byte("junk")
	h.files[root+"/tmp/0000000000000009"] = []byte("junk")
	h.files[format.ObjPath(root, "k")] = format.EncodeObject([]byte("kept"))

	expect(t, "Started", h.step(Started{}), "list /d/tmp")
	expect(t, "request during recovery", h.step(Request{Client: 1, Op: Get{Key: "k"}}), "")
	expect(t, "second request", h.step(Request{Client: 2, Op: Put{Key: "j", Data: []byte("x")}}), "")
	expect(t, "list done", h.completeNext(), "unlink tmp/*; unlink tmp/*")
	expect(t, "unlink 1", h.completeNext(), "")
	expect(t, "unlink 2", h.completeNext(), "fsyncdir /d/tmp")
	expect(t, "fsync tmp", h.completeNext(), "recovered 2; read /d/obj/k; write tmp/*")
	h.drain()
	for p := range h.files {
		if p == root+"/tmp/0000000000000007" || p == root+"/tmp/0000000000000009" {
			t.Fatalf("leftover %s survived recovery", p)
		}
	}
}

func TestRecoveryEmptyAndErrors(t *testing.T) {
	h := newHarness(t)
	h.step(Started{})
	expect(t, "empty list", h.completeNext(), "fsyncdir /d/tmp")
	expect(t, "done", h.completeNext(), "recovered 0")

	h = newHarness(t)
	h.fail = func(a Action) ErrCode {
		if _, ok := a.(FSList); ok {
			return ErrIO
		}
		return OK
	}
	h.step(Started{})
	expect(t, "list error", h.completeNext(), "halt")
}

func TestCorruptObject(t *testing.T) {
	h := newHarness(t)
	h.boot()
	bad := format.EncodeObject([]byte("payload"))
	bad[len(bad)-1] ^= 1
	h.files[format.ObjPath(root, "k")] = bad
	expect(t, "GET", h.do(1, Get{Key: "k"}), "read /d/obj/k; corrupt k; reply c1 500")
	expect(t, "COPY", h.do(2, Copy{Src: "k", Dst: "d"}), "read /d/obj/k; corrupt k; reply c2 500")
	h.files[format.ObjPath(root, "t")] = []byte("TOY") // torn header
	expect(t, "torn", h.do(3, Get{Key: "t"}), "read /d/obj/t; corrupt t; reply c3 500")
}

// failKind makes every action of type T fail with ErrIO.
func failKind[T Action]() func(Action) ErrCode {
	return func(a Action) ErrCode {
		if _, ok := a.(T); ok {
			return ErrIO
		}
		return OK
	}
}

func TestErrorPolicy(t *testing.T) {
	cases := []struct {
		name string
		fail func(Action) ErrCode
		op   Op
		want string
	}{
		{"write tmp: plain 500", failKind[FSWrite](), Put{Key: "k", Data: []byte("v")},
			"write tmp/*; reply c1 500"},
		{"fsync tmp: halt", failKind[FSFsync](), Put{Key: "k", Data: []byte("v")},
			"write tmp/*; fsync tmp/*; reply c1 500; halt"},
		{"rename: halt", failKind[FSRename](), Put{Key: "k", Data: []byte("v")},
			"write tmp/*; fsync tmp/*; rename tmp/* /d/obj/k; reply c1 500; halt"},
		{"fsyncdir: halt", failKind[FSFsyncDir](), Put{Key: "k", Data: []byte("v")},
			"write tmp/*; fsync tmp/*; rename tmp/* /d/obj/k; fsyncdir /d/obj; reply c1 500; halt"},
		{"read: plain 500", failKind[FSRead](), Get{Key: "k"},
			"read /d/obj/k; reply c1 500"},
		{"unlink: halt", failKind[FSUnlink](), Delete{Key: "k"},
			"unlink /d/obj/k; reply c1 500; halt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.boot()
			h.files[format.ObjPath(root, "k")] = format.EncodeObject([]byte("old"))
			h.fail = tc.fail
			expect(t, "op", h.do(1, tc.op), tc.want)
			if strings.HasSuffix(tc.want, "halt") {
				// Halted: everything afterwards is ignored.
				h.fail = nil
				expect(t, "after halt", h.step(Request{Client: 2, Op: Get{Key: "k"}}), "")
			} else {
				h.fail = nil
				expect(t, "still serving", h.do(2, Get{Key: "k"}), "read /d/obj/k; reply c2 200 old")
			}
		})
	}
}

func TestStaleIOIgnored(t *testing.T) {
	h := newHarness(t)
	h.boot()
	h.step(Request{Client: 1, Op: Put{Key: "k", Data: []byte("v")}})
	before := h.s.StateHash()
	writeIO, _ := ioOfAction(h.pending[0])
	for _, b := range []EventBody{
		FSWriteDone{IO: 9999},                // unknown IOID
		FSFsyncDone{IO: writeIO},             // right IOID, wrong completion kind
		FSListDone{IO: 1},                    // recovery's IOID, long finished
		FSFsyncDirDone{IO: 2, Err: ErrIO},    // recovery's fsync, long finished
		FSReadDone{IO: writeIO + 1, Err: OK}, // not yet issued
	} {
		if acts := h.step(b); len(acts) != 0 {
			t.Fatalf("%#v: got actions %s", b, describe(acts))
		}
	}
	if h.s.StateHash() != before {
		t.Fatal("stale completion changed state")
	}
	expect(t, "real completion", h.drain(), "fsync tmp/*; rename tmp/* /d/obj/k; fsyncdir /d/obj; committed c1 k del=false; reply c1 200")
	expect(t, "second Started ignored", h.step(Started{}), "")
}

func TestStateHash(t *testing.T) {
	run := func() []uint64 {
		h := newHarness(t)
		var hs []uint64
		hs = append(hs, h.s.StateHash())
		h.boot()
		hs = append(hs, h.s.StateHash())
		h.step(Request{Client: 1, Op: Put{Key: "k", Data: []byte("v")}})
		hs = append(hs, h.s.StateHash())
		h.step(Request{Client: 2, Op: Get{Key: "k"}})
		hs = append(hs, h.s.StateHash())
		for len(h.pending) > 0 {
			h.completeNext()
			hs = append(hs, h.s.StateHash())
		}
		return hs
	}
	a, b := run(), run()
	if !slices.Equal(a, b) {
		t.Fatalf("equal histories, different hashes:\n%x\n%x", a, b)
	}
	for i := 1; i < len(a); i++ {
		if a[i] == a[i-1] {
			t.Fatalf("hash did not change at step %d", i)
		}
	}

	// Same shape, different payload: different hash.
	h1, h2 := newHarness(t), newHarness(t)
	h1.boot()
	h2.boot()
	h1.step(Request{Client: 1, Op: Put{Key: "k", Data: []byte("v")}})
	h2.step(Request{Client: 1, Op: Put{Key: "k", Data: []byte("w")}})
	if h1.s.StateHash() == h2.s.StateHash() {
		t.Fatal("payload not covered by StateHash")
	}
}
