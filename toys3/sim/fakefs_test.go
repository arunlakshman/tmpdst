package sim

import (
	"bytes"
	"testing"

	"manifester/toys3/core"
)

const (
	tObj = "/d/obj"
	tTmp = "/d/tmp"
)

func newTestFS(seed uint64, f FSFaults) *FakeFS { return NewFakeFS(NewRand(seed), f, tObj, tTmp) }

// do lands a on fs with no injected failure and returns its error.
func do(t *testing.T, fs *FakeFS, a core.Action) core.ErrCode {
	t.Helper()
	return errOf(fs.Land(a, FSPlan{}))
}

func errOf(b core.EventBody) core.ErrCode {
	switch e := b.(type) {
	case core.FSWriteDone:
		return e.Err
	case core.FSFsyncDone:
		return e.Err
	case core.FSRenameDone:
		return e.Err
	case core.FSFsyncDirDone:
		return e.Err
	case core.FSReadDone:
		return e.Err
	case core.FSUnlinkDone:
		return e.Err
	case core.FSListDone:
		return e.Err
	}
	return 255
}

func TestFakeFSUnsyncedWriteMayVanish(t *testing.T) {
	data := []byte("hello world")
	survived, vanished := 0, 0
	for seed := uint64(1); seed <= 64; seed++ {
		fs := newTestFS(seed, FSFaults{})
		do(t, fs, core.FSWrite{IO: 1, Path: tObj + "/a", Data: data})
		if got, ok := fs.CurrentFile(tObj + "/a"); !ok || !bytes.Equal(got, data) {
			t.Fatal("write not visible before the crash")
		}
		if _, ok := fs.DurableFile(tObj + "/a"); ok {
			t.Fatal("un-synced create is in the durable view")
		}
		fs.Crash()
		if got, ok := fs.CurrentFile(tObj + "/a"); ok && bytes.Equal(got, data) {
			survived++
		} else {
			vanished++
		}
	}
	if survived == 0 || vanished == 0 {
		t.Fatalf("adversary not adversarial: survived=%d vanished=%d", survived, vanished)
	}
}

func TestFakeFSRenameWithoutDirFsyncMayVanish(t *testing.T) {
	data := []byte("payload")
	present, absent, both := 0, 0, 0
	for seed := uint64(1); seed <= 128; seed++ {
		fs := newTestFS(seed, FSFaults{})
		do(t, fs, core.FSWrite{IO: 1, Path: tTmp + "/t", Data: data})
		do(t, fs, core.FSFsync{IO: 2, Path: tTmp + "/t"})
		do(t, fs, core.FSFsyncDir{IO: 3, Dir: tTmp})
		do(t, fs, core.FSRename{IO: 4, From: tTmp + "/t", To: tObj + "/k"})
		fs.Crash()
		_, objOK := fs.CurrentFile(tObj + "/k")
		_, tmpOK := fs.CurrentFile(tTmp + "/t")
		switch {
		case objOK && tmpOK:
			both++
		case objOK:
			present++
		default:
			absent++
		}
	}
	if present == 0 || absent == 0 || both == 0 {
		t.Fatalf("want every outcome: present=%d absent=%d both-names=%d", present, absent, both)
	}
}

func TestFakeFSFullySyncedSurvives(t *testing.T) {
	data := []byte("durable bytes")
	for seed := uint64(1); seed <= 100; seed++ {
		fs := newTestFS(seed, FSFaults{TornWriteProb: 1})
		do(t, fs, core.FSWrite{IO: 1, Path: tTmp + "/t", Data: data})
		do(t, fs, core.FSFsync{IO: 2, Path: tTmp + "/t"})
		do(t, fs, core.FSRename{IO: 3, From: tTmp + "/t", To: tObj + "/k"})
		do(t, fs, core.FSFsyncDir{IO: 4, Dir: tObj})
		do(t, fs, core.FSFsyncDir{IO: 5, Dir: tTmp})
		if got, ok := fs.DurableFile(tObj + "/k"); !ok || !bytes.Equal(got, data) {
			t.Fatal("durable view lacks the synced file")
		}
		fs.Crash()
		if got, ok := fs.CurrentFile(tObj + "/k"); !ok || !bytes.Equal(got, data) {
			t.Fatalf("seed %d: fully synced file lost or changed: %q %v", seed, got, ok)
		}
		if _, ok := fs.CurrentFile(tTmp + "/t"); ok {
			t.Fatalf("seed %d: durable unlink of temp name undone", seed)
		}
	}
}

func TestFakeFSTornPrefix(t *testing.T) {
	data := []byte("0123456789abcdef")
	for seed := uint64(1); seed <= 50; seed++ {
		fs := newTestFS(seed, FSFaults{TornWriteProb: 1})
		do(t, fs, core.FSWrite{IO: 1, Path: tObj + "/k", Data: data})
		do(t, fs, core.FSFsyncDir{IO: 2, Dir: tObj}) // the name is durable, the bytes are not
		rep := fs.Crash()
		got, ok := fs.CurrentFile(tObj + "/k")
		if !ok || rep.Torn != 1 {
			t.Fatalf("seed %d: ok=%v report=%+v", seed, ok, rep)
		}
		if len(got) >= len(data) || !bytes.HasPrefix(data, got) {
			t.Fatalf("seed %d: %q is not a strict prefix of %q", seed, got, data)
		}
	}
}

func TestFakeFSEIOMayOrMayNotApply(t *testing.T) {
	applied, notApplied := 0, 0
	for seed := uint64(1); seed <= 64; seed++ {
		fs := newTestFS(seed, FSFaults{})
		do(t, fs, core.FSWrite{IO: 1, Path: tTmp + "/t", Data: []byte("x")})
		if err := errOf(fs.Land(core.FSRename{IO: 2, From: tTmp + "/t", To: tObj + "/k"}, FSPlan{Fail: true})); err != core.ErrIO {
			t.Fatalf("got %v, want EIO", err)
		}
		if _, ok := fs.CurrentFile(tObj + "/k"); ok {
			applied++
		} else {
			notApplied++
		}
	}
	if applied == 0 || notApplied == 0 {
		t.Fatalf("failed rename: applied=%d not=%d", applied, notApplied)
	}
	fs := newTestFS(1, FSFaults{})
	if err := errOf(fs.Land(core.FSRead{IO: 1, Path: tObj + "/missing"}, FSPlan{Fail: true})); err != core.ErrNotFound {
		t.Fatalf("missing path with a planned failure: got %v, want ENOENT", err)
	}
}

func TestFakeFSFsyncGateLies(t *testing.T) {
	fs := newTestFS(1, FSFaults{FsyncGate: true})
	do(t, fs, core.FSWrite{IO: 1, Path: tObj + "/k", Data: []byte("data")})
	do(t, fs, core.FSFsyncDir{IO: 2, Dir: tObj})
	if err := errOf(fs.Land(core.FSFsync{IO: 3, Path: tObj + "/k"}, FSPlan{Fail: true})); err != core.ErrIO {
		t.Fatal("want EIO")
	}
	if err := do(t, fs, core.FSFsync{IO: 4, Path: tObj + "/k"}); err != core.OK {
		t.Fatal("the retry should claim success")
	}
	fs.Crash()
	if got, _ := fs.CurrentFile(tObj + "/k"); len(got) != 0 {
		t.Fatalf("fsyncgate: data reached disk anyway: %q", got)
	}
}

func TestFakeFSNeverAliasesHandedOutSlices(t *testing.T) {
	fs := newTestFS(1, FSFaults{})
	in := []byte("abc")
	do(t, fs, core.FSWrite{IO: 1, Path: tObj + "/k", Data: in})
	in[0] = 'X'
	r := fs.Land(core.FSRead{IO: 2, Path: tObj + "/k"}, FSPlan{}).(core.FSReadDone)
	if string(r.Data) != "abc" {
		t.Fatalf("write did not copy: %q", r.Data)
	}
	r.Data[1] = 'Y'
	r2 := fs.Land(core.FSRead{IO: 3, Path: tObj + "/k"}, FSPlan{}).(core.FSReadDone)
	if string(r2.Data) != "abc" {
		t.Fatalf("read did not copy: %q", r2.Data)
	}
}
