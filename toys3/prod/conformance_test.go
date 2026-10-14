package prod

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"manifester/toys3/control"
	"manifester/toys3/veritas/veritastest"
)

var (
	flagSeeds    = flag.Int("seeds", 20, "TestC3...: number of seeds to run (from 1)")
	flagSeed     = flag.Int64("seed", -1, "TestC3...: replay this one seed")
	flagDuration = flag.Duration("duration", 0, "TestC3...: run seeds 1, 2, ... until this much wall-clock time has passed (overrides -seeds)")
)

// diskFS drives OSExecutor synchronously on a real directory as a
// veritastest.FSUnderTest: it rewrites veritastest.FSRoot to root on the way
// in. Names in FSListDone are base names, so nothing needs rewriting on the
// way out.
type diskFS struct{ root string }

func (d diskFS) Exec(a control.Actuation) control.Feedback {
	switch a := a.(type) {
	case control.FSWrite:
		a.Path = d.path(a.Path)
		return d.run(a)
	case control.FSFsync:
		a.Path = d.path(a.Path)
		return d.run(a)
	case control.FSRename:
		a.From, a.To = d.path(a.From), d.path(a.To)
		return d.run(a)
	case control.FSFsyncDir:
		a.Dir = d.path(a.Dir)
		return d.run(a)
	case control.FSRead:
		a.Path = d.path(a.Path)
		return d.run(a)
	case control.FSUnlink:
		a.Path = d.path(a.Path)
		return d.run(a)
	case control.FSList:
		a.Dir = d.path(a.Dir)
		return d.run(a)
	}
	panic("toys3/prod: diskFS got a non-filesystem action")
}

// run executes a through OSExecutor and waits for its completion.
func (d diskFS) run(a control.Actuation) control.Feedback {
	ch := make(chan control.EventBody, 1)
	OSExecutor{}.Execute(a, func(e control.EventBody) { ch <- e })
	return (<-ch).(control.Feedback)
}

func (d diskFS) path(p string) string {
	rest, ok := strings.CutPrefix(p, veritastest.FSRoot)
	if !ok {
		panic("toys3/prod: diskFS: path outside FSRoot: " + p)
	}
	return d.root + rest
}

// newDiskFS returns a constructor for fresh diskFS instances, each in its own
// directory under t.TempDir() with obj/ and tmp/ prepared. Lockstep runs and
// shrink replays are sequential, so each new directory replaces the last.
func newDiskFS(t *testing.T) func() veritastest.FSUnderTest {
	base := t.TempDir()
	prev := ""
	return func() veritastest.FSUnderTest {
		if prev != "" {
			if err := os.RemoveAll(prev); err != nil {
				t.Fatal(err)
			}
		}
		dir, err := os.MkdirTemp(base, "run")
		if err != nil {
			t.Fatal(err)
		}
		if err := PrepareDir(dir); err != nil {
			t.Fatal(err)
		}
		prev = dir
		return diskFS{root: filepath.ToSlash(dir)}
	}
}

// TestC3OSExecutorMatchesVeritasFS is conformance check C3: OSExecutor on a
// real disk agrees with veritas.FS on every storage command, which ties the
// simulator's storage model (checked against veritas.FS by C2) to real POSIX
// behaviour. Flags: -seeds N, -seed S (replay one seed), -duration D.
func TestC3OSExecutorMatchesVeritasFS(t *testing.T) {
	o := veritastest.Options{
		Seeds: *flagSeeds,
		Seed:  *flagSeed,
		Replay: func(seed uint64) string {
			return fmt.Sprintf("go test ./toys3/prod -run TestC3OSExecutorMatchesVeritasFS -seed=%d -v", seed)
		},
	}
	if *flagDuration > 0 {
		start := time.Now()
		o.Stop = func() bool { return time.Since(start) >= *flagDuration }
	}
	veritastest.CheckFS(t, newDiskFS(t), o)
}

// TestC3CaseSensitive pins a POSIX assumption the generator's alphabet
// (lower-case names only) cannot reach: names that differ only in case are
// different files. veritas.FS and StorageModel assume so, and format.EscapeKey
// relies on it, passing ASCII letters through unchanged, so keys "K" and "k"
// become obj/K and obj/k. On a case-insensitive filesystem (the macOS APFS
// default; Linux ext4 and xfs are case-sensitive) those are one file, and the
// two keys alias each other. That is a property of the volume, not of
// OSExecutor, so the test skips there rather than teach the veritas to fold
// case.
func TestC3CaseSensitive(t *testing.T) {
	newImpl := newDiskFS(t)
	probe := newImpl().(diskFS)
	if err := os.WriteFile(probe.root+"/obj/x", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(probe.root + "/obj/X"); err == nil {
		t.Skip("filesystem folds case: keys that differ only in case alias each other on this volume")
	}
	veritastest.ReplayFS(t, newImpl, []control.Actuation{
		control.FSWrite{IO: 1, Path: "/d/obj/a", Data: veritastest.Pattern(1, 'a')},
		control.FSWrite{IO: 2, Path: "/d/obj/A", Data: veritastest.Pattern(1, 'b')},
		control.FSRead{IO: 3, Path: "/d/obj/a"},
		control.FSList{IO: 4, Dir: "/d/obj"},
	})
}
