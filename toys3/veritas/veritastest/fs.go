package veritastest

import (
	"bytes"
	"fmt"
	"slices"

	"manifester/toys3/control"
	"manifester/toys3/veritas"
)

// FSUnderTest is an implementation of the Storage port driven synchronously:
// Exec runs one storage command to completion and returns its feedback.
type FSUnderTest interface {
	Exec(control.Actuation) control.Feedback
}

// The FS alphabet. Paths live under FSRoot, in its obj and tmp directories
// (format.ObjDir, format.TmpDir); an implementation rooted elsewhere (a real
// temp dir) rewrites the prefix. There are few paths, so operations collide:
// renames replace existing files, writes truncate, unlinks and reads hit
// files that exist about as often as files that don't.
const FSRoot = "/d"

// FSDirs returns the alphabet's directories, simplest first.
func FSDirs() []string { return []string{FSRoot + "/obj", FSRoot + "/tmp"} }

// FSPaths returns the alphabet's file paths, simplest first.
func FSPaths() []string {
	return []string{FSRoot + "/obj/a", FSRoot + "/obj/b", FSRoot + "/obj/c", FSRoot + "/tmp/1", FSRoot + "/tmp/2"}
}

// Pattern returns n bytes counting up from start through 'a'..'z' (wrapping),
// so a prefix, a truncation or a shifted copy reads differently from the
// whole. It returns nil for n == 0.
func Pattern(n int, start byte) []byte {
	if n == 0 {
		return nil
	}
	b := make([]byte, n)
	c := int(start-'a') % 26
	for i := range b {
		b[i] = 'a' + byte((c+i)%26)
	}
	return b
}

// genFS returns a random storage command. Kinds are listed simplest first;
// writes are the most common because they create what the others act on.
// Sizes cluster where bugs do: empty, small, and either side of a 4 KiB page.
func genFS(r *Rand, i int) control.Actuation {
	io := control.IOID(i + 1)
	paths, dirs := FSPaths(), FSDirs()
	path := func() string { return paths[r.Intn(len(paths))] }
	switch k := r.Intn(100); {
	case k < 15:
		return control.FSRead{IO: io, Path: path()}
	case k < 25:
		return control.FSList{IO: io, Dir: dirs[r.Intn(len(dirs))]}
	case k < 55:
		var n int
		switch s := r.Intn(4); s {
		case 0:
			n = 0
		case 1, 2:
			n = 1 + r.Intn(16)
		default:
			n = 4095 + r.Intn(3)
		}
		return control.FSWrite{IO: io, Path: path(), Data: Pattern(n, 'a'+byte(r.Intn(26)))}
	case k < 65:
		return control.FSFsync{IO: io, Path: path()}
	case k < 75:
		return control.FSFsyncDir{IO: io, Dir: dirs[r.Intn(len(dirs))]}
	case k < 85:
		return control.FSUnlink{IO: io, Path: path()}
	default:
		return control.FSRename{IO: io, From: path(), To: path()}
	}
}

// shrinkFS returns simpler versions of a: shorter data, then earlier paths.
func shrinkFS(a control.Actuation) []control.Actuation {
	var out []control.Actuation
	switch a := a.(type) {
	case control.FSRead:
		for _, p := range earlier(FSPaths(), a.Path) {
			out = append(out, control.FSRead{IO: a.IO, Path: p})
		}
	case control.FSList:
		for _, d := range earlier(FSDirs(), a.Dir) {
			out = append(out, control.FSList{IO: a.IO, Dir: d})
		}
	case control.FSWrite:
		start := byte('a')
		if len(a.Data) > 0 {
			start = a.Data[0]
		}
		for _, n := range shorter(len(a.Data)) {
			out = append(out, control.FSWrite{IO: a.IO, Path: a.Path, Data: Pattern(n, start)})
		}
		if start != 'a' && bytes.Equal(a.Data, Pattern(len(a.Data), start)) {
			out = append(out, control.FSWrite{IO: a.IO, Path: a.Path, Data: Pattern(len(a.Data), 'a')})
		}
		for _, p := range earlier(FSPaths(), a.Path) {
			out = append(out, control.FSWrite{IO: a.IO, Path: p, Data: a.Data})
		}
	case control.FSFsync:
		for _, p := range earlier(FSPaths(), a.Path) {
			out = append(out, control.FSFsync{IO: a.IO, Path: p})
		}
	case control.FSFsyncDir:
		for _, d := range earlier(FSDirs(), a.Dir) {
			out = append(out, control.FSFsyncDir{IO: a.IO, Dir: d})
		}
	case control.FSUnlink:
		for _, p := range earlier(FSPaths(), a.Path) {
			out = append(out, control.FSUnlink{IO: a.IO, Path: p})
		}
	case control.FSRename:
		for _, p := range earlier(FSPaths(), a.From) {
			out = append(out, control.FSRename{IO: a.IO, From: p, To: a.To})
		}
		for _, p := range earlier(FSPaths(), a.To) {
			out = append(out, control.FSRename{IO: a.IO, From: a.From, To: p})
		}
	}
	return out
}

// shrinkFSAll returns ops with one path (or directory) renamed to an earlier
// one everywhere, latest path first.
func shrinkFSAll(ops []control.Actuation) [][]control.Actuation {
	var out [][]control.Actuation
	for _, list := range [][]string{FSPaths(), FSDirs()} {
		for i := len(list) - 1; i > 0; i-- {
			for _, to := range list[:i] {
				if cand, changed := renameAll(ops, list[i], to); changed {
					out = append(out, cand)
				}
			}
		}
	}
	return out
}

// renameAll returns ops with every use of path from replaced by to.
func renameAll(ops []control.Actuation, from, to string) ([]control.Actuation, bool) {
	changed := false
	sub := func(p string) string {
		if p == from {
			changed = true
			return to
		}
		return p
	}
	out := make([]control.Actuation, len(ops))
	for i, a := range ops {
		switch a := a.(type) {
		case control.FSWrite:
			a.Path = sub(a.Path)
			out[i] = a
		case control.FSFsync:
			a.Path = sub(a.Path)
			out[i] = a
		case control.FSRename:
			a.From, a.To = sub(a.From), sub(a.To)
			out[i] = a
		case control.FSFsyncDir:
			a.Dir = sub(a.Dir)
			out[i] = a
		case control.FSRead:
			a.Path = sub(a.Path)
			out[i] = a
		case control.FSUnlink:
			a.Path = sub(a.Path)
			out[i] = a
		case control.FSList:
			a.Dir = sub(a.Dir)
			out[i] = a
		default:
			out[i] = a
		}
	}
	return out, changed
}

// earlier returns the elements of list before x.
func earlier(list []string, x string) []string {
	if i := slices.Index(list, x); i > 0 {
		return list[:i]
	}
	return nil
}

// shorter returns smaller lengths than n, smallest first: 0, n/2, n-1.
func shorter(n int) []int {
	var out []int
	for _, m := range []int{0, n / 2, n - 1} {
		if m >= 0 && m < n && !slices.Contains(out, m) {
			out = append(out, m)
		}
	}
	return out
}

// formatFS returns a as a Go expression.
func formatFS(a control.Actuation) string {
	switch a := a.(type) {
	case control.FSWrite:
		data := ""
		switch {
		case len(a.Data) == 0:
		case bytes.Equal(a.Data, Pattern(len(a.Data), a.Data[0])):
			data = fmt.Sprintf(", Data: veritastest.Pattern(%d, %q)", len(a.Data), a.Data[0])
		default:
			data = fmt.Sprintf(", Data: []byte(%q)", a.Data)
		}
		return fmt.Sprintf("control.FSWrite{IO: %d, Path: %q%s}", a.IO, a.Path, data)
	case control.FSFsync:
		return fmt.Sprintf("control.FSFsync{IO: %d, Path: %q}", a.IO, a.Path)
	case control.FSRename:
		return fmt.Sprintf("control.FSRename{IO: %d, From: %q, To: %q}", a.IO, a.From, a.To)
	case control.FSFsyncDir:
		return fmt.Sprintf("control.FSFsyncDir{IO: %d, Dir: %q}", a.IO, a.Dir)
	case control.FSRead:
		return fmt.Sprintf("control.FSRead{IO: %d, Path: %q}", a.IO, a.Path)
	case control.FSUnlink:
		return fmt.Sprintf("control.FSUnlink{IO: %d, Path: %q}", a.IO, a.Path)
	case control.FSList:
		return fmt.Sprintf("control.FSList{IO: %d, Dir: %q}", a.IO, a.Dir)
	}
	return fmt.Sprintf("%#v", a)
}

// fsPair is an implementation under test and its veritas.
type fsPair struct {
	impl FSUnderTest
	ver  *veritas.FS
}

// FSCheck is the conformance check of the Storage port: newImpl's
// implementation, rooted at FSRoot with FSDirs created and empty, against
// veritas.FS. After every command it compares the completions (kind, IO,
// ErrCode, read Data, list Names) and then, as the invariant, the whole
// visible state: every FSDirs listing and every FSPaths read.
func FSCheck(newImpl func() FSUnderTest) Check[*fsPair, control.Actuation] {
	return Check[*fsPair, control.Actuation]{
		Name: "control.Actuation",
		New:  func() *fsPair { return &fsPair{impl: newImpl(), ver: veritas.NewFS(FSDirs()...)} },
		Gen:  genFS,
		Step: func(s *fsPair, a control.Actuation) error {
			return diffFeedback(s.impl.Exec(a), s.ver.Exec(a))
		},
		Invariant: func(s *fsPair) error {
			for _, d := range FSDirs() {
				a := control.FSList{Dir: d}
				if err := diffFeedback(s.impl.Exec(a), s.ver.Exec(a)); err != nil {
					return fmt.Errorf("state of %s: %w", d, err)
				}
			}
			for _, p := range FSPaths() {
				a := control.FSRead{Path: p}
				if err := diffFeedback(s.impl.Exec(a), s.ver.Exec(a)); err != nil {
					return fmt.Errorf("state of %s: %w", p, err)
				}
			}
			return nil
		},
		Shrink:    shrinkFS,
		ShrinkAll: shrinkFSAll,
		Format:    formatFS,
		Ops:       100,
	}
}

// CheckFS runs FSCheck(newImpl) on the seeds o chooses.
func CheckFS(t TB, newImpl func() FSUnderTest, o Options) {
	t.Helper()
	Run(t, FSCheck(newImpl), o)
}

// ReplayFS runs ops (typically a pasted shrunk sequence) through FSCheck and
// reports the first difference.
func ReplayFS(t TB, newImpl func() FSUnderTest, ops []control.Actuation) {
	t.Helper()
	if i, err := FSCheck(newImpl).Replay(ops); err != nil {
		t.Errorf("operation %d (%s): %v", i+1, formatFS(ops[i]), err)
	}
}

// diffFeedback describes how got (the implementation's) differs from want
// (the veritas's), or returns nil.
func diffFeedback(got, want control.Feedback) error {
	if fmt.Sprintf("%T", got) != fmt.Sprintf("%T", want) || got.ID() != want.ID() {
		return fmt.Errorf("got %T IO %d, want %T IO %d", got, got.ID(), want, want.ID())
	}
	if g, w := errCode(got), errCode(want); g != w {
		return fmt.Errorf("got %v, want %v", g, w)
	}
	switch w := want.(type) {
	case control.FSReadDone:
		if g := got.(control.FSReadDone); !bytes.Equal(g.Data, w.Data) {
			return fmt.Errorf("read %d bytes %s, want %d bytes %s", len(g.Data), abbrev(g.Data), len(w.Data), abbrev(w.Data))
		}
	case control.FSListDone:
		if g := got.(control.FSListDone); !slices.Equal(g.Names, w.Names) {
			return fmt.Errorf("listed %q, want %q", g.Names, w.Names)
		}
	}
	return nil
}

func errCode(f control.Feedback) control.ErrCode {
	switch f := f.(type) {
	case control.FSWriteDone:
		return f.Err
	case control.FSFsyncDone:
		return f.Err
	case control.FSRenameDone:
		return f.Err
	case control.FSFsyncDirDone:
		return f.Err
	case control.FSReadDone:
		return f.Err
	case control.FSUnlinkDone:
		return f.Err
	case control.FSListDone:
		return f.Err
	}
	panic("veritastest: unknown storage completion")
}

// abbrev quotes b, eliding the middle of long contents.
func abbrev(b []byte) string {
	if len(b) <= 40 {
		return fmt.Sprintf("%q", b)
	}
	return fmt.Sprintf("%q...%q", b[:16], b[len(b)-16:])
}
