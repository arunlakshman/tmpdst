package veritas

import (
	"slices"
	"strings"

	"manifester/toys3/control"
)

// FS is the veritas of the Storage port (control/storage.go): a filesystem as
// a map from path to content. It is sequential, never fails and never
// crashes, so every change is immediately durable and FSFsync and FSFsyncDir
// only check that their target exists. What it pins down is everything else
// a filesystem promises on the happy path:
//
//	FSWrite     creates Path, or truncates it, and writes Data
//	FSFsync     OK; ENOENT if Path does not exist
//	FSRename    replaces To with From atomically; From == To is a no-op;
//	            ENOENT if From does not exist
//	FSFsyncDir  OK
//	FSRead      the whole content; ENOENT if Path does not exist
//	FSUnlink    removes Path; ENOENT if it does not exist
//	FSList      base names of the files in Dir, sorted ascending
//
// Directories are fixed at construction, as in the real data root (there is
// no mkdir): an operation on a path whose directory does not exist, or an
// FSFsyncDir or FSList of such a directory, is ENOENT.
//
// FS never aliases a caller's slice: it copies Data on the way in and on the
// way out.
type FS struct {
	dirs  map[string]bool
	files map[string][]byte // full path → content
}

// NewFS returns an empty filesystem with the given directories.
func NewFS(dirs ...string) *FS {
	fs := &FS{dirs: map[string]bool{}, files: map[string][]byte{}}
	for _, d := range dirs {
		fs.dirs[d] = true
	}
	return fs
}

// Exec runs one storage command and returns its completion. It panics on an
// Actuation it does not know: a new storage command must be added here first.
func (fs *FS) Exec(a control.Actuation) control.Feedback {
	switch a := a.(type) {
	case control.FSWrite:
		if !fs.dirOf(a.Path) {
			return control.FSWriteDone{IO: a.IO, Err: control.ErrNotFound}
		}
		fs.files[a.Path] = clone(a.Data)
		return control.FSWriteDone{IO: a.IO}
	case control.FSFsync:
		return control.FSFsyncDone{IO: a.IO, Err: fs.exists(a.Path)}
	case control.FSRename:
		data, ok := fs.files[a.From]
		if !ok || !fs.dirOf(a.To) {
			return control.FSRenameDone{IO: a.IO, Err: control.ErrNotFound}
		}
		delete(fs.files, a.From)
		fs.files[a.To] = data
		return control.FSRenameDone{IO: a.IO}
	case control.FSFsyncDir:
		if !fs.dirs[a.Dir] {
			return control.FSFsyncDirDone{IO: a.IO, Err: control.ErrNotFound}
		}
		return control.FSFsyncDirDone{IO: a.IO}
	case control.FSRead:
		data, ok := fs.files[a.Path]
		if !ok {
			return control.FSReadDone{IO: a.IO, Err: control.ErrNotFound}
		}
		return control.FSReadDone{IO: a.IO, Data: clone(data)}
	case control.FSUnlink:
		if _, ok := fs.files[a.Path]; !ok {
			return control.FSUnlinkDone{IO: a.IO, Err: control.ErrNotFound}
		}
		delete(fs.files, a.Path)
		return control.FSUnlinkDone{IO: a.IO}
	case control.FSList:
		if !fs.dirs[a.Dir] {
			return control.FSListDone{IO: a.IO, Err: control.ErrNotFound}
		}
		var names []string
		for _, p := range fs.Paths() {
			if dir, name := split(p); dir == a.Dir {
				names = append(names, name)
			}
		}
		return control.FSListDone{IO: a.IO, Names: names}
	}
	panic("toys3/veritas: FS.Exec: unknown storage command")
}

// ReadFile returns a copy of path's content.
func (fs *FS) ReadFile(path string) ([]byte, bool) {
	data, ok := fs.files[path]
	if !ok {
		return nil, false
	}
	return clone(data), true
}

// WriteFile sets path's content directly, for test setup (planting junk or a
// corrupt object). It panics if path's directory does not exist.
func (fs *FS) WriteFile(path string, data []byte) {
	if !fs.dirOf(path) {
		panic("toys3/veritas: FS.WriteFile: no such directory: " + path)
	}
	fs.files[path] = clone(data)
}

// Paths returns every file's path, sorted.
func (fs *FS) Paths() []string {
	paths := make([]string, 0, len(fs.files))
	for p := range fs.files { //detcheck:ok keys are sorted before use
		paths = append(paths, p)
	}
	slices.Sort(paths)
	return paths
}

// Dirs returns the directories, sorted.
func (fs *FS) Dirs() []string {
	dirs := make([]string, 0, len(fs.dirs))
	for d := range fs.dirs { //detcheck:ok keys are sorted before use
		dirs = append(dirs, d)
	}
	slices.Sort(dirs)
	return dirs
}

func (fs *FS) exists(path string) control.ErrCode {
	if _, ok := fs.files[path]; !ok {
		return control.ErrNotFound
	}
	return control.OK
}

// dirOf reports whether path names an entry in an existing directory.
func (fs *FS) dirOf(path string) bool {
	dir, name := split(path)
	return name != "" && fs.dirs[dir]
}

// split returns the directory of path and the base name in it.
func split(path string) (dir, name string) {
	i := strings.LastIndexByte(path, '/')
	if i < 0 {
		return "", ""
	}
	return path[:i], path[i+1:]
}

// clone copies b, keeping an empty file distinct from nil (no file).
func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
