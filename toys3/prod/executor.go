package prod

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"manifester/toys3/core"
	"manifester/toys3/format"
)

// Executor performs filesystem actions. Execute must not block the loop: it
// starts the operation and, when it finishes, calls done exactly once with
// the matching completion (core.FSWriteDone for core.FSWrite, and so on),
// from any goroutine.
type Executor interface {
	Execute(a core.Action, done func(core.EventBody))
}

// OSExecutor runs each action in its own goroutine with real os calls:
//
//	FSWrite     os.OpenFile(O_WRONLY|O_CREATE|O_TRUNC, 0o644) + Write + Close
//	FSFsync     os.Open + File.Sync + Close
//	FSRename    os.Rename
//	FSFsyncDir  os.Open(dir) + File.Sync + Close
//	FSRead      os.ReadFile
//	FSUnlink    os.Remove
//	FSList      os.ReadDir, names sorted
//
// Errors map to core.ErrCode: errors.Is(err, fs.ErrNotExist) → ErrNotFound,
// anything else → ErrIO.
type OSExecutor struct{}

// Execute implements Executor.
func (OSExecutor) Execute(a core.Action, done func(core.EventBody)) {
	go func() { done(run(a)) }()
}

// run performs a synchronously and returns its completion.
func run(a core.Action) core.EventBody {
	switch a := a.(type) {
	case core.FSWrite:
		return core.FSWriteDone{IO: a.IO, Err: code(writeFile(a.Path, a.Data))}
	case core.FSFsync:
		return core.FSFsyncDone{IO: a.IO, Err: code(syncPath(a.Path))}
	case core.FSRename:
		return core.FSRenameDone{IO: a.IO, Err: code(os.Rename(osPath(a.From), osPath(a.To)))}
	case core.FSFsyncDir:
		return core.FSFsyncDirDone{IO: a.IO, Err: code(syncPath(a.Dir))}
	case core.FSRead:
		data, err := os.ReadFile(osPath(a.Path))
		if err != nil {
			return core.FSReadDone{IO: a.IO, Err: code(err)}
		}
		return core.FSReadDone{IO: a.IO, Data: data}
	case core.FSUnlink:
		return core.FSUnlinkDone{IO: a.IO, Err: code(os.Remove(osPath(a.Path)))}
	case core.FSList:
		entries, err := os.ReadDir(osPath(a.Dir)) // sorted by name
		if err != nil {
			return core.FSListDone{IO: a.IO, Err: code(err)}
		}
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		return core.FSListDone{IO: a.IO, Names: names}
	}
	panic("toys3/prod: OSExecutor got a non-filesystem action")
}

func osPath(p string) string { return filepath.FromSlash(p) }

func writeFile(path string, data []byte) error {
	f, err := os.OpenFile(osPath(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// syncPath fsyncs a file or a directory.
func syncPath(path string) error {
	f, err := os.Open(osPath(path))
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// code maps an os error to a core.ErrCode, logging real I/O errors (the core
// only sees the code).
func code(err error) core.ErrCode {
	switch {
	case err == nil:
		return core.OK
	case errors.Is(err, fs.ErrNotExist):
		return core.ErrNotFound
	default:
		log.Printf("toys3: io error: %v", err)
		return core.ErrIO
	}
}

// PrepareDir creates root/obj and root/tmp if missing and fsyncs root, so the
// directories are durable before the server's first Started (the core has no
// mkdir action).
func PrepareDir(root string) error {
	for _, d := range []string{format.ObjDir(root), format.TmpDir(root)} {
		if err := os.MkdirAll(osPath(d), 0o755); err != nil {
			return err
		}
	}
	return syncPath(root)
}
