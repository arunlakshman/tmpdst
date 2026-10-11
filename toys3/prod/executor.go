package prod

import "manifester/toys3/core"

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
	panic("toys3/prod: OSExecutor.Execute unimplemented (phase 2)")
}

// PrepareDir creates root/obj and root/tmp if missing and fsyncs root, so the
// directories are durable before the server's first Started (the core has no
// mkdir action).
func PrepareDir(root string) error {
	panic("toys3/prod: PrepareDir unimplemented (phase 2)")
}
