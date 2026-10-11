// Package core is the deterministic heart of toys3, a toy single-node S3-style
// object server whose only job is to be a small, honest target for
// deterministic simulation testing (DST). See ../DESIGN.md and ../../loop.md.
//
// The server is a StateMachine: Step(Event) []Action. It never performs I/O.
// Client requests and filesystem completions arrive as Events; replies and
// filesystem operations leave as Actions. A runtime (../prod for real disks,
// ../sim for the simulator) executes the actions and feeds results back in.
//
// Rules (enforced by tools/detcheck in CI):
//   - imports only from tools/detcheck/allow.txt (no time, os, math/rand, sync);
//   - no goroutines, channels, select, or package-level variables;
//   - never let map iteration order decide behavior;
//   - Events and Actions are plain data; byte slices are immutable once sent.
//
// # On-disk layout
//
// Everything lives under Config.Dir (the runtime creates both subdirectories,
// durably, before delivering Started):
//
//	<dir>/obj/<EscapeKey(key)>   one file per committed object
//	<dir>/tmp/<id>               in-progress writes; garbage after a crash
//
// An object file is a fixed header followed by the payload (see EncodeObject):
//
//	offset size  field
//	0      4     magic "TOY3"
//	4      4     CRC-32 (IEEE) of the payload, little endian
//	8      8     payload length in bytes, little endian
//	16     n     payload
//
// A torn or truncated file fails the length or CRC check, so corruption is
// detected and reported as 500, never served as 200. With the protocol below a
// correct server never produces a torn object file; the CRC is the safety net
// that turns a protocol bug into a visible failure instead of silent garbage.
//
// # Durable write protocol
//
// A 200 for a mutation is sent only after every byte and directory entry it
// depends on has been fsynced:
//
//	PUT    FSWrite(tmp) → FSFsync(tmp) → FSRename(tmp→obj) → FSFsyncDir(obj/) → Reply 200
//	DELETE FSUnlink(obj) → FSFsyncDir(obj/) → Reply 200     (ErrNotFound → Reply 404)
//	COPY   FSRead(src) → verify CRC → then exactly the PUT path for dst
//	                                           (src ErrNotFound → 404, corrupt → 500)
//	GET    FSRead(obj) → verify CRC → Reply 200 / 404 / 500 (corrupt)
//
// Why each step: the file fsync makes the bytes durable; the rename makes the
// switch from old to new contents atomic (readers see old or new, never a mix);
// the directory fsync makes the rename itself durable. Skipping any one of them
// is a bug the simulator's crash model is designed to find.
//
// Any filesystem error replies 500 and is never acked. After an fsync error the
// kernel's view of the file is unknowable ("fsyncgate"), so the server should
// fail-stop with Halt rather than keep serving possibly-lost state.
//
// # Errors: 500, or 500 + Halt
//
// The danger is not the 500 itself (a 500 promises nothing) but what later
// reads observe. If a failed mutation may have changed obj/ visibly but not
// durably, a later GET can observe the change and a crash can then undo it:
// the client saw a value appear and vanish, which no linearization explains
// ("crashes erase nothing"). The per-key lock only protects reads while the
// mutation is in flight; once it has replied, the next GET on that key runs.
// So the rule is: any error at or after the first step that can touch obj/
// halts, and errors before it are a plain 500.
//
//	FSRead (GET, COPY src)    ENOENT → 404; EIO → 500. Nothing changed.
//	FSWrite(tmp)              any error → 500. Only a temp file changed;
//	                          the next recovery deletes it.
//	FSFsync(tmp)              any error → 500 + Halt (fsyncgate).
//	FSRename(tmp→obj)         any error → 500 + Halt: it may have applied.
//	FSUnlink(obj) (DELETE)    ENOENT → 404; EIO → 500 + Halt: it may have applied.
//	FSFsyncDir(obj)           any error → 500 + Halt (fsyncgate, and the
//	                          rename/unlink above it is visible but not durable).
//	recovery (list, unlink,   any error (except unlink ENOENT) → Halt; the
//	fsync tmp/)               next boot retries recovery from scratch.
//
// Halt is always the last action of its Step; the 500 Reply (if any)
// precedes it, and the server ignores every event after it. Corrupt object
// files are a plain 500 plus a CorruptObject fact, not a Halt: the file is
// already durable, restarting would not fix it.
//
// Every successful mutation emits Emit{Committed} immediately followed by its
// 200 Reply, in the same Step, right after FSFsyncDirDone(obj) returns OK.
//
// # Concurrency
//
// Operations on the same key are serialized by a per-key FIFO queue inside the
// state machine; different keys proceed concurrently. COPY occupies both src
// and dst: a request joins the queue of every key it touches in arrival order
// and runs once it is at the head of all of them. Because every queue is
// ordered by the same arrival sequence, no cycle (deadlock) is possible.
//
// In the code the queues are implicit: the server keeps one slice of
// unfinished requests in arrival order, and the queue of a key is the
// subsequence of that slice touching the key. Invalid requests (bad key,
// oversize PUT) get 400 at once and never enter it.
//
// # Recovery
//
// Memory is disposable. On Started the server lists <dir>/tmp, unlinks every
// leftover temp file, fsyncs the tmp directory (even if nothing was removed),
// emits Recovered, and only then serves requests, starting the queued ones in
// arrival order. Requests that arrive during recovery are queued, not
// rejected. Completions with an unknown IOID (or of the wrong kind for the
// step the owner is on) are ignored. Committed
// objects need no recovery: the rename protocol means obj/ only ever holds
// complete files.
package core
