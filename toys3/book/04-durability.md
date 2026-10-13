# Chapter 4: The durability problem

> *"`write()` returning success means the kernel has your bytes. It does not mean the disk has them. Everything in this chapter lives in the gap between those two sentences."*

toys3 promises one thing above all: **once it has replied 200 to a PUT, COPY or DELETE, that mutation survives a power cut.** This chapter is about what it takes to keep that promise on a POSIX filesystem. We will build the protocol from first principles, starting from the naive approach and breaking it one crash at a time, until we arrive at the sequence the server actually uses:

```
PUT    FSWrite(tmp) → FSFsync(tmp) → FSRename(tmp→obj) → FSFsyncDir(obj/) → Reply 200
DELETE FSUnlink(obj) → FSFsyncDir(obj/) → Reply 200
```
(`toys3/control/doc.go:42-43`)

Then we will look at the bytes on disk (package `toys3/format`), byte by byte, and at how keys become file names. Along the way you will meet the two ideas that the rest of the book leans on most heavily: the difference between **visible** and **durable**, and the fact that **a directory is a file too**.

---

## 4.1 Where your bytes actually are

A process that calls `write(fd, buf, n)` on Linux (or macOS, or any Unix) is not talking to a disk. It is talking to the kernel's **page cache**: a large pool of memory pages, each caching a piece of some file. `write` copies `buf` into the page cache, marks those pages *dirty*, and returns. The disk has not been touched.

```
 process             kernel                               device
 ───────             ──────                               ──────
 write(fd, "v2") ──► page cache: [file X, page 0] dirty   (nothing yet)
   ◄── returns n                                          
 read(fd)        ──► served from page cache: "v2"        (nothing yet)
                     ... later ...
                     writeback thread / journal commit ──► sectors written
```

Every other process on the machine sees the new contents immediately, because every read goes through the same page cache. So after `write` returns, the data is **visible**. It is not **durable**: if power is lost before the kernel writes those dirty pages back, they are gone. When writeback happens is up to the kernel (a periodic flusher, memory pressure, a journal commit), and it may write some dirty pages and not others, in any order.

`fsync(fd)` is the way to force the issue. It asks the kernel to write every dirty page of that file (and the file's metadata, like its size) to stable storage and to wait until the device confirms. When `fsync` returns 0, the contents of the file as they were at the time of the call will survive a power loss. When it returns an error, all bets are off (§4.9).

### Two kinds of "state" for every file

It helps to stop thinking of a file as having *one* content and start thinking of it as having two:

```
           ┌──────────────── file X ─────────────────┐
           │  volatile content : what reads see now  │ ◄── write() changes this
           │  durable content  : what survives power │ ◄── fsync() copies volatile → durable
           └─────────────────────────────────────────┘
```

On a crash, the volatile content is lost and the durable content becomes the content. But "lost" is subtle: because writeback can happen at any moment, some of the volatile changes may *already* have been made durable by the kernel on its own. After a crash, a file whose last `fsync` was before some `write`s may contain:

- the old durable content (nothing was written back),
- the new content (all of it was written back),
- or some mixture: typically a **prefix** of the new content, or new content with the old length, depending on the filesystem. This is a **torn write**.

This is exactly how toys3's PlantModel models a file (`toys3/sim/plantmodel.go:65-71`):

```go
// inode is a file's content in two versions.
type inode struct {
	durable  []byte // what survives a crash (nil for never-synced)
	volatile []byte // what reads see now
	dirty    bool   // volatile differs from durable (un-fsynced writes)
	poisoned bool   // FsyncGate: a past fsync failed; fsyncs now lie
}
```

and on a crash, each dirty inode independently keeps its volatile content, reverts to durable, or (with probability `TornWriteProb`, 0.3 in `BaseConfig`) is cut to a random strict prefix (`toys3/sim/plantmodel.go:499-517`). Chapter 11 walks through the crash resolution in detail. For now, the point is that the model in the simulator is the model in your head: two versions, and an adversary choosing between them at the worst moment.

## 4.2 Directories are files, and names are data

Here is the part that bites experienced engineers. A file's **name** is not part of the file. It is an entry in a **directory**, and a directory is (conceptually) just another file whose content is a table of `name → inode`. Creating a file, renaming a file and deleting a file are all **writes to a directory**. They are visible immediately, through the directory's cached content, and they are durable only when the directory itself is flushed.

So `fsync(fd)` on a file makes the file's *contents* durable. It does **not**, in general, make the file's *name* durable. If you create `tmp/x`, write to it, fsync it, and lose power, you may come back to a disk that has the data blocks safely written and no directory entry pointing at them. The data is there; nobody can find it.

To make a name durable, you open the **directory** and fsync *that*:

```c
int dfd = open("/data/obj", O_RDONLY);
fsync(dfd);   // makes creates/renames/unlinks in /data/obj durable
close(dfd);
```

Whether this is strictly required depends on the filesystem and its mount options. Some journaling filesystems happen to persist the directory change along with the file's metadata in many cases. POSIX does not promise it, and the ALICE study (Pillai et al., OSDI 2014) showed that real applications that relied on such behaviour broke on filesystems that did not provide it. A correct program fsyncs the directory.

toys3's PlantModel models directory entries with the same two-version idea (`toys3/sim/plantmodel.go:73-77`):

```go
// dirent is one name in a directory, in two versions. 0 means absent.
type dirent struct {
	cur     InodeID // visible now (after creates/renames/unlinks)
	durable InodeID // what survives a crash (after FSFsyncDir)
}
```

And the controller's plant port says so in its doc comments (`toys3/control/plant.go:51-54`):

```go
// Paths are slash-separated and absolute under Config.Dir (see ObjPath,
// TmpPath). Changes are volatile until made durable: file contents by FSFsync
// of the file, directory entries (create, rename, unlink) by FSFsyncDir of the
// directory that holds them.
```

That sentence is the whole durability model of toys3. Hold onto it.

## 4.3 `rename` is atomic, but only for visibility

`rename(from, to)` on POSIX has a famous guarantee: if `to` already exists, it is **atomically replaced**. At every instant, any process looking up `to` finds either the old file or the new file, never neither and never a half-written mixture. This is why the "write a temp file, then rename it into place" idiom exists: readers never see a partial file.

But notice what that guarantee is about: *lookups by running processes*. It is a guarantee about the **visible** state. It says nothing about what happens on power loss. After a crash, the rename may be:

- fully durable (the directory was flushed afterwards, by you or by the kernel),
- not durable at all (the directory still maps `to` to the old inode),
- and for a rename *across two directories* (toys3 renames from `tmp/` to `obj/`), the two directory changes, "remove `from` from `tmp/`" and "add `to` to `obj/`", are two separate durable writes. Each can survive or not independently, unless both directories are fsynced.

```
before:   tmp/  {t → I_new}            obj/ {k → I_old}
rename:   tmp/  {t → ∅}       (cur)    obj/ {k → I_new}  (cur)

possible durable outcomes after a crash with neither directory fsynced:
   tmp/ {t → I_new}   obj/ {k → I_old}     rename lost entirely
   tmp/ {t → ∅}       obj/ {k → I_new}     rename fully kept
   tmp/ {t → I_new}   obj/ {k → I_new}     BOTH names: a hard link!
   tmp/ {t → ∅}       obj/ {k → I_old}     temp name gone, object unchanged
```

The PlantModel models precisely this. A rename updates the `cur` field of two dirents (`toys3/sim/plantmodel.go:354-362`), and at a crash each dirent with `cur != durable` is resolved by its own coin flip (`toys3/sim/plantmodel.go:471-488`). The third outcome, two names for one inode, is not an exotic corner case: it is what the test `TestPlantModelRenameWithoutDirFsyncMayVanish` checks for (`toys3/sim/plantmodel_test.go:69-93`, the `both` counter), and it is the root of mutant **f** (Chapter 15).

## 4.4 Building the PUT protocol by breaking it

Let us now design a PUT. The goal: after replying 200 to `PUT k = v2`, a crash at any later moment leaves `obj/k` holding exactly `v2` (or a later value), never `v1`, never nothing, never a torn `v2`.

We will try four protocols, each one a step closer, and break each with a crash. (Every one of the broken variants is a real mutant in `toys3/sim/mutants_test.go`, and Chapter 15 shows the simulator catching each in a handful of seeds.)

### Attempt 1: write in place

```
write(obj/k, v2) → reply 200
```

Breaks in two ways at once:

```
t0  write(obj/k, "v2v2v2v2")   volatile: v2v2v2v2   durable: v1v1v1
t1  reply 200                  ◄── client believes v2 is safe
t2  ⚡ power loss
    adversary: torn prefix      durable: "v2v2"   (or v1v1v1, or v2v2v2v2)
t3  GET k → ???                 garbage, old value, or right value
```

Not durable (no fsync), and not atomic (a reader *during* the write, or a crash during writeback, sees a mixture). Nobody serious does this, but it is worth stating why.

### Attempt 2: write in place, fsync, reply

```
write(obj/k, v2) → fsync(obj/k) → reply 200
```

Durable after the reply, but a crash *before* fsync completes can still tear the file, and the old value is destroyed before the new one is safe. If a client reads during the write, it may see half of `v2`. We need the old and new versions to coexist until the switch.

### Attempt 3: temp file, rename, reply (mutant c's cousin)

```
write(tmp/t, v2) → rename(tmp/t, obj/k) → reply 200
```

Now readers are safe: they see `v1` or `v2`, never a mixture, because rename is atomic for visibility. But nothing is durable:

```
t0  write(tmp/t, v2)           tmp/t: volatile v2, durable ∅ (never synced)
t1  rename(tmp/t → obj/k)      obj/k cur → I_t ; durable still → I_v1
t2  reply 200
t3  ⚡ crash
    dirent obj/k: kept or reverted?   inode I_t: kept, reverted (∅), or torn?
    worst case A: obj/k → I_v1        acked v2 lost
    worst case B: obj/k → I_t, I_t durable = ""  → obj/k is an EMPTY file
```

Case B is the nastiest: the *rename* became durable (the kernel happened to flush the directory) but the *data* did not, so the committed name points at an empty or torn inode. On ext4 this exact pattern was the subject of a famous 2009 controversy in which applications that did "write temp, rename" without fsync lost configuration files after crashes, and ext4 added heuristics (`auto_da_alloc`) to paper over it for the common case. A correct program does not rely on the heuristics.

### Attempt 4: temp file, fsync, rename, reply (mutant a)

```
write(tmp/t, v2) → fsync(tmp/t) → rename(tmp/t, obj/k) → reply 200
```

The data is durable before the rename, so case B is gone: whatever `obj/k` points at after a crash, the inode it points at has complete, durable bytes. But the *name* is not durable:

```
t0  write(tmp/t, v2); fsync(tmp/t)   I_t durable = v2  ✓
t1  rename(tmp/t → obj/k)            obj/k: cur = I_t, durable = I_v1
t2  reply 200                        ◄── acked
t3  ⚡ crash; adversary reverts obj/k dirent
    obj/k → I_v1                     acked v2 LOST
```

The reply promised `v2`; after the crash the client reads `v1`. This is mutant **a** (`mutSkipDirFsync`, `toys3/sim/mutants_test.go:86-89`). The simulator catches it at seed 1, step 46, *without even waiting for a crash*, because the `AckedIsDurable` invariant compares the `Committed` telemetry against the PlantModel's durable view on the same step:

```
#46  t=7.377477ms inc=1 deliver FSRenameDone{io12 OK} => Emit{Committed{c6 "k2" crc=9b35c638}} Reply{c6 200} FSRead{io15 /data/obj/k2}
#46  t=7.377477ms inc=1 crash   FAILURE acked-is-durable: c6: write of "k2" acked but /data/obj/k2 has no durable entry
```

(Chapter 14 explains why checking against the durable view eagerly is such a powerful trick.)

### The correct protocol: temp, fsync, rename, fsync-dir, reply

```
write(tmp/t, v2) → fsync(tmp/t) → rename(tmp/t, obj/k) → fsync(obj/) → reply 200
```

```
step               tmp/t dirent     I_t content         obj/k dirent       safe to crash here?
─────────────────  ───────────────  ──────────────────  ─────────────────  ─────────────────────────────
start              —                —                   cur=dur=I_v1       yes (v1, nothing acked)
FSWrite(tmp)       cur=I_t,dur=∅    vol=v2, dur=∅       cur=dur=I_v1       yes; tmp garbage, recovery cleans
FSFsync(tmp)       cur=I_t,dur=∅    vol=dur=v2          cur=dur=I_v1       yes; tmp garbage
FSRename           cur=∅            vol=dur=v2          cur=I_t,dur=I_v1   yes; obj/k is v1 or v2, both whole
FSFsyncDir(obj/)   cur=∅,dur=?      vol=dur=v2          cur=dur=I_t        yes; obj/k is v2
Reply 200          ...              ...                 cur=dur=I_t        acked AND durable ✓
```

Each step closes exactly one hole:

| Step | Without it, a crash after the 200 can... | Mutant |
|---|---|---|
| write to a **temp** file, not in place | expose a half-written object to readers; destroy v1 before v2 is safe | (design) |
| **fsync** the temp file | leave `obj/k` naming an inode whose durable content is empty or a torn prefix | b |
| **rename** | (no atomic switch; same as writing in place) | (design) |
| **fsync obj/** | revert `obj/k` to the old inode: acked value lost | a |
| reply **last** | acknowledge something that is neither visible nor durable | c |

The server's package doc says the same thing in one paragraph (`toys3/control/doc.go:48-51`):

```go
// Why each step: the file fsync makes the bytes durable; the rename makes the
// switch from old to new contents atomic (readers see old or new, never a mix);
// the directory fsync makes the rename itself durable. Skipping any one of them
// is a bug the simulator's crash model is designed to find.
```

### What the protocol does *not* do: fsync `tmp/`

Notice what is absent: the server never fsyncs `tmp/` after the rename. The *removal* of the temp name from `tmp/` is therefore never made durable by the PUT. That is deliberate. The only thing a client cares about is `obj/`; what happens in `tmp/` after a crash is cleaned up by recovery (Chapter 5). But it has a consequence that is easy to miss: **after a crash, the temp name can come back**, pointing at the same inode as the committed object. Two names, one inode: a hard link.

This actually happens in seed 1 of the default configuration. Here is the life of temp file `0000000000000031` (the hex form of IOID 49):

```
#150 t=35.285919ms inc=1 deliver Request{c23 PUT "k1" value-of-23} => FSWrite{io49 /data/tmp/0000000000000031 obj(value-of-23)}
#154 t=35.592579ms inc=1 fs-land land FSWrite{io49 /data/tmp/0000000000000031 obj(value-of-23)} -> FSWriteDone{io49 OK}
#155 t=35.792963ms inc=1 deliver FSWriteDone{io49 OK} => FSFsync{io51 /data/tmp/0000000000000031}
#156 t=36.356855ms inc=1 fs-land land FSFsync{io51 /data/tmp/0000000000000031} -> FSFsyncDone{io51 OK}
#159 t=36.788933ms inc=1 deliver FSFsyncDone{io51 OK} => FSRename{io53 /data/tmp/0000000000000031 -> /data/obj/k1}
#160 t=37.150944ms inc=1 fs-land land FSRename{io53 /data/tmp/0000000000000031 -> /data/obj/k1} -> FSRenameDone{io53 OK}
   ...
#233 t=53.112270ms inc=1 crash   CRASH (trip: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
#245 t=56.901846ms inc=2 fs-land land FSList{io1 /data/tmp} -> FSListDone{io1 OK [0000000000000031 000000000000004b 000000000000004c]}
```

At step 160 the name `tmp/0000000000000031` disappeared from the visible `tmp/` directory. Somewhere between step 150 and the crash, the *creation* of that name became durable (either a background writeback made it so, or the crash-time coin kept it; the PlantModel's `WritebackProb` is 0.05 per landed action, Chapter 11). The *removal* never did. So in incarnation 2, the server lists `tmp/` and finds a "leftover temp file" which is in fact a second name for the live object `obj/k1`.

For the correct server this is harmless: recovery unlinks the temp name, and unlinking one name of a hard-linked inode does not touch the other name. But if anything ever *wrote through* that resurrected name, it would silently overwrite a committed object. That is exactly what happens when recovery is broken (mutant **f**): Chapter 15 shows a later incarnation reusing the temp name `0000000000000004`, truncating and overwriting the inode that `obj/k5` also names, and a client reading another key's value back from `k5`. Keep this picture in mind:

```
after crash:   tmp/0000000000000031 ──┐
                                      ├──► inode I (content: obj(value-of-23))
               obj/k1 ────────────────┘
```

### DELETE

DELETE is shorter but follows the same logic (`toys3/control/doc.go:43`):

```
FSUnlink(obj/k) → FSFsyncDir(obj/) → Reply 200      (ErrNotFound → Reply 404)
```

`unlink` removes a directory entry: it is a directory write, visible immediately, durable only after the directory is fsynced. Without the fsync, the classic failure is **resurrection**:

```
t0  unlink(obj/k)          obj/k: cur=∅, durable=I_v1
t1  reply 200              ◄── acked delete
t2  ⚡ crash; adversary reverts dirent
    obj/k → I_v1            the deleted object is back
```

`DESIGN.md:102` says it in one line: "Without the dir fsync, a crash can bring the object back."

### COPY and GET

COPY is "read src, verify CRC, then exactly the PUT path for dst" (`toys3/control/doc.go:44-45`). It needs no new durability reasoning: once the source bytes are in memory, it is a PUT of those bytes. GET reads the file and verifies the CRC. Reads change nothing, so they need no fsyncs; but they do need to be *correct about what they return*, which brings us to the on-disk format.

## 4.5 The object file format, byte by byte

Every committed object is one file under `obj/`, consisting of a fixed 16-byte header followed by the payload (`toys3/format/doc.go:19-26`):

```
offset size  field
0      4     magic "TOY3"
4      4     CRC-32 (IEEE) of the payload, little endian
8      8     payload length in bytes, little endian
16     n     payload
```

The constants (`toys3/format/object.go:8-12` and `toys3/format/layout.go:8-10`):

```go
// toys3/format/object.go:8-12
// Object file header (see the package doc for the layout).
const (
	Magic      = "TOY3"
	HeaderSize = 16
)
```

```go
// toys3/format/layout.go:8-10
// MaxNameLen is the longest file name most filesystems accept; an escaped
// key must fit in it.
const MaxNameLen = 255
```

### Why the format is a package of its own

Before the refactor these functions lived in `core/format.go`, inside the controller's package. They now form a package of their own, `toys3/format`, split into `object.go` (the bytes of one object file) and `layout.go` (names and paths). The reason is the domain model of Chapter 3a: the format is not part of the control law. It is the **contract between one incarnation of the server and the next**, the only thing that survives a crash, and it has four users: the controller (which writes and validates object files), the simulator's monitors (which decode durable files to check them), the plant model (which folds content checksums into its state hash), and the production runtime (which creates the directories). In DDD terms it is a *shared kernel*: a small model that several contexts share and agree to change together.

Making it a package had three concrete effects. Everyone who needed only the bytes stopped importing the controller. The package doc (`toys3/format/doc.go`) now holds the byte layout, which used to take twenty lines in the middle of the controller's doc. And the format got its first unit tests (`toys3/format/format_test.go`): round trips, rejection of every kind of damage, the injectivity and safety of `EscapeKey`, `ValidKey`'s length limit, and the paths. Until then nothing tested `EscapeKey` directly; it was covered only by the simulator, and only for the handful of keys the workload uses.

### Encoding

```go
// toys3/format/object.go:14-25
// Checksum is the CRC-32 (IEEE) stored in the object header.
func Checksum(payload []byte) uint32 { return crc32.ChecksumIEEE(payload) }

// EncodeObject returns the full object file for payload: header + payload.
func EncodeObject(payload []byte) []byte {
	b := make([]byte, HeaderSize+len(payload))
	copy(b, Magic)
	binary.LittleEndian.PutUint32(b[4:], Checksum(payload))
	binary.LittleEndian.PutUint64(b[8:], uint64(len(payload)))
	copy(b[HeaderSize:], payload)
	return b
}
```

Line by line: allocate exactly header plus payload; copy the four magic bytes into `b[0:4]`; write the CRC-32 of the payload (not of the header) into `b[4:8]` little endian; write the payload length as a 64-bit little-endian integer into `b[8:16]`; copy the payload. The function allocates a fresh slice, which matters for the "byte slices are immutable once sent" rule (Chapter 2): the server's `FSWrite` carries this new slice, and nobody else holds a reference.

Here is a real object, written by the production binary while this chapter was being prepared. A `PUT /photos/cat.jpg` with body `hello world` (11 bytes) produced a 27-byte file, dumped with `od -A d -t x1 -c`:

```
0000000    54  4f  59  33  85  11  4a  0d  0b  00  00  00  00  00  00  00
           T   O   Y   3 205 021   J  \r  \v  \0  \0  \0  \0  \0  \0  \0
0000016    68  65  6c  6c  6f  20  77  6f  72  6c  64
           h   e   l   l   o       w   o   r   l   d
0000027
```

Decoded:

```
 bytes 0-3   54 4f 59 33                 "TOY3"
 bytes 4-7   85 11 4a 0d                 CRC-32, little endian = 0x0d4a1185 = 222957957
 bytes 8-15  0b 00 00 00 00 00 00 00     length, little endian = 11
 bytes 16-26 68 65 6c 6c 6f 20 77 6f 72 6c 64   "hello world"
```

And the server's log line for that PUT shows the same checksum in the `Committed` telemetry, in decimal:

```
toys3: telemetry control.Committed {Client:1 Key:photos/cat.jpg Deleted:false Sum:222957957}
```

`0x0d4a1185` is `222957957`. The `Committed` telemetry's `Sum` is `format.Checksum(r.data)` (`toys3/control/controller.go:257-260`), the same function that wrote the header, which is what lets the simulator's `AckedIsDurable` invariant check "the durable file decodes to a payload with exactly the committed checksum" (`toys3/sim/invariant.go:156-161`).

### Decoding

```go
// toys3/format/object.go:27-44
func DecodeObject(file []byte) (payload []byte, ok bool) {
	if len(file) < HeaderSize || string(file[:4]) != Magic {
		return nil, false
	}
	sum := binary.LittleEndian.Uint32(file[4:])
	n := binary.LittleEndian.Uint64(file[8:])
	if n != uint64(len(file)-HeaderSize) {
		return nil, false
	}
	payload = file[HeaderSize:]
	if Checksum(payload) != sum {
		return nil, false
	}
	return payload, true
}
```

Four checks, in increasing cost:

1. **Short file**: fewer than 16 bytes cannot even hold a header. A file torn inside its header (the PlantModel cuts torn files to a random strict prefix, so this happens) fails here. `TestCorruptObject` exercises exactly this with a 3-byte file `"TOY"` (`toys3/control/controller_test.go:357-358`).
2. **Magic**: the first four bytes must be `TOY3`. An empty file (length 0) also fails at check 1.
3. **Length**: the header's length must equal the actual payload length. A file torn anywhere in the payload fails here, because a strict prefix is shorter than the header claims. Note the comparison is in `uint64` and `len(file)-HeaderSize` is non-negative because check 1 passed.
4. **CRC**: the CRC-32 of the payload must match. A file whose length is intact but whose bytes are wrong (a bit flip) fails here.

The returned payload **aliases** the input (`file[HeaderSize:]`), as the doc comment says. The server immediately puts it in a `Reply` or in `r.data` for a COPY; neither mutates it.

Why is the length field 8 bytes when objects are capped at `MaxObjectSize` (64 bytes in simulation)? Because the format should not have to change when the cap does, and 8 bytes is cheap. Why little endian? Because `encoding/binary.LittleEndian` is the conventional choice on the hardware this runs on; nothing depends on it except that encoder and decoder agree.

### Why a CRC at all? A safety net, not a mechanism

Here is a subtle and important point. With the correct protocol, **a torn file never appears under `obj/`**. The temp file is fsynced before it is renamed, so every inode that `obj/` ever names, durably or visibly, has complete durable content. The CRC check, therefore, should *never* fire for the correct server on an honest disk.

So why have it? Two reasons, both stated in the package doc (`toys3/format/doc.go:27-30`):

```go
// A torn or truncated file fails the length or CRC check, so corruption is
// detected and reported as 500, never served as 200. With the protocol below a
// correct server never produces a torn object file; the CRC is the safety net
// that turns a protocol bug into a visible failure instead of silent garbage.
```

1. **To turn bugs into visible failures.** If someone breaks the protocol (mutant **b** skips the temp fsync), the CRC turns "serve garbage with 200" into "500 + `CorruptionDetected` telemetry". A 500 is honest; silent garbage is not.
2. **Because real disks and buses do flip bits.** Media errors, bad RAM, bad cables. A storage server that returns whatever bytes it reads is trusting hardware it should not trust.

The simulator makes the "safety net, not mechanism" claim **checkable**. The PlantModel has a `ReadCorruptProb` knob (`toys3/sim/plantmodel.go:32-36`) that flips one bit in a read's returned buffer while leaving the stored data intact. And there is a test that turns it off and asserts the CRC check never fires across 200 seeds (`toys3/sim/sim_test.go:120-133`):

```go
// TestCorruptionUnreachableWithoutBitFlips documents that for the
// correct server the CRC check is only a safety net: with no read
// corruption injected, torn temp files never reach obj/, so no read ever
// fails its CRC.
func TestCorruptionUnreachableWithoutBitFlips(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Disturbances.ReadCorruptProb = 0
		r := Run(cfg)
		if len(r.Failures) > 0 || r.Stats.CorruptionsDetected > 0 {
			t.Fatalf("seed %d: failures=%v corrupt=%d", seed, r.Failures, r.Stats.CorruptionsDetected)
		}
	}
}
```

This test is unusual and worth admiring: it asserts that a piece of defensive code is **unreachable** under correct operation. That is a strong statement about the protocol, and it also explains why the read-corruption fault had to be added at all. Without it, the CRC check is dead code under test, and a mutant that removes it (mutant **e**, `mutSkipCRC`) cannot be caught. While writing this book, mutant e was run against 1,000 seeds with `ReadCorruptProb = 0`: not caught once. With bit flips on, it is caught at seed 18. Here is the same instant in seed 18 with the correct server and with mutant e:

```
correct server:
#656 t=153.982439ms inc=7 fs-land land FSRead{io87 /data/obj/k3} -> FSReadDone{io87 OK raw(29 bytes)}
#657 t=154.136154ms inc=7 deliver FSReadDone{io87 OK raw(29 bytes)} => Emit{CorruptionDetected{"k3"}} Reply{c98 500}

mutant e (CRC check skipped):
FAIL no-corrupt-reads step 657: 200 GET returned bytes no PUT wrote: req 98 client 0 GET "k3" [184..185 @153865676..154136154] -> 200 garbage(13 bytes)
```

`raw(29 bytes)` is the describer's way of saying "this does not decode as an object": 16 bytes of header plus 13 bytes of payload with one bit flipped. The correct server emits `CorruptionDetected` and replies 500; the mutant serves 13 bytes of garbage with a 200.

Note, too, what the CRC does **not** protect against: a stale-but-valid object. If a crash reverts `obj/k` to an older complete file, the CRC is perfectly happy. Detecting *that* requires knowing what the right answer was, which is the oracle's job (Chapter 13). The CRC catches *torn* and *corrupt*, not *wrong*.

## 4.6 From keys to file names

S3 keys are arbitrary strings, up to 1024 bytes, containing slashes, dots, spaces, Unicode, anything. File names on a POSIX filesystem may not contain `/` or NUL, may not be `.` or `..`, and are limited (on most filesystems) to 255 bytes. toys3 needs a mapping from keys to single file names inside `obj/`.

### EscapeKey

```go
// toys3/format/layout.go:12-30, 54-56
// EscapeKey maps a key to a single safe file name: ASCII letters, digits, '-'
// and '_' pass through; every other byte becomes %XX (uppercase hex). The
// mapping is injective, and the result never contains '/', and is never "."
// or "..".
func EscapeKey(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		c := key[i]
		if isUnreserved(c) {
			b.WriteByte(c)
			continue
		}
		const hex = "0123456789ABCDEF"
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&15])
	}
	return b.String()
}

func isUnreserved(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '-' || c == '_'
}
```

It works byte by byte (not rune by rune), so multi-byte UTF-8 characters become a sequence of `%XX` triples. Worked examples:

| Key | File name | Notes |
|---|---|---|
| `k0` | `k0` | the simulator's keys pass through unchanged |
| `b/k` | `b%2Fk` | `/` is 0x2F; from `TestHappyPath` (`toys3/control/controller_test.go:215-216`) |
| `photos/cat.jpg` | `photos%2Fcat%2Ejpg` | `.` is 0x2E; seen in the real `obj/` listing |
| `sp ace/x` | `sp%20ace%2Fx` | from `TestEndToEnd` (`/b/sp%20ace/x` URL) |
| `.` | `%2E` | never `.` |
| `..` | `%2E%2E` | never `..` |
| `100%` | `100%25` | `%` itself is escaped, which is what makes it reversible |
| `é` (UTF-8 `c3 a9`) | `%C3%A9` | byte-wise |

Why is `.` escaped when it would be perfectly legal inside a file name like `cat.jpg`? Because the rule "only `[A-Za-z0-9_-]` pass through" is *simple*, and simple rules are easy to prove things about. With `.` passing through you would need a special case for keys `.` and `..`. With it escaped, the claim "the result is never `.` or `..`" is immediate: neither contains anything but dots, and dots are never output. Likewise "never contains `/`" is immediate.

### Injectivity, and why uppercase-only matters

Two different keys must never map to the same file name, or two objects would share one file. The mapping is injective because:

- unreserved bytes map to themselves,
- every other byte maps to `%` plus two hex digits,
- `%` is itself not unreserved, so a `%` in the output always starts an escape, never stands for itself.

So the output can be parsed back unambiguously, left to right. `UnescapeKey` (`toys3/format/layout.go:32-52`) does exactly that, and its validity checks are stricter than you might expect:

```go
		v, err := strconv.ParseUint(name[i+1:i+3], 16, 8)
		if err != nil || name[i+1:i+3] != strings.ToUpper(name[i+1:i+3]) || isUnreserved(byte(v)) {
			return "", false
		}
```

It rejects lowercase hex (`%2f`) and escapes of unreserved bytes (`%41` for `A`). Why? Because those would be *alternative spellings* of names that `EscapeKey` produces. If `%2f` and `%2F` both unescaped to `/`, then two different file names would map to one key, and a stray file in `obj/` could shadow or alias a real object. Requiring the canonical spelling makes `EscapeKey ∘ UnescapeKey` the identity on valid names, i.e. a bijection between keys and valid names.

(A small aside: `UnescapeKey` is not called anywhere in the server today. The server never lists `obj/`; it only constructs paths from keys. The function exists so that tools, or a future `ListObjects`, can turn names back into keys. Its bounds check `i+2 >= len(name)` is correct but subtle: an escape at index `i` needs indices `i+1` and `i+2`, so it requires `i+2 < len(name)`.)

### ValidKey and MaxNameLen

```go
// toys3/format/layout.go:58-62
// ValidKey reports whether key can be stored: non-empty and short enough that
// its escaped name fits in MaxNameLen.
func ValidKey(key string) bool {
	return key != "" && len(EscapeKey(key)) <= MaxNameLen
}
```

The limit is on the **escaped** length, because that is what the filesystem sees. Escaping can triple the length, so a key of 100 slashes (300 escaped bytes) is invalid, which is precisely the "long key" case in `TestBadRequest` (`toys3/control/controller_test.go:256`): `strings.Repeat("/", 100)` gets a 400. An empty key is invalid too; the simulator's workload deliberately issues empty-key requests (`OpMix.Invalid`) to exercise this path (Chapter 12).

Invalid requests get `400` immediately and never enter the request queue (`toys3/control/controller.go:150-175`), which is why the oracle can ignore them completely (`toys3/sim/oracle.go:278-280`): "rejected before touching anything".

## 4.7 Paths and the on-disk layout

The remaining helpers in `layout.go` construct paths:

```go
// toys3/format/layout.go:64-79
// ObjDir is the directory holding committed objects.
func ObjDir(root string) string { return root + "/obj" }

// TmpDir is the directory holding in-progress writes.
func TmpDir(root string) string { return root + "/tmp" }

// ObjPath is the file holding key's committed object.
func ObjPath(root, key string) string { return ObjDir(root) + "/" + EscapeKey(key) }

// TmpPath is the temp file for one write, named by the IOID of its FSWrite
// (16 hex digits). Names are unique within an incarnation, and recovery clears
// tmp/ before a new incarnation writes anything, so they never collide.
func TmpPath(root string, id uint64) string {
	s := strconv.FormatUint(uint64(id), 16)
	return TmpDir(root) + "/" + strings.Repeat("0", 16-len(s)) + s
}
```

Note the parameter type of `TmpPath`: a plain `uint64`, not a `control.IOID`, even though the doc says the name comes from an IOID. That is deliberate. A shared kernel must not depend on any of the contexts that share it, so `format` imports nothing from toys3; the controller converts at the call site, `format.TmpPath(s.cfg.Dir, uint64(io))` (`toys3/control/controller.go:231`). (The `uint64(id)` conversion inside the function is now redundant, a harmless leftover of the move.)

Paths are always slash-separated strings, even on Windows; the production executor converts with `filepath.FromSlash` (`toys3/prod/executor.go:74`). Neither `control` nor `format` imports `path/filepath` (it is not on the determinism allow-list, Chapter 8).

A temp file is named after the IOID of the `FSWrite` that creates it, in 16 lowercase hex digits. In the logs: `io49` → `0000000000000031` (0x31 = 49), `io75` → `000000000000004b`, `io7` → `0000000000000007`. (Note: lowercase hex here, uppercase in `EscapeKey`. They are different namespaces in different directories; nothing ever compares them.)

The comment's claim "they never collide" rests on two facts, and it is worth stating both because a mutant breaks the second:

1. **Within one incarnation**, IOIDs come from a counter that only goes up (`newIO`, `toys3/control/controller.go:135-138`), so temp names are unique.
2. **Across incarnations**, IOIDs restart at 1 (a new `Controller` has `nextIO = 0`), so incarnation 2 will reuse names incarnation 1 used. That is safe only because **recovery empties `tmp/` before the new incarnation issues any write**. Mutant **f** skips the cleanup, and the reuse turns into the hard-link overwrite described in §4.4.

The full layout:

```
<Config.Dir>/                      e.g. /data (sim) or ./toys3-data (prod)
├── obj/                           committed objects; durable view must always be clean
│   ├── k0                         ┐
│   ├── b%2Fk                      │ one file per key: "TOY3" | crc | len | payload
│   └── photos%2Fcat%2Ejpg         ┘
└── tmp/                           in-progress writes; anything here after a boot is garbage
    ├── 0000000000000007           FSWrite io7 of the current incarnation
    └── 0000000000000031           possibly a resurrected alias of an obj/ file (§4.4)
```

Two invariants describe this layout:

- **`obj/` holds only complete objects, durably.** This is invariant (e), `ObjNeverTorn`, checked whenever the durable state changes (`toys3/sim/invariant.go:168-186`).
- **`tmp/` holds nothing the server cares about after a boot.** Recovery enforces it (Chapter 5).

### Who creates the directories?

The controller has no `mkdir` action. `Config.Dir`'s comment says "obj/ and tmp/ live under it and must already exist" (`toys3/control/controller.go:7`). In simulation, the PlantModel is constructed with both directories already present and durable (`toys3/sim/simulator.go:59`, `toys3/sim/plantmodel.go:128-143`). In production, `PrepareDir` creates them before the first `Started`:

```go
// toys3/prod/executor.go:115-125
// PrepareDir creates root/obj and root/tmp if missing and fsyncs root, so the
// directories are durable before the server's first Started (the controller
// has no mkdir action).
func PrepareDir(root string) error {
	for _, d := range []string{format.ObjDir(root), format.TmpDir(root)} {
		if err := os.MkdirAll(osPath(d), 0o755); err != nil {
			return err
		}
	}
	return syncPath(root)
}
```

`MkdirAll` creates the directories; `syncPath(root)` opens `root` and fsyncs it, making the `obj` and `tmp` entries in `root` durable. That is the right fsync for the two subdirectories. A careful reader will spot one gap: if `root` itself did not exist and `MkdirAll` created it, the entry for `root` in *its* parent directory is not fsynced. On a crash immediately after first start, the whole data directory could vanish. Nothing has been acked yet at that point, so no promise is broken, but it is the kind of edge that DST of this code cannot see because the simulator never models directory creation. (Listed among the limitations in Chapter 17.)

## 4.8 What a real filesystem actually does

The PlantModel (Chapter 11) is a model, and it is deliberately *more adversarial* than most real filesystems. It is worth knowing where it is stricter so you can calibrate.

- **ext4 (default `data=ordered`)** writes file data before committing the metadata that references it, which in practice prevents many "rename durable, data not" outcomes. ext4 also has `auto_da_alloc`, a heuristic that forces data out on rename-over-existing-file. A rename and the unlink of the old name within one directory are committed together in one journal transaction. The PlantModel promises none of this: each un-synced dirent and each dirty inode is resolved *independently*.
- **Cross-directory renames** on a journaling filesystem are typically atomic in the journal: both directory changes commit together. The PlantModel resolves the two dirents independently, which is how it can produce "both names survive" (the hard-link alias in §4.4). On ext4 that particular outcome is unlikely. Defending against it costs nothing (recovery unlinks temps), so the stricter model is a good bet: it is the kind of thing that *some* filesystem, under *some* mount option, eventually does.
- **XFS, btrfs, APFS, ZFS** each have their own ordering guarantees and their own history of surprises. The ALICE study found crash-consistency bugs in widely used applications (including databases and version control systems) precisely because those applications had been tested on one filesystem's guarantees and relied on them implicitly.
- **macOS** `fsync` does not force data past the drive's write cache; `fcntl(F_FULLFSYNC)` does. Go's `File.Sync` on darwin uses `F_FULLFSYNC`. The production executor uses `File.Sync` (`toys3/prod/executor.go:94`), so on macOS it pays that cost.

The guiding principle: **program against POSIX's promises, test against an adversary that keeps only those promises**, and treat everything a particular filesystem does beyond that as luck you did not ask for.

## 4.9 When fsync fails: a preview of fsyncgate

All of the above assumed `fsync` succeeds. What should a program do when it returns `EIO`?

The intuitive answer is "retry". In 2018, PostgreSQL developers discovered (in a long mailing-list thread later nicknamed "fsyncgate", and in LWN's coverage of it) that on Linux this is wrong in a particularly treacherous way. When writeback of a dirty page fails, the kernel reports the error to the next `fsync` caller and then **marks the page clean**. The data is still not on disk, but the kernel no longer considers it dirty. A retried `fsync` therefore has nothing to write and **returns success**. The application believes its data is durable; it is not, and the page may later be evicted from the cache, at which point even *reads* return the old content.

The PlantModel models this with `FsyncGate` (`toys3/sim/plantmodel.go:45-48`):

```go
	// FsyncGate models Linux after a failed fsync: the dirty data is marked
	// clean, later fsyncs of that file return OK, and the data still never
	// reaches disk. Only a fresh FSWrite clears it.
	FsyncGate bool
```

and the test `TestPlantModelFsyncGateLies` (`toys3/sim/plantmodel_test.go:157-171`) demonstrates the lie: a failed fsync, a retry that "succeeds", a crash, and an empty file.

The consequence for the server is simple and strict: **after an fsync error, the server cannot trust anything it believes about that file, so it must not keep serving.** It replies 500 and **trips** (fail-stop), and the next boot recovers from whatever is actually on disk. Chapter 7 develops the full error policy, including the less obvious rule that an error from `FSRename`, `FSUnlink` or `FSFsyncDir(obj/)` must also trip, and the mutants (d, d2) that prove why.

## 4.10 Summary: the durability contract in one picture

```
      client                     server                              disk (durable view)
        │  PUT k=v2                 │                                   obj/k → v1
        │ ────────────────────────► │ write tmp/t (v2)                  tmp/t ?   (garbage if crash)
        │                           │ fsync tmp/t                       I_t = v2  (data safe)
        │                           │ rename tmp/t → obj/k   (visible)  obj/k → v1 or v2  (both whole)
        │                           │ fsync obj/             (durable)  obj/k → v2
        │ ◄──────────────────────── │ 200  (Committed telemetry)        obj/k → v2  ✓ promise kept
        │  GET k                    │ read obj/k, check magic/len/CRC
        │ ◄──────────────────────── │ 200 v2 | 404 | 500 (corrupt)
```

## Key takeaways

- Every file and every directory entry has two states: **visible** (what reads see now) and **durable** (what survives power loss). `write`, `rename`, `unlink` change the visible state. `fsync(file)` makes contents durable; `fsync(dir)` makes names durable.
- `rename` is atomic for **visibility**, not for durability. A cross-directory rename is two directory writes, and can survive partially.
- The minimal correct PUT is **write temp → fsync temp → rename → fsync obj/ → reply**. Each step closes a specific hole, and each omission is a mutant the simulator catches.
- DELETE needs the directory fsync too, or deleted objects resurrect.
- The server never fsyncs `tmp/` after a rename, so temp names can come back as hard-link aliases of committed objects. Recovery must remove them before any temp name is reused.
- The object header (`TOY3`, CRC-32, 64-bit length) makes torn and corrupt files detectable. For a correct protocol on an honest disk the check is **unreachable**, and a test proves it. It is a safety net that turns bugs into 500s, not a mechanism the protocol depends on.
- Key escaping is a canonical, injective, uppercase-only byte-wise percent encoding that never produces `/`, `.` or `..`, and validity is judged on the escaped length.
- The PlantModel is deliberately more adversarial than real filesystems. Program against POSIX, test against an adversary that keeps only POSIX's promises.

## Exercises

1. **Verify the hex dump yourself.** Build and run the server in a fresh directory:
   ```sh
   go build -o /tmp/toys3 ./toys3/cmd/toys3
   D=$(mktemp -d); /tmp/toys3 -dir $D -addr 127.0.0.1:18099 & sleep 1
   curl -s -X PUT --data-binary 'hello world' http://127.0.0.1:18099/photos/cat.jpg -w '%{http_code}\n'
   od -A d -t x1 -c "$D/obj/photos%2Fcat%2Ejpg"; kill %1
   ```
   Predict every byte of the header before you look. Then change the body to 300 bytes and predict bytes 8-15.
2. **Write a property test for escaping.** In a scratch `_test.go` file in `toys3/control` (delete it afterwards), check for random byte strings `k` that `UnescapeKey(EscapeKey(k)) == (k, true)`, that `EscapeKey(k)` never contains `/` and is never `.` or `..`, and that `UnescapeKey("%2f")` fails. Then find a name that `UnescapeKey` accepts which `EscapeKey` would never produce, or argue that none exists.
3. **Break the format.** Change `DecodeObject` to skip the length check (keep the CRC check). Predict: which kinds of torn file would still be caught, and by which check? Run `go test ./toys3/control -run TestCorruptObject -v`. Does it still pass? Why? (Hint: compute the CRC of a prefix.) Revert.
4. **Find the resurrected temp.** Run seed 1 with the event log (see `TestSeeds`'s `-seed` flag; the log is printed only on failure, so you will need a small program like the one described in Chapter 16, or temporarily add `t.Log(FormatLog(r.Log))`). Locate the temp file `0000000000000031`, the step at which it was renamed into `obj/k1`, and the step at which incarnation 2 lists it. Then explain, using only `plantmodel.go`, the two ways its *creation* could have become durable.
5. **Predict before running.** Without running anything, write down which invariant you expect to catch each of: (a) skipping the temp-file fsync, (b) skipping the `obj/` fsync, (c) replying before the rename. Then run `go test ./toys3/sim -run TestCatchesBugs -v` and compare with mutants a, b and c. If you got (a) wrong, re-read §4.4 Attempt 3, case B.
6. **The ext4 question.** Suppose you knew your deployment used ext4 with `data=ordered`. Which of the steps in the PUT protocol could you argue are unnecessary on that filesystem? Write the argument down, then list what you would lose if the deployment ever moved to a different filesystem. Decide whether you would remove the step.

## Further reading

- Thanumalayan Sankaranarayana Pillai et al., *"All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications"*, OSDI 2014. The ALICE tool and study; the single best reference for this chapter.
- Dan Luu, *"Files are hard"* (2015). A tour of how hard it is to get file I/O right, with references to the research.
- Jonathan Corbet, *"PostgreSQL's fsync() surprise"*, LWN.net, 2018. The accessible account of fsyncgate.
- Anthony Rebello et al., *"Can Applications Recover from fsync Failures?"*, USENIX ATC 2020. Systematic study of how applications and filesystems behave after fsync errors.
- Jeff Moyer, *"Ensuring data reaches disk"*, LWN.net, 2011. A clear explanation of the write / fsync / directory-fsync rules.
- The `rename(2)` and `fsync(2)` man pages. Read the "NOTES" sections.
