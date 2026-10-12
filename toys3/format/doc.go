// Package format is the on-disk contract of toys3: the byte layout of an
// object file, how keys become file names, and where files live. It is the
// only thing that survives a crash, so it is the contract between one
// incarnation of the server and the next. It is shared by the controller
// (../control), the simulator's monitors and plant model (../sim), and the
// production runtime (../prod). Like control and sim, it is checked by
// tools/detcheck.
//
// # Layout
//
// Everything lives under the data root (the runtime creates both
// subdirectories, durably, before the server starts):
//
//	<root>/obj/<EscapeKey(key)>   one file per committed object
//	<root>/tmp/<id>               in-progress writes; garbage after a crash
//
// # Object file
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
// detected and reported as 500, never served as 200. With the server's write
// protocol a correct server never produces a torn object file; the CRC is the
// safety net that turns a protocol bug into a visible failure instead of
// silent garbage.
package format
