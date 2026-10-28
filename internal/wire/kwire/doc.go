// Package kwire is the pure Kafka wire layer shared by the broker state
// machine and the simulated Kafka clients: stream framing, request/response
// codec and record-batch (magic v2) helpers. It wraps franz-go's kmsg.
//
// Rules, from docs/dep-audits/github.com/twmb/franz-go/pkg/kmsg@v1.14.0.md:
//
//   - Framer returns every frame as a fresh slice that it never reuses or
//     mutates, because decoded kmsg messages alias their input frame (Record
//     keys and values, Produce partition Records, Fetch record batches).
//   - kmsg is used through ReadFrom only, never UnsafeReadFrom.
//   - Everything kmsg writes verbatim is computed here, with no clock: the
//     record-batch CRC (CRC-32C), lengths, and timestamps (passed in).
//
// Framing symmetry: Codec's Encode* functions return a complete frame WITH the
// 4-byte length prefix, ready to write to a connection; Framer strips the
// prefix, so Codec's Decode* functions take a frame WITHOUT it.
//
// Unsupported requests surface as *UnsupportedError from DecodeRequest, which
// also returns the parsed header so the broker can still answer; see
// ApiVersionsResponse for the ApiVersions case.
package kwire
