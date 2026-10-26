package prod

import (
	"os"
	"strconv"
	"strings"
)

// Machine memory detection, to size the broker's unacked-produce buffer. Not
// deterministic (reads the host), so it lives here and not in broker/.

// Buffer sizing (decision 35). The broker holds roughly 2-3 copies of buffered
// record data (the request frame, the admitted batch, the encoded segment), so
// real memory is a multiple of the cap: a 1/16 share keeps that near 1/5 of RAM.
const (
	MinBufferedBytes = 128 << 20
	MaxBufferedCap   = 1 << 30
	unknownLimit     = 1 << 60 // cgroup "no limit" values are close to MaxInt64
)

// MemFS reads a file; injectable for tests.
type MemFS func(path string) ([]byte, error)

// MemoryLimit returns the memory this process may use and where the number
// came from: "cgroup v2", "cgroup v1", "meminfo", "sysctl hw.memsize", or
// "unknown" (bytes 0).
func MemoryLimit() (uint64, string) { return memoryLimit(os.ReadFile, sysMemory) }

// memoryLimit prefers the container limit over the host total.
func memoryLimit(read MemFS, sys func() (uint64, string)) (uint64, string) {
	if b, err := read("/sys/fs/cgroup/memory.max"); err == nil {
		if n, ok := parseCgroupLimit(string(b)); ok {
			return n, "cgroup v2"
		}
	}
	if b, err := read("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		if n, ok := parseCgroupLimit(string(b)); ok {
			return n, "cgroup v1"
		}
	}
	if b, err := read("/proc/meminfo"); err == nil {
		if n, ok := parseMemTotal(string(b)); ok {
			return n, "meminfo"
		}
	}
	return sys()
}

// parseCgroupLimit parses a cgroup memory limit; "max" and absurdly large
// values (v1 reports ~2^63 for unlimited) mean no limit.
func parseCgroupLimit(s string) (uint64, bool) {
	s = strings.TrimSpace(s)
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 || n >= unknownLimit {
		return 0, false
	}
	return n, true
}

// parseMemTotal reads MemTotal (kB) from /proc/meminfo.
func parseMemTotal(s string) (uint64, bool) {
	for line := range strings.SplitSeq(s, "\n") {
		rest, ok := strings.CutPrefix(line, "MemTotal:")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			return 0, false
		}
		kb, err := strconv.ParseUint(f[0], 10, 64)
		if err != nil || kb == 0 {
			return 0, false
		}
		return kb << 10, true
	}
	return 0, false
}

// AutoBuffered derives the produce buffer cap and its resume mark from a memory
// limit (0 = unknown): clamp(limit/16, 128 MiB, 1 GiB), resume at half.
func AutoBuffered(limit uint64) (int, int) {
	m := uint64(MinBufferedBytes)
	if limit != 0 {
		m = min(max64(limit/16, MinBufferedBytes), MaxBufferedCap)
	}
	return int(m), int(m / 2)
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
