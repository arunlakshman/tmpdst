package production

import (
	"errors"
	"testing"
	"time"
)

func TestParseCgroupLimit(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want uint64
	}{{"536870912\n", 512 << 20}, {" 1073741824 ", 1 << 30}} {
		in, want := tc.in, tc.want
		if n, ok := parseCgroupLimit(in); !ok || n != want {
			t.Errorf("%q: %d %v", in, n, ok)
		}
	}
	for _, in := range []string{"max\n", "", "0", "x", "9223372036854771712\n", "18446744073709551615"} {
		if n, ok := parseCgroupLimit(in); ok {
			t.Errorf("%q accepted as %d", in, n)
		}
	}
}

func TestParseMemTotal(t *testing.T) {
	const info = "MemTotal:       16384000 kB\nMemFree:         1234 kB\n"
	if n, ok := parseMemTotal(info); !ok || n != 16384000<<10 {
		t.Fatal(n, ok)
	}
	for _, in := range []string{"", "MemFree: 1 kB\n", "MemTotal: lots kB\n", "MemTotal:\n"} {
		if _, ok := parseMemTotal(in); ok {
			t.Errorf("%q accepted", in)
		}
	}
}

func TestMemoryLimitSources(t *testing.T) {
	files := map[string]string{}
	read := func(p string) ([]byte, error) {
		if s, ok := files[p]; ok {
			return []byte(s), nil
		}
		return nil, errors.New("absent")
	}
	sys := func() (uint64, string) { return 7 << 30, "sysctl hw.memsize" }
	check := func(name string, wantN uint64, wantSrc string) {
		t.Helper()
		if n, src := memoryLimit(read, sys); n != wantN || src != wantSrc {
			t.Errorf("%s: %d %q, want %d %q", name, n, src, wantN, wantSrc)
		}
	}
	check("no files", 7<<30, "sysctl hw.memsize")
	files["/proc/meminfo"] = "MemTotal: 8388608 kB\n"
	check("meminfo", 8<<30, "meminfo")
	files["/sys/fs/cgroup/memory/memory.limit_in_bytes"] = "9223372036854771712\n"
	check("v1 unlimited", 8<<30, "meminfo")
	files["/sys/fs/cgroup/memory/memory.limit_in_bytes"] = "2147483648\n"
	check("v1", 2<<30, "cgroup v1")
	files["/sys/fs/cgroup/memory.max"] = "max\n"
	check("v2 max falls through", 2<<30, "cgroup v1")
	files["/sys/fs/cgroup/memory.max"] = "1073741824\n"
	check("v2", 1<<30, "cgroup v2")
	if n, src := MemoryLimit(); src == "" || (src != "unknown" && n == 0) {
		t.Errorf("MemoryLimit on this host: %d %q", n, src)
	}
}

func TestAutoBuffered(t *testing.T) {
	const mib = 1 << 20
	for _, c := range []struct{ limit, max uint64 }{
		{0, 128 * mib},          // unknown
		{1 << 30, 128 * mib},    // 1 GiB / 16 = 64 MiB, clamped up
		{2 << 30, 128 * mib},    // exactly the floor
		{8 << 30, 512 * mib},    // 8 GiB / 16
		{16 << 30, 1024 * mib},  // exactly the ceiling
		{256 << 30, 1024 * mib}, // clamped down
	} {
		max, resume := AutoBuffered(c.limit)
		if uint64(max) != c.max || resume != max/2 {
			t.Errorf("limit %d: max %d resume %d, want %d", c.limit, max, resume, c.max)
		}
	}
}

func TestObjTimeoutScalesWithBody(t *testing.T) {
	c := ExecConfig{}.withDefaults()
	if got := c.objTimeout(0); got != 30*time.Second {
		t.Fatal(got)
	}
	if got := c.objTimeout(16 << 20); got != 38*time.Second { // 30 s + 16 MiB at 2 MiB/s
		t.Fatal(got)
	}
	c = ExecConfig{ObjTimeout: time.Minute, ObjTransferRate: 1 << 20}.withDefaults()
	if got := c.objTimeout(16 << 20); got != 76*time.Second {
		t.Fatal(got)
	}
}
