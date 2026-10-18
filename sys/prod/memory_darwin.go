package prod

import "golang.org/x/sys/unix"

func sysMemory() (uint64, string) {
	if n, err := unix.SysctlUint64("hw.memsize"); err == nil && n > 0 {
		return n, "sysctl hw.memsize"
	}
	return 0, "unknown"
}
