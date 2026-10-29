//go:build !darwin

package production

func sysMemory() (uint64, string) { return 0, "unknown" }
