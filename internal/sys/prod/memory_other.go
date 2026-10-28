//go:build !darwin

package prod

func sysMemory() (uint64, string) { return 0, "unknown" }
