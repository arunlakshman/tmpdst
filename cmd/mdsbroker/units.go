package main

import (
	"fmt"
	"strconv"
	"strings"
)

// parseSize reads a byte count: plain bytes ("1048576"), binary units
// (KiB, MiB, GiB, TiB) or decimal ones (KB, MB, GB, TB), with an optional
// fraction ("1.5GiB") and optional space. Case does not matter.
func parseSize(s string) (int64, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	units := []struct {
		suffix string
		mult   float64
	}{
		{"tib", 1 << 40}, {"gib", 1 << 30}, {"mib", 1 << 20}, {"kib", 1 << 10},
		{"tb", 1e12}, {"gb", 1e9}, {"mb", 1e6}, {"kb", 1e3}, {"b", 1},
	}
	mult := 1.0
	for _, u := range units {
		if before, ok := strings.CutSuffix(t, u.suffix); ok {
			t, mult = strings.TrimSpace(before), u.mult
			break
		}
	}
	n, err := strconv.ParseFloat(t, 64)
	if err != nil || n < 0 || t == "" {
		return 0, fmt.Errorf("invalid size %q (want e.g. 16MiB, 16MB, 1GiB or plain bytes)", s)
	}
	v := n * mult
	if v >= 1<<62 {
		return 0, fmt.Errorf("size %q is too large", s)
	}
	return int64(v), nil
}
