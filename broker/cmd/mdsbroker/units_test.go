package main

import "testing"

func TestParseSize(t *testing.T) {
	ok := map[string]int64{
		"0": 0, "1048576": 1 << 20, "16MiB": 16 << 20, "16mib": 16 << 20, "16 MiB": 16 << 20,
		"16MB": 16_000_000, "128MiB": 128 << 20, "1GiB": 1 << 30, "1.5GiB": 3 << 29,
		"4KiB": 4096, "4KB": 4000, "2GB": 2_000_000_000, "512b": 512, "1TiB": 1 << 40,
	}
	for in, want := range ok {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("%q: %d, %v (want %d)", in, got, err, want)
		}
	}
	for _, in := range []string{"", "auto", "MiB", "-1MiB", "12XB", "1e3x", "99999999999TiB"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}
