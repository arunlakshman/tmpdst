// Package golden checks campaign fingerprints against fixed expected hashes.
package golden

import (
	"bufio"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Assert fails t when want[name] is missing or differs from hash.
func Assert(t testing.TB, name string, hash uint64, want map[string]uint64) {
	t.Helper()
	exp, ok := want[name]
	if !ok {
		t.Fatalf("golden %q: no expected hash", name)
	}
	if exp != hash {
		t.Fatalf("golden %q: got %#x want %#x", name, hash, exp)
	}
}

// AssertCampaign fails t when got and want differ for any seed.
// Missing or extra seeds are reported with clear seed lists.
func AssertCampaign(t testing.TB, got, want map[uint64]uint64) {
	t.Helper()
	var missing, extra, mismatch []string
	for seed, exp := range want {
		h, ok := got[seed]
		if !ok {
			missing = append(missing, strconv.FormatUint(seed, 10))
			continue
		}
		if h != exp {
			mismatch = append(mismatch, fmt.Sprintf("seed %d: got %#x want %#x", seed, h, exp))
		}
	}
	for seed := range got {
		if _, ok := want[seed]; !ok {
			extra = append(extra, strconv.FormatUint(seed, 10))
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)
	slices.Sort(mismatch)
	var b strings.Builder
	if len(missing) > 0 {
		fmt.Fprintf(&b, "missing seeds [%s]; ", strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		fmt.Fprintf(&b, "extra seeds [%s]; ", strings.Join(extra, ", "))
	}
	if len(mismatch) > 0 {
		fmt.Fprintf(&b, "mismatches: %s", strings.Join(mismatch, "; "))
	}
	if b.Len() > 0 {
		t.Fatalf("golden campaign: %s", strings.TrimSpace(b.String()))
	}
}

// Load reads a golden file of lines "seed hash" (decimal seed, hex or decimal hash).
// Blank lines and lines starting with # are ignored.
func Load(path string) (map[uint64]uint64, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path is the caller's golden file by design
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	out := map[uint64]uint64{}
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		seedStr, hashStr, ok := strings.Cut(line, " ")
		if !ok {
			return nil, fmt.Errorf("%s:%d: want \"seed hash\"", path, lineNo)
		}
		seed, err := strconv.ParseUint(seedStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: seed: %w", path, lineNo, err)
		}
		hash, err := strconv.ParseUint(hashStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: hash: %w", path, lineNo, err)
		}
		out[seed] = hash
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Save writes got as sorted "seed 0xhash" lines.
func Save(path string, got map[uint64]uint64) error {
	seeds := slices.Sorted(maps.Keys(got))

	var b strings.Builder
	b.WriteString("# seed hash\n")
	for _, seed := range seeds {
		fmt.Fprintf(&b, "%d %#x\n", seed, got[seed])
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}
