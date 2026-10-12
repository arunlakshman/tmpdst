package sim

// Fold mixes x into the running trace hash h. The simulator folds the step
// number, the event time and the server's StateHash after every step (and the
// plant model's hash), so two runs of one seed agree only if their entire
// histories agree.
func Fold(h, x uint64) uint64 {
	h ^= x + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	h *= 0xff51afd7ed558ccd
	return h ^ (h >> 33)
}

// hashString is 64-bit FNV-1a of s.
func hashString(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// hashBytes is 64-bit FNV-1a of b, length-prefixed.
func hashBytes(b []byte) uint64 {
	h := uint64(14695981039346656037) ^ uint64(len(b))
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}
