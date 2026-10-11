package sim

// Fold mixes x into the running trace hash h. The simulator folds the step
// number, the event time and the server's StateHash after every step (and the
// fake filesystem's hash), so two runs of one seed agree only if their entire
// histories agree.
func Fold(h, x uint64) uint64 {
	h ^= x + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	h *= 0xff51afd7ed558ccd
	return h ^ (h >> 33)
}
