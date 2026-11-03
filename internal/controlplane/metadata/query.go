package metadata

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/topics"
)

// StateHash is a digest of the materialised state (topics, ends, runs), not of
// the idempotency window: two metadata stores that applied the same commands in the
// same order agree, whatever they trimmed. It is not speccheck's StateHash.
func (a *Store) StateHash() uint64 {
	h := fnv.New64a()
	_ = a.keyValue.View(func(transaction kv.Transaction) {
		transaction.Ascend([]byte{0}, nil, func(k, v []byte) bool {
			if !topics.InWindow(k) {
				if _, err := h.Write(binary.AppendUvarint(nil, uint64(len(k)))); err != nil {
					return false
				}
				if _, err := h.Write(k); err != nil {
					return false
				}
				if _, err := h.Write(v); err != nil {
					return false
				}
			}
			return true
		})
	})
	return h.Sum64()
}
