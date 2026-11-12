package metadb

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/topic"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
)

// StateHash is a digest of the materialised state (topics, ends, runs), not of
// the idempotency window: two metadb instances that applied the same commands in the
// same order agree, whatever they trimmed. The model has no state hash.
func (a *DB) StateHash() uint64 {
	h := fnv.New64a()
	_ = a.keyValue.View(func(transaction engine.Txn) {
		transaction.Ascend([]byte{0}, nil, func(k, v []byte) bool {
			if !topic.InWindow(k) {
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
