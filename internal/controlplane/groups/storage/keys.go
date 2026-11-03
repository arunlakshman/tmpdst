// Package storage keeps consumer group state in the ordered key-value store: the
// fold of the four group writes and the three group reads. The keys are G
// (group header), M (member) and O (committed offset). The rules are in
// internal/verify/speccheck (groups.go), which is the oracle.
//
// It does not know about topics. An offset commit asks the caller how many
// partitions a topic has, through the Partitions function.
package storage

import (
	"encoding/binary"
	"errors"

	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/core"
)

const (
	prefixGroup  = 'G'
	prefixMember = 'M'
	prefixOffset = 'O'
)

var errCorrupt = errors.New("groups: corrupt record")

// Partitions tells how many partitions a topic has (0 when it does not exist).
type Partitions func(transaction kv.Transaction, topic string) int64

// groupKey is the G key of a group: its header.
func groupKey(group string) []byte { return append([]byte{prefixGroup}, group...) }

// groupPrefix is the prefix-free key prefix of one group for kind p.
func groupPrefix(p byte, group string) []byte {
	b := binary.AppendUvarint([]byte{p}, uint64(len(group)))
	return append(b, group...)
}

// memberKey is the M key of one member of a group.
func memberKey(group, member string) []byte {
	return append(groupPrefix(prefixMember, group), member...)
}

// offsetKey is the O key of one committed partition of a group.
func offsetKey(group string, topicPartition core.TopicPartition) []byte {
	return kv.AppendTopicPartition(groupPrefix(prefixOffset, group), topicPartition)
}
