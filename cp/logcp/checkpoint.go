package logcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
)

// A checkpoint is the applier's whole state (applier.Snapshot) at slot n, an
// object ckpt/<n> in the object store, named by one pointer item in the table:
//
//	pk CKPT, sk "-", ck_slot N, ck_key S, ck_size N, ck_sum S (sha-256 hex), ck_ts N (ms)
//
// The pointer moves forward only (a conditional write), after the object is
// complete, so a reader sees either the old checkpoint or the new one. The
// snapshot carries the idempotency window, so replay of the log from n gives
// the state of a full replay and a restart answers replays of recent keys as
// before. The log below n is no longer needed to restart; it is kept for
// KeepSlots more (decision 48) and is then trimmed.

const (
	ckptPK     = "CKPT"
	ckptPrefix = "ckpt/"
	ckptKeep   = 2 // checkpoint objects kept
)

// ErrNoCheckpoint: the pointer's object is unusable and the log no longer
// starts at slot 0, so the state cannot be rebuilt.
var ErrNoCheckpoint = errors.New("logcp: checkpoint unusable and the log is trimmed")

type ckptState struct {
	mu   sync.Mutex
	slot int64 // newest checkpoint known (written or restored)
	at   int64 // when it was written by this node, env ns (0: never)
}

func (s *Service) canCheckpoint() bool {
	return s.cfg.Objects != nil && s.cfg.DB != nil && s.cfg.Table != ""
}

func pkSK(pk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": sv(pk), "sk": sv("-")}
}

// getPointer reads a pointer item (consistent); nil if absent.
func (s *Service) getPointer(ctx context.Context, pk string) (map[string]types.AttributeValue, error) {
	out, err := s.cfg.DB.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.cfg.Table, Key: pkSK(pk), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, err
	}
	return out.Item, nil
}

func numAttr(it map[string]types.AttributeValue, name string) int64 {
	n, _ := it[name].(*types.AttributeValueMemberN)
	if n == nil {
		return 0
	}
	var v int64
	fmt.Sscan(n.Value, &v)
	return v
}

func strAttr(it map[string]types.AttributeValue, name string) string {
	v, _ := it[name].(*types.AttributeValueMemberS)
	if v == nil {
		return ""
	}
	return v.Value
}

// Checkpoint writes a checkpoint of the applied state now and trims the log
// behind it. It returns the slot of the checkpoint.
func (s *Service) Checkpoint(ctx context.Context) (int64, error) {
	if !s.canCheckpoint() {
		return 0, errors.New("logcp: checkpointing needs DB, Table and Objects")
	}
	t0 := time.Now()
	data, slot, err := s.snapshot()
	if err == nil {
		err = s.publish(ctx, data, slot)
	}
	if err != nil {
		s.m.checkpointErr()
		return 0, err
	}
	s.ck.mu.Lock()
	s.ck.slot, s.ck.at = max(s.ck.slot, slot), s.env.Now()
	s.ck.mu.Unlock()
	s.m.checkpoint(slot, len(data), time.Since(t0))
	s.trimLog(ctx, slot)
	return slot, nil
}

// snapshot copies the applier at a consistent slot. The apply loop is held
// only until the store's view exists.
func (s *Service) snapshot() ([]byte, int64, error) {
	var buf bytes.Buffer
	var slot int64
	s.rep.applyMu.Lock()
	unlock := sync.OnceFunc(s.rep.applyMu.Unlock)
	defer unlock()
	err := s.app.SnapshotAt(&buf, func() int64 {
		slot = s.rep.appliedNow()
		unlock()
		return slot
	})
	return buf.Bytes(), slot, err
}

func ckptKey(slot int64) string { return ckptPrefix + slotKey(slot) }

// publish stores the object, then moves the pointer to it.
func (s *Service) publish(ctx context.Context, data []byte, slot int64) error {
	key := ckptKey(slot)
	if err := s.cfg.Objects.Put(ctx, key, data); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	_, err := s.cfg.DB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.cfg.Table,
		Item: map[string]types.AttributeValue{
			"pk": sv(ckptPK), "sk": sv("-"), "ck_slot": nv(slot), "ck_key": sv(key),
			"ck_size": nv(int64(len(data))), "ck_sum": sv(hex.EncodeToString(sum[:])), "ck_ts": nv(s.env.Now() / 1e6),
		},
		ConditionExpression:       aws.String("attribute_not_exists(ck_slot) OR ck_slot < :s"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":s": nv(slot)},
	})
	var cc *types.ConditionalCheckFailedException
	if errors.As(err, &cc) { // a checkpoint at this slot or newer exists; pruning removes our object if it is older
		s.pruneObjects(ctx)
		return nil
	}
	if err != nil {
		return err
	}
	s.pruneObjects(ctx)
	return nil
}

// pruneObjects deletes checkpoint objects older than the one the pointer names,
// keeping the newest ckptKeep-1 of them. It never touches the pointed object or
// anything newer (another node may be about to move the pointer to it).
func (s *Service) pruneObjects(ctx context.Context) {
	it, err := s.getPointer(ctx, ckptPK)
	keys, lerr := s.cfg.Objects.List(ctx, ckptPrefix)
	if err != nil || lerr != nil || it == nil {
		return
	}
	cur := strAttr(it, "ck_key")
	sort.Strings(keys)
	var older []string
	for _, k := range keys {
		if strings.HasPrefix(k, ckptPrefix) && k < cur {
			older = append(older, k)
		}
	}
	for len(older) > ckptKeep-1 {
		_ = s.cfg.Objects.Delete(ctx, older[0])
		older = older[1:]
	}
}

// trimLog drops log slots below the checkpoint minus KeepSlots.
func (s *Service) trimLog(ctx context.Context, ckptSlot int64) {
	below := ckptSlot - s.cfg.KeepSlots
	tr, ok := s.log.(Trimmer)
	if !s.cfg.TrimDelete || !ok || below <= 0 {
		return
	}
	n, err := tr.TrimBelow(ctx, below)
	s.m.trim(n)
	if err != nil {
		s.cfg.Logger.Warn("log: trim failed", "event", "log_trim_failed", "below", below, "err", err.Error())
	}
}

// restore loads the newest checkpoint into the (empty) applier and returns
// its slot (0: none). An unusable object falls back to replaying the whole
// log if it still starts at slot 0.
func (s *Service) restore(ctx context.Context) (int64, error) {
	if !s.canCheckpoint() {
		return 0, nil
	}
	it, err := s.getPointer(ctx, ckptPK)
	if err != nil || it == nil {
		return 0, err
	}
	data, err := s.fetchCheckpoint(ctx, it)
	if err != nil {
		return s.fromScratch(ctx, err)
	}
	slot, err := s.app.Restore(bytes.NewReader(data))
	if err != nil {
		return 0, fmt.Errorf("logcp: restoring checkpoint: %w", err)
	}
	if want := numAttr(it, "ck_slot"); slot != want {
		return 0, fmt.Errorf("logcp: checkpoint holds slot %d, pointer says %d", slot, want)
	}
	s.ck.slot = slot
	return slot, nil
}

// fetchCheckpoint reads and verifies the object the pointer names.
func (s *Service) fetchCheckpoint(ctx context.Context, it map[string]types.AttributeValue) ([]byte, error) {
	key, size := strAttr(it, "ck_key"), numAttr(it, "ck_size")
	data, err := s.cfg.Objects.Get(ctx, core.SegmentRef{Key: key, Off: 0, Len: size})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if int64(len(data)) != size || hex.EncodeToString(sum[:]) != strAttr(it, "ck_sum") {
		return nil, fmt.Errorf("logcp: checkpoint %s fails its checksum", key)
	}
	return data, nil
}

func (s *Service) fromScratch(ctx context.Context, cause error) (int64, error) {
	first, err := s.log.Read(ctx, 0, 1)
	if errors.Is(err, ErrTrimmed) {
		return 0, fmt.Errorf("%w: %v", ErrNoCheckpoint, cause)
	}
	if err != nil {
		return 0, err
	}
	if len(first) > 0 && first[0].N != 0 {
		return 0, fmt.Errorf("%w: %v", ErrNoCheckpoint, cause)
	}
	s.cfg.Logger.Warn("log: checkpoint unusable, replaying the whole log", "event", "log_ckpt_unusable", "err", cause.Error())
	return 0, nil
}

// ckptLoop runs periodic checkpoints on the active node and keeps the age and
// window gauges.
func (s *Service) ckptLoop(ctx context.Context) {
	tick := time.Second
	if s.cfg.CkptEvery > 0 && s.cfg.CkptEvery < tick {
		tick = s.cfg.CkptEvery
	}
	last := s.env.Now()
	for n := 0; s.env.sleep(ctx, tick) == nil; n++ {
		if n%15 == 0 {
			s.m.window(s.app.Window())
		}
		s.ck.mu.Lock()
		at, have := s.ck.at, s.ck.slot
		s.ck.mu.Unlock()
		if at > 0 {
			s.m.checkpointAge(s.since(at))
		}
		due := s.cfg.CkptEvery > 0 && s.canCheckpoint() && s.isActive() && s.since(last) >= s.cfg.CkptEvery
		if due && s.rep.appliedNow()-have >= s.cfg.CkptMinSlots {
			last = s.env.Now()
			if _, err := s.Checkpoint(ctx); err != nil {
				s.cfg.Logger.Warn("log: checkpoint failed", "event", "log_ckpt_failed", "err", err.Error())
			}
		}
	}
}
