package logcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// writeHooks lets the caller pace and count BatchWriteItem retries. A nil
// *writeHooks sleeps for real and counts nothing.
type writeHooks struct {
	sleep       func(ctx context.Context, d time.Duration) error
	unprocessed func(n int)
	throttled   func()
}

func (h *writeHooks) pause(ctx context.Context, d time.Duration) error {
	if h == nil || h.sleep == nil {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}
	return h.sleep(ctx, d)
}

func (h *writeHooks) noteUnprocessed(n int) {
	if h != nil && h.unprocessed != nil {
		h.unprocessed(n)
	}
}

func (h *writeHooks) noteThrottle() {
	if h != nil && h.throttled != nil {
		h.throttled()
	}
}

const (
	maxBatchTries = 14
	backoffStart  = 20 * time.Millisecond
	backoffMax    = 2 * time.Second
)

func isThrottle(err error) bool {
	var a *types.ProvisionedThroughputExceededException
	var b *types.ThrottlingException
	var c *types.RequestLimitExceeded
	return errors.As(err, &a) || errors.As(err, &b) || errors.As(err, &c)
}

// writeAll sends up to 25 put/delete requests for table in one BatchWriteItem
// and resends UnprocessedItems and throttled calls with a doubling backoff
// until everything is written, ctx is done or maxBatchTries calls were needed.
func writeAll(ctx context.Context, db DB, table string, reqs []types.WriteRequest, h *writeHooks) error {
	backoff := backoffStart
	for try := 0; len(reqs) > 0; try++ {
		if try == maxBatchTries {
			return fmt.Errorf("logcp: batch write of %s: %d requests still unwritten after %d tries", table, len(reqs), try)
		}
		out, err := db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{table: reqs}})
		switch {
		case err == nil:
			reqs = out.UnprocessedItems[table]
			h.noteUnprocessed(len(reqs))
		case isThrottle(err):
			h.noteThrottle()
		default:
			return err
		}
		if len(reqs) == 0 {
			return nil
		}
		if err := h.pause(ctx, backoff); err != nil {
			return err
		}
		backoff = min(backoff*2, backoffMax)
	}
	return nil
}
