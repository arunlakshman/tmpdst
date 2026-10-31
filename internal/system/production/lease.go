package production

import (
	"context"
	"log/slog"
	"time"

	"mds/internal/core"
	"mds/internal/system"
)

// LeaseClient is the control plane call behind a broker's heartbeat (Q4). It
// renews the lease of self and returns the live broker set.
type LeaseClient interface {
	BrokerHeartbeat(ctx context.Context, self core.BrokerAddr) ([]core.BrokerAddr, error)
}

// staleBeats is how many missed heartbeats make the last live set stale. The
// control plane drops a broker after 3 intervals, so an older set may name
// brokers that are gone.
const staleBeats = 3

// leaseLoop beats every interval and gives each live set to the broker as a
// LiveBrokers event. When no beat has worked for staleBeats intervals, it
// sends an empty set, so Metadata answers with the broker itself only. It runs
// until ctx is done.
func leaseLoop(ctx context.Context, in system.InboxWriter, lease LeaseClient, self core.BrokerAddr,
	every time.Duration, log *slog.Logger) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	var last time.Time // when the last beat worked
	stale, failing := true, false
	for {
		cctx, cancel := context.WithTimeout(ctx, every)
		live, err := lease.BrokerHeartbeat(cctx, self)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			if failing {
				log.Info("lease: heartbeat works again")
			}
			failing, stale, last = false, false, time.Now()
			in.DeliverControl(core.LiveBrokers{Brokers: live})
		} else {
			if !failing {
				log.Warn("lease: heartbeat failed", "err", err)
			}
			failing = true
			if !stale && time.Since(last) > staleBeats*every {
				stale = true
				in.DeliverControl(core.LiveBrokers{})
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
