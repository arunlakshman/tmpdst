package logsm

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/core"
	"manifester/cp/fakedb"
)

// The commit-path model of docs/commit-path-redesign.md section 6, extended
// with idea A. 6 brokers seal 150-partition segments open loop at the offered
// rate with random phases, each broker commits one segment at a time in seal
// order (L 5.8). Modes:
//
//	L          one sequencer loop, one transaction in flight, group commit,
//	           tx time = base + perItem x (k+1) (a sleep: the prior model,
//	           not fakedb), the commit path of design L.
//	A pref K   all brokers use ONE replica, K appends in flight (no slot
//	           contention).
//	A spread K 3 replicas with 2 brokers each, K appends in flight each,
//	           all appending to one log (slot contention).
//
// The log is the real DynLog over fakedb with latency injected per call.
// PutItem latency is an ASSUMPTION until X1 runs on AWS.
//
//	LOGSM_MODEL=1 go test ./cp/logsm -run TestModel -v -timeout 30m

type latModel struct {
	name           string
	putLo, putHi   time.Duration // PutItem
	read           time.Duration // Query
	txBase, txItem time.Duration // TransactWriteItems, design L
}

var latModels = []latModel{
	{"mild: Put 5-8 ms, read 4 ms, L tx 8+0.7/item", 5 * time.Millisecond, 8 * time.Millisecond, 4 * time.Millisecond, 8 * time.Millisecond, 700 * time.Microsecond},
	{"pessimistic: Put 15 ms, read 8 ms, L tx 15+1.5/item", 14 * time.Millisecond, 16 * time.Millisecond, 8 * time.Millisecond, 15 * time.Millisecond, 1500 * time.Microsecond},
}

type row struct {
	committed        float64 // segments per second
	p50, p99, avg    time.Duration
	qAvg, qP99       time.Duration
	lostPerCommit    float64
	putsPerCommit    float64
	queriesPerCommit float64
}

type committerFn func(ctx context.Context, broker int, e Entry) error

func (m latModel) fake() (*fakedb.Fake, *atomic.Int64, *atomic.Int64) {
	db := fakedb.New()
	var puts, queries atomic.Int64
	db.Latency = func(call string) time.Duration {
		switch call {
		case "PutItem":
			puts.Add(1)
			return m.putLo + rand.N(m.putHi-m.putLo+1)
		case "Query":
			queries.Add(1)
			return m.read
		}
		return 0
	}
	return db, &puts, &queries
}

// modeL returns the design L commit function (and stops its loop with ctx).
func (m latModel) modeL(ctx context.Context) committerFn {
	type req struct{ done chan struct{} }
	reqs := make(chan *req, 4096)
	go func() {
		for {
			var group []*req
			select {
			case <-ctx.Done():
				return
			case r := <-reqs:
				group = append(group, r)
			}
		drain:
			for len(group) < 98 {
				select {
				case r := <-reqs:
					group = append(group, r)
				default:
					break drain
				}
			}
			time.Sleep(m.txBase + time.Duration(len(group)+1)*m.txItem)
			for _, r := range group {
				close(r.done)
			}
		}
	}()
	return func(ctx context.Context, _ int, _ Entry) error {
		r := &req{done: make(chan struct{})}
		reqs <- r
		select {
		case <-r.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// modeA returns the idea A commit function over replicas sharing one fakedb log.
func (m latModel) modeA(ctx context.Context, db *fakedb.Fake, nrep, k int) (committerFn, func() int64) {
	if _, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("log")}); err != nil {
		panic(err)
	}
	var cs []*Committer
	var rs []*Replica
	for i := range nrep {
		r := NewReplica(Config{Log: NewDynLog(db, "log"), Writer: fmt.Sprint("r", i), HoleAfter: int64(250 * time.Millisecond), TailPoll: int64(10 * time.Millisecond)})
		rs = append(rs, r)
		cs = append(cs, NewCommitter(r, k))
		go r.Run(ctx)
	}
	lost := func() int64 {
		var n int64
		for _, r := range rs {
			n += r.Stats().Lost
		}
		return n
	}
	return func(ctx context.Context, broker int, e Entry) error {
		d, err := cs[broker%nrep].Do(ctx, e)
		if err == nil && d.Err != core.OK {
			err = fmt.Errorf("answer %v", d.Err)
		}
		return err
	}, lost
}

// sample is one finished commit.
type sample struct{ queue, commit time.Duration }

// runBrokers drives 6 brokers at the offered total rate for warm+dur. Brokers
// stop submitting at the end of the window; throughput and latency are those
// of commits that COMPLETED inside [warm, warm+dur], so a saturated mode
// cannot hide its backlog. total counts every completed commit.
func runBrokers(commit committerFn, rate float64, warm, dur time.Duration) ([]sample, int, int) {
	const brokers = 6
	period := time.Duration(float64(time.Second) * brokers / rate)
	start := time.Now()
	from, end := start.Add(warm), start.Add(warm+dur)
	var in []sample
	var total, failed int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for b := range brokers {
		wg.Go(func() {
			s := newSynth(uint64(100+b), 288, 150)
			for next := start.Add(rand.N(period)); next.Before(end); next = next.Add(period) {
				time.Sleep(time.Until(next))
				if time.Now().After(end) {
					return
				}
				submit := time.Now()
				err := commit(context.Background(), b, s.command())
				done := time.Now()
				mu.Lock()
				switch {
				case err != nil:
					failed++
				default:
					total++
					if done.After(from) && done.Before(end) {
						in = append(in, sample{queue: submit.Sub(next), commit: done.Sub(submit)})
					}
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return in, total, failed
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[min(len(s)-1, int(float64(len(s))*p))]
}

func mean(ds []time.Duration) time.Duration {
	var m time.Duration
	for _, d := range ds {
		m += d
	}
	if len(ds) == 0 {
		return 0
	}
	return m / time.Duration(len(ds))
}

func (m latModel) measure(mode string, k int, rate float64, warm, dur time.Duration) (row, int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, puts, queries := m.fake()
	var commit committerFn
	lost := func() int64 { return 0 }
	switch mode {
	case "L":
		commit = m.modeL(ctx)
	case "A pref":
		commit, lost = m.modeA(ctx, db, 1, k)
	default:
		commit, lost = m.modeA(ctx, db, 3, k)
	}
	in, total, failed := runBrokers(commit, rate, warm, dur)
	var commits, queues []time.Duration
	for _, s := range in {
		commits, queues = append(commits, s.commit), append(queues, s.queue)
	}
	n := float64(max(total, 1))
	return row{
		committed: float64(len(in)) / dur.Seconds(), p50: pct(commits, .5), p99: pct(commits, .99), avg: mean(commits),
		qAvg: mean(queues), qP99: pct(queues, .99),
		lostPerCommit: float64(lost()) / n, putsPerCommit: float64(puts.Load()) / n, queriesPerCommit: float64(queries.Load()) / n,
	}, failed
}

func TestModel(t *testing.T) {
	if os.Getenv("LOGSM_MODEL") == "" {
		t.Skip("set LOGSM_MODEL=1 (takes minutes: real sleeps)")
	}
	warm, dur := 2*time.Second, 8*time.Second
	type cfg struct {
		mode string
		k    int
	}
	cfgs := []cfg{{"L", 0}, {"A pref", 1}, {"A pref", 4}, {"A pref", 16}, {"A spread", 1}, {"A spread", 4}, {"A spread", 16}}
	for _, m := range latModels {
		fmt.Printf("\n%s\n| mode | K | offered seg/s | committed seg/s | commit avg / p50 / p99 ms | broker queue avg / p99 ms | lost slots per commit | PutItem per commit | Query per commit |\n|---|---|---|---|---|---|---|---|---|\n", m.name)
		for _, rate := range []float64{64, 256} {
			for _, c := range cfgs {
				r, failed := m.measure(c.mode, c.k, rate, warm, dur)
				fmt.Printf("| %s | %d | %.0f | %.1f | %.0f / %.0f / %.0f | %.0f / %.0f | %.2f | %.2f | %.2f |",
					c.mode, c.k, rate, r.committed, ms(r.avg), ms(r.p50), ms(r.p99), ms(r.qAvg), ms(r.qP99), r.lostPerCommit, r.putsPerCommit, r.queriesPerCommit)
				if failed > 0 {
					fmt.Printf(" FAILED %d", failed)
				}
				fmt.Println()
			}
		}
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
