package executor

import (
	"context"
	"time"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// Config bounds each kind of I/O. Zero fields take the defaults.
type Config struct {
	LogTimeout time.Duration
	ObjTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.LogTimeout <= 0 {
		c.LogTimeout = 10 * time.Second
	}
	if c.ObjTimeout <= 0 {
		c.ObjTimeout = 30 * time.Second
	}
	return c
}

// Executor runs Log* and Obj* IOCalls on goroutines and delivers exactly
// one IOResult per action to the Sink.
type Executor struct {
	ctx    context.Context
	config Config
	log    Log
	objs   ObjectStore
	sink   Sink
}

// Sink takes the results. The run loop's inbox is the sink.
type Sink interface {
	// Deliver queues one result for the process, in the queue of class c. It must not block.
	Deliver(c machine.Class, body runtime.EventData)
	// Closed is closed when the process has ended; results are then dropped.
	Closed() <-chan struct{}
}

// New builds an Executor. Cancelling ctx aborts calls in flight.
func New(ctx context.Context, config Config, log Log, objs ObjectStore, sink Sink) *Executor {
	if objs == nil {
		objs = NewMemoryObjects()
	}
	return &Executor{ctx: ctx, config: config.withDefaults(), log: log, objs: objs, sink: sink}
}

func (e *Executor) deliver(c runtime.IOResult) {
	select {
	case <-e.sink.Closed():
		return
	default:
	}
	e.sink.Deliver(machine.Data, c)
}

// Execute never blocks: it starts a goroutine per action.
func (e *Executor) Execute(a runtime.IOCall) {
	go func() { e.deliver(e.run(a)) }()
}

func (e *Executor) run(a runtime.IOCall) runtime.IOResult {
	switch a := a.(type) {
	case runtime.LogAppend, runtime.LogRead, runtime.LogGetEnd, runtime.LogDeleteOld, runtime.LogClaimEpoch:
		return e.logIO(a)
	case runtime.ObjectPut, runtime.ObjectGet, runtime.ObjectDelete, runtime.ObjectList:
		return e.objIO(a)
	default:
		return runtime.LogAppendDone{Req: a.RequestID(), Err: runtime.ErrInvalid, Outcome: runtime.NotApplied}
	}
}

func (e *Executor) logIO(a runtime.IOCall) runtime.IOResult {
	ctx, cancel := context.WithTimeout(e.ctx, e.config.LogTimeout)
	defer cancel()
	switch a := a.(type) {
	case runtime.LogAppend:
		existing, taken, err := e.log.Append(ctx, a.Slot, a.Data)
		if err != nil {
			c, o := Classify(a, err)
			return runtime.LogAppendDone{Req: a.Req, Slot: a.Slot, Err: c, Outcome: o}
		}
		if taken && len(existing) == 0 {
			// At or below the floor: the put is not a commit. There is no
			// existing entry to report.
			return runtime.LogAppendDone{Req: a.Req, Slot: a.Slot, Taken: true, Err: runtime.ErrConflict, Outcome: runtime.NotApplied}
		}
		if taken {
			return runtime.LogAppendDone{
				Req: a.Req, Slot: a.Slot, Taken: true, Existing: existing,
				Err: runtime.OK, Outcome: runtime.Applied,
			}
		}
		return runtime.LogAppendDone{Req: a.Req, Slot: a.Slot, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogRead:
		ents, err := e.log.Read(ctx, a.From, int(a.Limit))
		if err != nil {
			c, o := Classify(a, err)
			return runtime.LogReadDone{Req: a.Req, Err: c, Outcome: o}
		}
		return runtime.LogReadDone{Req: a.Req, Entries: ents, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogGetEnd:
		tail, err := e.log.CheckTail(ctx)
		if err != nil {
			c, o := Classify(a, err)
			return runtime.LogGetEndDone{Req: a.Req, Err: c, Outcome: o}
		}
		return runtime.LogGetEndDone{Req: a.Req, Tail: tail, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogDeleteOld:
		err := e.log.TrimThrough(ctx, a.Through)
		if err != nil {
			c, o := Classify(a, err)
			return runtime.LogDeleteOldDone{Req: a.Req, Err: c, Outcome: o}
		}
		return runtime.LogDeleteOldDone{Req: a.Req, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogClaimEpoch:
		epoch, owner, won, err := e.log.ClaimEpoch(ctx, a.Expect, a.Owner)
		if err != nil {
			c, o := Classify(a, err)
			return runtime.LogClaimEpochDone{Req: a.Req, Err: c, Outcome: o}
		}
		return runtime.LogClaimEpochDone{Req: a.Req, Epoch: epoch, Owner: owner, Won: won, Err: runtime.OK, Outcome: runtime.Applied}
	default:
		return runtime.LogAppendDone{Req: a.RequestID(), Err: runtime.ErrInvalid, Outcome: runtime.NotApplied}
	}
}

func (e *Executor) objIO(a runtime.IOCall) runtime.IOResult {
	ctx, cancel := context.WithTimeout(e.ctx, e.config.ObjTimeout)
	defer cancel()
	switch a := a.(type) {
	case runtime.ObjectPut:
		err := e.objs.Put(ctx, a.Key, a.Data)
		c, o := Classify(a, err)
		return runtime.ObjectPutDone{Req: a.Req, Key: a.Key, Err: c, Outcome: o}
	case runtime.ObjectGet:
		return e.objGet(ctx, a)
	case runtime.ObjectDelete:
		err := e.objs.Delete(ctx, a.Key)
		c, o := Classify(a, err)
		return runtime.ObjectDeleteDone{Req: a.Req, Err: c, Outcome: o}
	case runtime.ObjectList:
		return e.objList(ctx, a)
	default:
		return runtime.ObjectGetDone{Req: a.RequestID(), Err: runtime.ErrInvalid}
	}
}

//nolint:dupl // ObjectGet/ObjectList share err-or-value shape with different fields
func (e *Executor) objGet(ctx context.Context, a runtime.ObjectGet) runtime.IOResult {
	data, err := e.objs.Get(ctx, a.Ref)
	if err != nil {
		c, _ := Classify(a, err)
		return runtime.ObjectGetDone{Req: a.Req, Err: c}
	}
	return runtime.ObjectGetDone{Req: a.Req, Data: data}
}

//nolint:dupl // ObjectGet/ObjectList share err-or-value shape with different fields
func (e *Executor) objList(ctx context.Context, a runtime.ObjectList) runtime.IOResult {
	keys, err := e.objs.List(ctx, a.Prefix)
	if err != nil {
		c, _ := Classify(a, err)
		return runtime.ObjectListDone{Req: a.Req, Err: c}
	}
	return runtime.ObjectListDone{Req: a.Req, Keys: keys}
}
