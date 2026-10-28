package exec

import (
	"context"
	"time"

	"manifester/internal/controlplane/run"
	"manifester/internal/core"
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
// one IOResult per action to the InboxWriter.
type Executor struct {
	ctx  context.Context
	cfg  Config
	log  Log
	objs ObjectStore
	sink run.InboxWriter
}

var _ run.Executor = (*Executor)(nil)

// New builds an Executor. Cancelling ctx aborts calls in flight.
func New(ctx context.Context, cfg Config, log Log, objs ObjectStore, sink run.InboxWriter) *Executor {
	if objs == nil {
		objs = NewMemoryObjects()
	}
	return &Executor{ctx: ctx, cfg: cfg.withDefaults(), log: log, objs: objs, sink: sink}
}

func (e *Executor) deliver(c core.IOResult) {
	select {
	case <-e.sink.Closed():
		return
	default:
	}
	e.sink.DeliverControl(c)
}

// Execute never blocks: it starts a goroutine per action.
func (e *Executor) Execute(a core.IOCall) {
	go func() { e.deliver(e.run(a)) }()
}

func (e *Executor) run(a core.IOCall) core.IOResult {
	switch a := a.(type) {
	case core.LogAppend, core.LogRead, core.LogGetEnd, core.LogDeleteOld:
		return e.logIO(a)
	case core.ObjectPut, core.ObjectGet, core.ObjectDelete, core.ObjectList:
		return e.objIO(a)
	default:
		return core.LogAppendDone{Req: a.RequestID(), Err: core.ErrInvalid, Outcome: core.NotApplied}
	}
}

func (e *Executor) logIO(a core.IOCall) core.IOResult {
	ctx, cancel := context.WithTimeout(e.ctx, e.cfg.LogTimeout)
	defer cancel()
	switch a := a.(type) {
	case core.LogAppend:
		existing, taken, err := e.log.Append(ctx, a.Slot, a.Data)
		if err != nil {
			c, o := Classify(a, err)
			return core.LogAppendDone{Req: a.Req, Slot: a.Slot, Err: c, Outcome: o}
		}
		if taken {
			return core.LogAppendDone{
				Req: a.Req, Slot: a.Slot, Taken: true, Existing: existing,
				Err: core.OK, Outcome: core.Applied,
			}
		}
		return core.LogAppendDone{Req: a.Req, Slot: a.Slot, Err: core.OK, Outcome: core.Applied}
	case core.LogRead:
		ents, err := e.log.Read(ctx, a.From, int(a.Limit))
		if err != nil {
			c, o := Classify(a, err)
			return core.LogReadDone{Req: a.Req, Err: c, Outcome: o}
		}
		return core.LogReadDone{Req: a.Req, Entries: ents, Err: core.OK, Outcome: core.Applied}
	case core.LogGetEnd:
		tail, err := e.log.CheckTail(ctx)
		if err != nil {
			c, o := Classify(a, err)
			return core.LogGetEndDone{Req: a.Req, Err: c, Outcome: o}
		}
		return core.LogGetEndDone{Req: a.Req, Tail: tail, Err: core.OK, Outcome: core.Applied}
	case core.LogDeleteOld:
		err := e.log.TrimThrough(ctx, a.Through)
		if err != nil {
			c, o := Classify(a, err)
			return core.LogDeleteOldDone{Req: a.Req, Err: c, Outcome: o}
		}
		return core.LogDeleteOldDone{Req: a.Req, Err: core.OK, Outcome: core.Applied}
	default:
		return core.LogAppendDone{Req: a.RequestID(), Err: core.ErrInvalid, Outcome: core.NotApplied}
	}
}

func (e *Executor) objIO(a core.IOCall) core.IOResult {
	ctx, cancel := context.WithTimeout(e.ctx, e.cfg.ObjTimeout)
	defer cancel()
	switch a := a.(type) {
	case core.ObjectPut:
		err := e.objs.Put(ctx, a.Key, a.Data)
		c, o := Classify(a, err)
		return core.ObjectPutDone{Req: a.Req, Key: a.Key, Err: c, Outcome: o}
	case core.ObjectGet:
		return e.objGet(ctx, a)
	case core.ObjectDelete:
		err := e.objs.Delete(ctx, a.Key)
		c, o := Classify(a, err)
		return core.ObjectDeleteDone{Req: a.Req, Err: c, Outcome: o}
	case core.ObjectList:
		return e.objList(ctx, a)
	default:
		return core.ObjectGetDone{Req: a.RequestID(), Err: core.ErrInvalid}
	}
}

//nolint:dupl // ObjectGet/ObjectList share err-or-value shape with different fields
func (e *Executor) objGet(ctx context.Context, a core.ObjectGet) core.IOResult {
	data, err := e.objs.Get(ctx, a.Ref)
	if err != nil {
		c, _ := Classify(a, err)
		return core.ObjectGetDone{Req: a.Req, Err: c}
	}
	return core.ObjectGetDone{Req: a.Req, Data: data}
}

//nolint:dupl // ObjectGet/ObjectList share err-or-value shape with different fields
func (e *Executor) objList(ctx context.Context, a core.ObjectList) core.IOResult {
	keys, err := e.objs.List(ctx, a.Prefix)
	if err != nil {
		c, _ := Classify(a, err)
		return core.ObjectListDone{Req: a.Req, Err: c}
	}
	return core.ObjectListDone{Req: a.Req, Keys: keys}
}
