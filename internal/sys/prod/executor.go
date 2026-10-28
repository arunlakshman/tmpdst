package prod

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"manifester/internal/core"
	"manifester/internal/sys"
)

// AsyncConns is the connection set the Executor writes to: sys.Conns plus a
// write that reports when the bytes left (Net implements it).
type AsyncConns interface {
	sys.Conns
	WriteAsync(conn core.ConnID, data []byte, done func(error)) error
}

// ExecConfig bounds each kind of I/O. Zero fields take the defaults.
type ExecConfig struct {
	// ObjTimeout is the base budget of an object-store call (30s). A PUT or GET
	// of a large body gets ObjTransferRate-worth of extra time on top, so a
	// 16 MiB segment on a slow link is not cut off by the base alone.
	ObjTimeout      time.Duration
	ObjTransferRate int64         // assumed minimum bytes/s for the extra time (2 MiB/s)
	CPTimeout       time.Duration // per control-plane call (10s)
}

// objTimeout is the budget of a call that moves n bytes: the base plus n at
// the assumed minimum transfer rate.
func (c ExecConfig) objTimeout(n int64) time.Duration {
	return c.ObjTimeout + time.Duration(n)*time.Second/time.Duration(c.ObjTransferRate)
}

func (c ExecConfig) withDefaults() ExecConfig {
	if c.ObjTimeout <= 0 {
		c.ObjTimeout = 30 * time.Second
	}
	if c.ObjTransferRate <= 0 {
		c.ObjTransferRate = 2 << 20
	}
	if c.CPTimeout <= 0 {
		c.CPTimeout = 10 * time.Second
	}
	return c
}

// Executor is the production sys.Executor: each IOCall runs on its own
// goroutine under a timeout and yields exactly one IOResult on the Sink.
// Sends go through the connection's ordered writer queue instead.
type Executor struct {
	ctx   context.Context
	cfg   ExecConfig
	objs  sys.ObjectStore
	cp    sys.ControlPlane
	conns AsyncConns
	class sys.Classifier
	sink  sys.Sink
}

var _ sys.Executor = (*Executor)(nil)

// NewExecutor builds an Executor; cancelling ctx aborts calls in flight.
func NewExecutor(ctx context.Context, cfg ExecConfig, objs sys.ObjectStore, cp sys.ControlPlane,
	conns AsyncConns, class sys.Classifier, sink sys.Sink) *Executor {
	return &Executor{ctx: ctx, cfg: cfg.withDefaults(), objs: objs, cp: cp, conns: conns, class: class, sink: sink}
}

func (e *Executor) deliver(c core.IOResult) {
	select {
	case <-e.sink.Closed():
		return
	default:
	}
	e.sink.DeliverControl(c)
}

// Execute never blocks: it queues a write or starts a goroutine.
func (e *Executor) Execute(a core.IOCall) {
	switch a := a.(type) {
	case core.Send:
		e.send(a)
	case core.Connect:
		go e.deliver(core.ConnectDone{Req: a.Req, Err: core.ErrInvalid}) // brokers reach the CP through ControlPlaneCall
	case core.ObjectPut, core.ObjectGet, core.ObjectDelete, core.ObjectList:
		go func() { e.deliver(e.object(a)) }()
	case core.ControlPlaneCall:
		go func() { e.deliver(e.control(a)) }()
	default:
		//nolint:forbidigo // impossible: machine emits only known IOCall types
		panic(fmt.Sprintf("prod: unknown IOCall %T", a))
	}
}

func (e *Executor) send(a core.Send) {
	done := func(err error) { e.deliver(core.SendDone{Req: a.Req, Conn: a.Conn, Err: sendErr(err)}) }
	if err := e.conns.WriteAsync(a.Conn, a.Data, done); err != nil {
		go done(err)
	}
}

func sendErr(err error) core.ErrCode {
	var ne net.Error
	switch {
	case err == nil:
		return core.OK
	case errors.As(err, &ne) && ne.Timeout():
		return core.ErrTimeout
	}
	return core.ErrConnClosed
}

func mutateOutcome(err error, fail func(error) (core.ErrCode, core.Outcome)) (core.ErrCode, core.Outcome) {
	if err != nil {
		return fail(err)
	}
	return core.OK, core.Applied
}

// failCode classifies err. The bool is false when err is nil.
func failCode(err error, fail func(error) (core.ErrCode, core.Outcome)) (core.ErrCode, bool) {
	if err == nil {
		return core.OK, false
	}
	c, _ := fail(err)
	return c, true
}

func (e *Executor) object(a core.IOCall) core.IOResult {
	var n int64
	switch a := a.(type) {
	case core.ObjectPut:
		n = int64(len(a.Data))
	case core.ObjectGet:
		n = a.Ref.Len
	}
	ctx, cancel := context.WithTimeout(e.ctx, e.cfg.objTimeout(n))
	defer cancel()
	fail := func(err error) (core.ErrCode, core.Outcome) { return e.class.Classify(a, err) }
	switch a := a.(type) {
	case core.ObjectPut:
		c, o := mutateOutcome(e.objs.Put(ctx, a.Key, a.Data), fail)
		return core.ObjectPutDone{Req: a.Req, Key: a.Key, Err: c, Outcome: o}
	case core.ObjectGet:
		data, err := e.objs.Get(ctx, a.Ref)
		if c, bad := failCode(err, fail); bad {
			return core.ObjectGetDone{Req: a.Req, Err: c}
		}
		return core.ObjectGetDone{Req: a.Req, Data: data}
	case core.ObjectDelete:
		c, o := mutateOutcome(e.objs.Delete(ctx, a.Key), fail)
		return core.ObjectDeleteDone{Req: a.Req, Err: c, Outcome: o}
	case core.ObjectList:
		keys, err := e.objs.List(ctx, a.Prefix)
		c, bad := failCode(err, fail)
		if !bad {
			return core.ObjectListDone{Req: a.Req, Keys: keys}
		}
		return core.ObjectListDone{Req: a.Req, Err: c}
	default:
		return core.ObjectPutDone{Req: a.ReqID(), Err: core.ErrInvalid}
	}
}

func (e *Executor) control(a core.ControlPlaneCall) core.IOResult {
	ctx, cancel := context.WithTimeout(e.ctx, e.cfg.CPTimeout)
	defer cancel()
	resp, err := e.cp.Call(ctx, a.Op)
	if err != nil {
		c, o := e.class.Classify(a, err)
		return core.ControlPlaneDone{Req: a.Req, Err: c, Outcome: o}
	}
	return core.ControlPlaneDone{Req: a.Req, Resp: resp, Outcome: core.Applied}
}
