package production

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/system"
	"github.com/manifesterio/mds/shared/objectstore"
)

// AsyncConnections is the connection set the Executor writes to: system.Connections plus a
// write that reports when the bytes left (Net implements it).
type AsyncConnections interface {
	system.Connections
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

// Executor is the production system.Executor: each IOCall runs on its own
// goroutine under a timeout and yields exactly one IOResult on the InboxWriter.
// Sends go through the connection's ordered writer queue instead.
type Executor struct {
	ctx    context.Context
	config ExecConfig
	objs   system.ObjectStore
	cp     system.ControlPlane
	conns  AsyncConnections
	class  system.ErrorClassifier
	sink   system.InboxWriter
}

var _ system.Executor = (*Executor)(nil)

// NewExecutor builds an Executor; cancelling ctx aborts calls in flight.
func NewExecutor(ctx context.Context, config ExecConfig, objs system.ObjectStore, cp system.ControlPlane,
	conns AsyncConnections, class system.ErrorClassifier, sink system.InboxWriter) *Executor {
	return &Executor{ctx: ctx, config: config.withDefaults(), objs: objs, cp: cp, conns: conns, class: class, sink: sink}
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
	case core.ObjectCall:
		go func() { e.deliver(core.ObjectDone{Result: e.object(a)}) }()
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
	ne, isNet := errors.AsType[net.Error](err)
	switch {
	case err == nil:
		return core.OK
	case isNet && ne.Timeout():
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

func (e *Executor) object(a core.ObjectCall) objectstore.Result {
	var n int64
	switch c := a.Call.(type) {
	case objectstore.Put:
		n = int64(len(c.Data))
	case objectstore.Get:
		n = c.Ref.Len
	case objectstore.Delete, objectstore.List:
	}
	ctx, cancel := context.WithTimeout(e.ctx, e.config.objTimeout(n))
	defer cancel()
	// The classifier gets the whole action, so it routes by action type.
	fail := func(err error) (core.ErrCode, core.Outcome) { return e.class.Classify(a, err) }
	switch c := a.Call.(type) {
	case objectstore.Put:
		code, outcome := mutateOutcome(e.objs.Put(ctx, c.Key, c.Data), fail)
		return objectstore.PutDone{Req: c.Req, Key: c.Key, Err: code, Outcome: outcome}
	case objectstore.Get:
		data, err := e.objs.Get(ctx, c.Ref)
		if code, bad := failCode(err, fail); bad {
			return objectstore.GetDone{Req: c.Req, Err: code}
		}
		return objectstore.GetDone{Req: c.Req, Data: data}
	case objectstore.Delete:
		code, outcome := mutateOutcome(e.objs.Delete(ctx, c.Key), fail)
		return objectstore.DeleteDone{Req: c.Req, Err: code, Outcome: outcome}
	case objectstore.List:
		keys, err := e.objs.List(ctx, c.Prefix)
		code, bad := failCode(err, fail)
		if !bad {
			return objectstore.ListDone{Req: c.Req, Keys: keys}
		}
		return objectstore.ListDone{Req: c.Req, Err: code}
	}
	//nolint:forbidigo // impossible: Call is a sealed sum and the switch lists every case
	panic("production: unknown object store call")
}

func (e *Executor) control(a core.ControlPlaneCall) core.IOResult {
	limit := e.config.CPTimeout
	if a.Timeout > 0 { // group joins and syncs wait for a rebalance (core.ControlPlaneCall)
		limit = time.Duration(a.Timeout)
	}
	ctx, cancel := context.WithTimeout(e.ctx, limit)
	defer cancel()
	response, err := e.cp.Call(ctx, a.Op)
	if err != nil {
		c, o := e.class.Classify(a, err)
		return core.ControlPlaneDone{Req: a.Req, Err: c, Outcome: o}
	}
	return core.ControlPlaneDone{Req: a.Req, Resp: response, Outcome: core.Applied}
}
