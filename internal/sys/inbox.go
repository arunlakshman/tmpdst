package sys

import (
	"sync"

	"manifester/internal/core"
)

// Inbox is the Sink: a control queue that never blocks its senders (bounded
// by construction: the machine caps in-flight work) and a data queue bounded
// at dataCap that blocks when full. Both drop after Close.
type Inbox struct {
	mu      sync.Mutex
	control []core.EventData
	data    []core.EventData
	slots   chan struct{} // data capacity semaphore
	wake    chan struct{} // cap 1: something was enqueued
	done    chan struct{}
	once    sync.Once
}

var _ Sink = (*Inbox)(nil)

// NewInbox returns an Inbox whose data queue holds at most dataCap events.
func NewInbox(dataCap int) *Inbox {
	if dataCap < 1 {
		dataCap = 1
	}
	return &Inbox{
		slots: make(chan struct{}, dataCap),
		wake:  make(chan struct{}, 1),
		done:  make(chan struct{}),
	}
}

func (i *Inbox) closed() bool {
	select {
	case <-i.done:
		return true
	default:
		return false
	}
}

func (i *Inbox) notify() {
	select {
	case i.wake <- struct{}{}:
	default:
	}
}

// DeliverControl enqueues a control event; control events are never dropped.
func (i *Inbox) DeliverControl(b core.EventData) {
	i.mu.Lock()
	if i.closed() {
		i.mu.Unlock()
		return
	}
	i.control = append(i.control, b)
	i.mu.Unlock()
	i.notify()
}

// DeliverData enqueues a data event, blocking or backpressuring per the data capacity.
func (i *Inbox) DeliverData(b core.EventData) {
	if i.closed() {
		return
	}
	select {
	case i.slots <- struct{}{}:
	case <-i.done:
		return
	}
	i.mu.Lock()
	if i.closed() {
		i.mu.Unlock()
		return
	}
	i.data = append(i.data, b)
	i.mu.Unlock()
	i.notify()
}

// Closed returns a channel that is closed when the inbox is closed.
func (i *Inbox) Closed() <-chan struct{} { return i.done }

// Close ends the incarnation: pending and later deliveries are dropped and
// blocked data senders are released.
func (i *Inbox) Close() {
	i.once.Do(func() {
		i.mu.Lock()
		close(i.done)
		i.control, i.data = nil, nil
		i.mu.Unlock()
	})
}

// Control and Data are the two queues as core.Queue views for the policy.
func (i *Inbox) Control() core.Queue { return ctrlQ{i} }

// Data returns the data queue view of the inbox.
func (i *Inbox) Data() core.Queue { return dataQ{i} }

type ctrlQ struct{ i *Inbox }

func (q ctrlQ) Len() int {
	q.i.mu.Lock()
	defer q.i.mu.Unlock()
	return len(q.i.control)
}

func (q ctrlQ) Pop() (core.EventData, bool) {
	q.i.mu.Lock()
	defer q.i.mu.Unlock()
	if len(q.i.control) == 0 {
		return nil, false
	}
	b := q.i.control[0]
	q.i.control[0] = nil
	q.i.control = q.i.control[1:]
	return b, true
}

type dataQ struct{ i *Inbox }

func (q dataQ) Len() int {
	q.i.mu.Lock()
	defer q.i.mu.Unlock()
	return len(q.i.data)
}

func (q dataQ) Pop() (core.EventData, bool) {
	q.i.mu.Lock()
	if len(q.i.data) == 0 {
		q.i.mu.Unlock()
		return nil, false
	}
	b := q.i.data[0]
	q.i.data[0] = nil
	q.i.data = q.i.data[1:]
	q.i.mu.Unlock()
	select {
	case <-q.i.slots:
	default:
	}
	return b, true
}
