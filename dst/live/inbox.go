package live

import (
	"sync"

	"github.com/manifesterio/mds/dst/machine"
)

// Inbox holds the inputs for one machine. Senders can be any goroutine.
//
// It has two queues. Control inputs are taken before Data inputs. Control never blocks a
// sender. Data may be bounded: NewInbox(dataCap) with dataCap above zero makes Deliver
// block while dataCap Data inputs wait, so a slow machine slows its senders down. Inside
// one queue, inputs come out in the order they went in.
//
// After Close, waiting inputs are dropped, new inputs are refused, and blocked senders
// are released.
type Inbox[In machine.Message] struct {
	mu      sync.Mutex
	control []In
	data    []In
	slots   chan struct{} // one slot per waiting Data input; nil when Data has no bound
	wake    chan struct{} // capacity 1: something was put in
	done    chan struct{}
	once    sync.Once
}

// NewInbox returns an empty inbox. A dataCap of zero or less means that the Data queue
// has no bound.
func NewInbox[In machine.Message](dataCap int) *Inbox[In] {
	i := &Inbox[In]{wake: make(chan struct{}, 1), done: make(chan struct{})}
	if dataCap > 0 {
		i.slots = make(chan struct{}, dataCap)
	}
	return i
}

func (i *Inbox[In]) closed() bool {
	select {
	case <-i.done:
		return true
	default:
		return false
	}
}

func (i *Inbox[In]) notify() {
	select {
	case i.wake <- struct{}{}:
	default:
	}
}

// Deliver queues msg in the queue of class c. It is dropped after Close. A Data input
// waits here while the queue is full.
func (i *Inbox[In]) Deliver(c machine.Class, msg In) {
	if c == machine.Control {
		i.push(&i.control, msg)
		return
	}
	if i.slots != nil {
		select {
		case i.slots <- struct{}{}:
		case <-i.done:
			return
		}
	}
	i.push(&i.data, msg)
}

func (i *Inbox[In]) push(queue *[]In, msg In) {
	i.mu.Lock()
	if i.closed() {
		i.mu.Unlock()
		return
	}
	*queue = append(*queue, msg)
	i.mu.Unlock()
	i.notify()
}

// Len is the number of inputs that wait, in both queues.
func (i *Inbox[In]) Len() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.control) + len(i.data)
}

// Pop takes the next input: Control first, then Data.
func (i *Inbox[In]) Pop() (In, bool) {
	var none In
	i.mu.Lock()
	defer i.mu.Unlock()
	switch {
	case len(i.control) > 0:
		msg := i.control[0]
		i.control[0] = none
		i.control = i.control[1:]
		return msg, true
	case len(i.data) > 0:
		msg := i.data[0]
		i.data[0] = none
		i.data = i.data[1:]
		if i.slots != nil {
			select {
			case <-i.slots:
			default:
			}
		}
		return msg, true
	}
	return none, false
}

// Closed is closed after Close.
func (i *Inbox[In]) Closed() <-chan struct{} { return i.done }

// Close drops all waiting inputs and refuses new ones. Run calls it when it ends.
func (i *Inbox[In]) Close() {
	i.once.Do(func() {
		i.mu.Lock()
		close(i.done)
		i.control, i.data = nil, nil
		i.mu.Unlock()
	})
}
