package live

import "sync"

// Inbox holds the values for the process. Senders never block. Urgent values
// (the stop request) are taken before the others, so a long queue can not
// delay a shutdown. Other values are taken in the order they came.
type Inbox struct {
	mu     sync.Mutex
	urgent []any
	queue  []any
	wake   chan struct{}
	done   chan struct{}
	once   sync.Once
}

// NewInbox returns an empty inbox.
func NewInbox() *Inbox {
	return &Inbox{wake: make(chan struct{}, 1), done: make(chan struct{})}
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

// Deliver queues body. It is dropped after Close.
func (i *Inbox) Deliver(body any) { i.push(body, false) }

// DeliverUrgent queues body before every value that Deliver queued.
func (i *Inbox) DeliverUrgent(body any) { i.push(body, true) }

func (i *Inbox) push(body any, urgent bool) {
	i.mu.Lock()
	if i.closed() {
		i.mu.Unlock()
		return
	}
	if urgent {
		i.urgent = append(i.urgent, body)
	} else {
		i.queue = append(i.queue, body)
	}
	i.mu.Unlock()
	i.notify()
}

// Len is the number of values that wait.
func (i *Inbox) Len() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.urgent) + len(i.queue)
}

// Pop takes the next value: urgent first.
func (i *Inbox) Pop() (any, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	switch {
	case len(i.urgent) > 0:
		b := i.urgent[0]
		i.urgent[0] = nil
		i.urgent = i.urgent[1:]
		return b, true
	case len(i.queue) > 0:
		b := i.queue[0]
		i.queue[0] = nil
		i.queue = i.queue[1:]
		return b, true
	}
	return nil, false
}

// Closed is closed after Close.
func (i *Inbox) Closed() <-chan struct{} { return i.done }

// Close drops all waiting values and refuses new ones. Run calls it when it ends.
func (i *Inbox) Close() {
	i.once.Do(func() {
		i.mu.Lock()
		close(i.done)
		i.urgent, i.queue = nil, nil
		i.mu.Unlock()
	})
}
