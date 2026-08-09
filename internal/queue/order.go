package queue

import "container/heap"

// Key is the total order over ready messages. Everything about how a queue
// behaves is in these two fields and the comparison below.
type Key struct {
	Priority int    // higher wins
	Seq      uint64 // monotonic enqueue counter
}

// less reports whether a should be delivered before b.
//
// This is the whole ordering design. Priority queues fall out of the first
// comparison; when priority is disabled every message carries priority 0 and
// the comparison falls through to the sequence number. LIFO is FIFO with the
// sequence comparator inverted. There is no separate LIFO code path, no
// separate priority code path, and no separate delay code path: delay only
// decides *when* a message enters this order, not how it is ordered once it
// is in.
func less(a, b Key, lifo bool) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if lifo {
		return a.Seq > b.Seq
	}
	return a.Seq < b.Seq
}

// readyHeap is the set of messages that are visible and unleased.
type readyHeap struct {
	msgs []*Message
	lifo bool
}

func (h *readyHeap) Len() int           { return len(h.msgs) }
func (h *readyHeap) Less(i, j int) bool { return less(h.msgs[i].key(), h.msgs[j].key(), h.lifo) }
func (h *readyHeap) Swap(i, j int)      { h.msgs[i], h.msgs[j] = h.msgs[j], h.msgs[i] }

func (h *readyHeap) Push(x any) { h.msgs = append(h.msgs, x.(*Message)) }

func (h *readyHeap) Pop() any {
	old := h.msgs
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	h.msgs = old[:n-1]
	return m
}

func (h *readyHeap) push(m *Message) { heap.Push(h, m) }

func (h *readyHeap) pop() *Message {
	if len(h.msgs) == 0 {
		return nil
	}
	return heap.Pop(h).(*Message)
}

func (h *readyHeap) peek() *Message {
	if len(h.msgs) == 0 {
		return nil
	}
	return h.msgs[0]
}

// delayedHeap orders invisible messages by the time they become visible. Ties
// break on the ready order so that two messages with the same visible_at are
// promoted in the order they will be delivered, which keeps the fold
// deterministic.
type delayedHeap struct {
	msgs []*Message
	lifo bool
}

func (h *delayedHeap) Len() int { return len(h.msgs) }

func (h *delayedHeap) Less(i, j int) bool {
	if h.msgs[i].VisibleAt != h.msgs[j].VisibleAt {
		return h.msgs[i].VisibleAt < h.msgs[j].VisibleAt
	}
	return less(h.msgs[i].key(), h.msgs[j].key(), h.lifo)
}

func (h *delayedHeap) Swap(i, j int) { h.msgs[i], h.msgs[j] = h.msgs[j], h.msgs[i] }

func (h *delayedHeap) Push(x any) { h.msgs = append(h.msgs, x.(*Message)) }

func (h *delayedHeap) Pop() any {
	old := h.msgs
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	h.msgs = old[:n-1]
	return m
}

func (h *delayedHeap) push(m *Message) { heap.Push(h, m) }

func (h *delayedHeap) pop() *Message { return heap.Pop(h).(*Message) }

func (h *delayedHeap) peek() *Message {
	if len(h.msgs) == 0 {
		return nil
	}
	return h.msgs[0]
}

// leaseHeap orders live leases by expiry so the timer only ever has to look at
// one of them. Cancelled leases are not removed from the heap; they are
// skipped on pop by comparing the token against the message's current lease,
// which keeps ack off the O(n) path.
type leaseHeap struct {
	entries []leaseEntry
}

type leaseEntry struct {
	id        string
	token     string
	expiresAt int64
}

func (h *leaseHeap) Len() int           { return len(h.entries) }
func (h *leaseHeap) Less(i, j int) bool { return h.entries[i].expiresAt < h.entries[j].expiresAt }
func (h *leaseHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *leaseHeap) Push(x any)         { h.entries = append(h.entries, x.(leaseEntry)) }
func (h *leaseHeap) push(e leaseEntry)  { heap.Push(h, e) }
func (h *leaseHeap) pop() leaseEntry    { return heap.Pop(h).(leaseEntry) }
func (h *leaseHeap) peek() *leaseEntry  { return &h.entries[0] }

func (h *leaseHeap) Pop() any {
	old := h.entries
	n := len(old)
	e := old[n-1]
	h.entries = old[:n-1]
	return e
}
