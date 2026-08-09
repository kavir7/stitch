package queue

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/kavir7/stitch/internal/wal"
)

// pollInterval is how often a waiting dequeue re-checks the queue. The plan
// picks sleep-and-retry over a condition variable deliberately: a consumer
// that waits 5ms longer than strictly necessary costs nothing here, and the
// wakeup bookkeeping a condvar needs is where subtle bugs live.
const pollInterval = 5 * time.Millisecond

// Queue is one queue: its config, its ordering structures and its leases.
// Every queue has its own mutex, so two queues never contend.
type Queue struct {
	cfg    Config
	log    *wal.Log
	now    func() time.Time
	logger *log.Logger

	mu       sync.Mutex
	seq      uint64
	msgs     map[string]*Message // every live message, whatever state it is in
	ready    readyHeap
	delayed  delayedHeap
	inflight map[string]*Message
	leases   leaseHeap
	dlq      []*Message
	dedup    map[string]*dedupEntry
	cnt      counters
	armed    int64 // deadline the maintenance loop is currently waiting on

	wake chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

// dedupEntry holds a dedup key's claim on a message ID. The entry is created
// before the enqueue's fsync and published by closing ready, so a duplicate
// that arrives mid-fsync waits for the original instead of being told the
// message exists when it might yet fail to reach disk.
type dedupEntry struct {
	msg       Message
	expiresAt int64
	ready     chan struct{}
	err       error
}

func newQueue(cfg Config, l *wal.Log, now func() time.Time, logger *log.Logger) *Queue {
	return &Queue{
		cfg:      cfg,
		log:      l,
		now:      now,
		logger:   logger,
		msgs:     make(map[string]*Message),
		ready:    readyHeap{lifo: cfg.Mode == LIFO},
		delayed:  delayedHeap{lifo: cfg.Mode == LIFO},
		inflight: make(map[string]*Message),
		dedup:    make(map[string]*dedupEntry),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

// Config returns the queue's configuration.
func (q *Queue) Config() Config { return q.cfg }

// Name returns the queue's name.
func (q *Queue) Name() string { return q.cfg.Name }

// Enqueue appends the message to the log, waits for the fsync covering it, and
// only then makes it visible. Doing it in that order is what stops a LEASE
// record from reaching disk before the ENQUEUE it refers to.
//
// The returned bool reports whether the dedup key collapsed this call onto an
// earlier message.
func (q *Queue) Enqueue(body []byte, priority int, delay time.Duration, dedupKey string) (*Message, bool, error) {
	now := unixMilli(q.now())

	q.mu.Lock()
	if dedupKey != "" {
		if e, ok := q.dedup[dedupKey]; ok && e.expiresAt > now {
			q.cnt.deduped++
			q.mu.Unlock()
			<-e.ready
			if e.err != nil {
				return nil, false, e.err
			}
			dup := e.msg
			return &dup, true, nil
		}
	}

	q.seq++
	m := &Message{
		ID:         randomID(12),
		Body:       body,
		Seq:        q.seq,
		EnqueuedAt: now,
		VisibleAt:  now,
		DedupKey:   dedupKey,
	}
	if q.cfg.Priority {
		m.Priority = priority
	}
	if q.cfg.Delay && delay > 0 {
		m.VisibleAt = now + delay.Milliseconds()
	}

	var entry *dedupEntry
	if dedupKey != "" {
		entry = &dedupEntry{msg: *m, expiresAt: now + DedupTTL.Milliseconds(), ready: make(chan struct{})}
		q.dedup[dedupKey] = entry
	}
	q.mu.Unlock()

	err := appendJSON(q.log, wal.RecordEnqueue, enqueuePayload{
		Queue:      q.cfg.Name,
		ID:         m.ID,
		Body:       m.Body,
		Priority:   m.Priority,
		Seq:        m.Seq,
		EnqueuedAt: m.EnqueuedAt,
		VisibleAt:  m.VisibleAt,
		DedupKey:   m.DedupKey,
	})

	q.mu.Lock()
	if err != nil {
		if entry != nil {
			delete(q.dedup, dedupKey)
			entry.err = err
		}
		q.mu.Unlock()
		if entry != nil {
			close(entry.ready)
		}
		return nil, false, err
	}
	q.insert(m, unixMilli(q.now()))
	q.cnt.enqueued++
	// Snapshot inside the lock: the moment insert returns, a consumer can
	// lease this message and start mutating it.
	out := *m
	q.mu.Unlock()

	if entry != nil {
		close(entry.ready)
	}
	return &out, false, nil
}

// Dequeue leases up to n visible messages. It returns as soon as anything is
// available and otherwise retries until wait elapses.
func (q *Queue) Dequeue(ctx context.Context, n int, wait time.Duration) ([]Delivery, error) {
	if n <= 0 {
		n = 1
	}
	deadline := q.now().Add(wait)
	for {
		out, err := q.tryDequeue(n)
		if err != nil || len(out) > 0 {
			return out, err
		}
		remaining := deadline.Sub(q.now())
		if remaining <= 0 {
			return nil, nil
		}
		sleep := pollInterval
		if remaining < sleep {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-q.done:
			timer.Stop()
			return nil, ErrClosed
		case <-timer.C:
		}
	}
}

func (q *Queue) tryDequeue(n int) ([]Delivery, error) {
	now := unixMilli(q.now())

	q.mu.Lock()
	q.promoteDue(now)
	var picked []*Message
	for len(picked) < n {
		m := q.ready.pop()
		if m == nil {
			break
		}
		picked = append(picked, m)
	}
	if len(picked) == 0 {
		q.mu.Unlock()
		return nil, nil
	}
	expires := now + q.cfg.VisibilityTimeoutMS
	items := make([]leaseItem, len(picked))
	for i, m := range picked {
		items[i] = leaseItem{ID: m.ID, Token: randomID(16), Attempts: m.Attempts + 1}
	}
	q.mu.Unlock()

	// One record and one fsync for the whole batch, not one per message.
	err := appendJSON(q.log, wal.RecordLease, leasePayload{Queue: q.cfg.Name, ExpiresAt: expires, Items: items})

	q.mu.Lock()
	defer q.mu.Unlock()
	if err != nil {
		// Nothing was mutated, so putting the messages back is the whole undo.
		for _, m := range picked {
			q.ready.push(m)
		}
		return nil, err
	}
	out := make([]Delivery, len(picked))
	for i, m := range picked {
		m.Attempts = items[i].Attempts
		m.leaseToken = items[i].Token
		m.leaseExpires = expires
		q.inflight[m.ID] = m
		q.leases.push(leaseEntry{id: m.ID, token: m.leaseToken, expiresAt: expires})
		snapshot := *m
		out[i] = Delivery{Message: &snapshot, LeaseToken: m.leaseToken, ExpiresAt: expires}
	}
	q.cnt.dequeued += uint64(len(picked))
	q.wakeIfSooner(expires)
	return out, nil
}

// Ack removes the message for good. The token must match the lease that is
// currently held, so a consumer that comes back after its lease expired and
// the message was redelivered cannot ack someone else's delivery.
func (q *Queue) Ack(id, token string) error {
	q.mu.Lock()
	m, err := q.takeLeased(id, token)
	if err != nil {
		q.mu.Unlock()
		return err
	}
	q.mu.Unlock()

	aerr := appendJSON(q.log, wal.RecordAck, ackPayload{Queue: q.cfg.Name, ID: id})

	q.mu.Lock()
	defer q.mu.Unlock()
	if aerr != nil {
		q.restoreLeased(m)
		return aerr
	}
	delete(q.msgs, id)
	m.leaseToken = ""
	q.cnt.acked++
	return nil
}

// Nack returns the message for redelivery, or moves it to the dead-letter
// queue if it has already used up its attempts.
func (q *Queue) Nack(id, token string, requeueDelay time.Duration) error {
	now := unixMilli(q.now())

	q.mu.Lock()
	m, err := q.takeLeased(id, token)
	if err != nil {
		q.mu.Unlock()
		return err
	}
	toDLQ := m.Attempts >= q.cfg.MaxAttempts
	visibleAt := now
	if !toDLQ && requeueDelay > 0 {
		visibleAt = now + requeueDelay.Milliseconds()
	}
	q.mu.Unlock()

	var aerr error
	if toDLQ {
		aerr = appendJSON(q.log, wal.RecordDLQMove, dlqPayload{Queue: q.cfg.Name, IDs: []string{id}, Reason: "max_attempts"})
	} else {
		aerr = appendJSON(q.log, wal.RecordNack, nackPayload{Queue: q.cfg.Name, ID: id, VisibleAt: visibleAt})
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if aerr != nil {
		q.restoreLeased(m)
		return aerr
	}
	if toDLQ {
		q.moveToDLQ(m)
		q.cnt.dlqMoved++
		return nil
	}
	m.leaseToken = ""
	m.VisibleAt = visibleAt
	q.insert(m, now)
	q.cnt.nacked++
	return nil
}

// Maintain promotes delayed messages that are due and reclaims leases that
// have expired. The maintenance goroutine calls it when the next deadline
// arrives; tests call it directly so they can drive a fake clock.
func (q *Queue) Maintain() error {
	now := unixMilli(q.now())

	q.mu.Lock()
	q.promoteDue(now)

	var (
		requeue    []*Message
		requeueEnt []leaseEntry
		toDLQ      []*Message
		dlqEnt     []leaseEntry
	)
	for q.leases.Len() > 0 && q.leases.peek().expiresAt <= now {
		e := q.leases.pop()
		m := q.inflight[e.id]
		if m == nil || m.leaseToken != e.token {
			continue // acked, nacked, or superseded by a later lease
		}
		delete(q.inflight, e.id)
		if m.Attempts >= q.cfg.MaxAttempts {
			toDLQ = append(toDLQ, m)
			dlqEnt = append(dlqEnt, e)
		} else {
			requeue = append(requeue, m)
			requeueEnt = append(requeueEnt, e)
		}
	}
	q.mu.Unlock()

	if len(requeue) == 0 && len(toDLQ) == 0 {
		return nil
	}

	var firstErr error
	if len(requeue) > 0 {
		items := make([]expireItem, len(requeue))
		for i, m := range requeue {
			items[i] = expireItem{ID: m.ID, VisibleAt: now}
		}
		err := appendJSON(q.log, wal.RecordLeaseExpire, expirePayload{Queue: q.cfg.Name, Items: items})

		q.mu.Lock()
		if err != nil {
			firstErr = err
			for i, m := range requeue {
				q.restoreLeasedEntry(m, requeueEnt[i])
			}
		} else {
			for _, m := range requeue {
				m.leaseToken = ""
				m.VisibleAt = now
				q.insert(m, now)
				q.cnt.expired++
			}
		}
		q.mu.Unlock()
	}

	if len(toDLQ) > 0 {
		ids := make([]string, len(toDLQ))
		for i, m := range toDLQ {
			ids[i] = m.ID
		}
		err := appendJSON(q.log, wal.RecordDLQMove, dlqPayload{Queue: q.cfg.Name, IDs: ids, Reason: "max_attempts"})

		q.mu.Lock()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			for i, m := range toDLQ {
				q.restoreLeasedEntry(m, dlqEnt[i])
			}
		} else {
			for _, m := range toDLQ {
				q.moveToDLQ(m)
				q.cnt.expired++
				q.cnt.dlqMoved++
			}
		}
		q.mu.Unlock()
	}
	return firstErr
}

// Stats snapshots the queue. OldestAgeMS is an O(n) scan over live messages;
// see DESIGN.md for why that is not a fourth heap.
func (q *Queue) Stats() Stats {
	now := unixMilli(q.now())

	q.mu.Lock()
	defer q.mu.Unlock()
	q.promoteDue(now)

	s := Stats{
		Name:     q.cfg.Name,
		Config:   q.cfg,
		Depth:    q.ready.Len(),
		Delayed:  q.delayed.Len(),
		InFlight: len(q.inflight),
		DLQ:      len(q.dlq),
		Total:    len(q.msgs) + len(q.dlq),
		Enqueued: q.cnt.enqueued,
		Dequeued: q.cnt.dequeued,
		Acked:    q.cnt.acked,
		Nacked:   q.cnt.nacked,
		Expired:  q.cnt.expired,
		DLQMoved: q.cnt.dlqMoved,
		Deduped:  q.cnt.deduped,
	}
	oldest := int64(0)
	for _, m := range q.msgs {
		if oldest == 0 || m.EnqueuedAt < oldest {
			oldest = m.EnqueuedAt
		}
	}
	if oldest != 0 {
		s.OldestAgeMS = now - oldest
	}
	return s
}

// IDs returns every live message ID, dead-lettered ones included. The crash
// test uses it to compare what survived a kill against what was acked.
func (q *Queue) IDs() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, len(q.msgs)+len(q.dlq))
	for id := range q.msgs {
		out = append(out, id)
	}
	for _, m := range q.dlq {
		out = append(out, m.ID)
	}
	return out
}

// --- helpers; all of these require q.mu ---

// takeLeased removes a message from the in-flight set so no other operation
// can touch it while its record is being fsynced.
func (q *Queue) takeLeased(id, token string) (*Message, error) {
	m, ok := q.inflight[id]
	if !ok {
		return nil, ErrNotLeased
	}
	if m.leaseToken != token {
		return nil, ErrBadLeaseToken
	}
	delete(q.inflight, id)
	return m, nil
}

func (q *Queue) restoreLeased(m *Message) {
	q.inflight[m.ID] = m
}

func (q *Queue) restoreLeasedEntry(m *Message, e leaseEntry) {
	q.inflight[m.ID] = m
	q.leases.push(e)
}

func (q *Queue) insert(m *Message, now int64) {
	q.msgs[m.ID] = m
	if m.VisibleAt > now {
		q.delayed.push(m)
		q.wakeIfSooner(m.VisibleAt)
		return
	}
	q.ready.push(m)
}

func (q *Queue) promoteDue(now int64) {
	for {
		m := q.delayed.peek()
		if m == nil || m.VisibleAt > now {
			return
		}
		q.ready.push(q.delayed.pop())
	}
}

func (q *Queue) moveToDLQ(m *Message) {
	delete(q.msgs, m.ID)
	m.leaseToken = ""
	if q.cfg.DLQ {
		q.dlq = append(q.dlq, m)
	}
}

// wakeIfSooner nudges the maintenance loop only when the new deadline lands
// before the one it is already waiting on, so a busy queue does not wake it
// on every single operation.
func (q *Queue) wakeIfSooner(deadline int64) {
	if q.armed != 0 && deadline >= q.armed {
		return
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// nextDeadline is the earliest time at which Maintain would have work.
func (q *Queue) nextDeadline() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	var next int64
	if m := q.delayed.peek(); m != nil {
		next = m.VisibleAt
	}
	if q.leases.Len() > 0 {
		if e := q.leases.peek(); next == 0 || e.expiresAt < next {
			next = e.expiresAt
		}
	}
	q.armed = next
	return next
}

// maintain waits for the next deadline rather than polling on a tick, so an
// idle queue costs one blocked goroutine and no wakeups at all.
func (q *Queue) maintain() {
	defer q.wg.Done()
	for {
		next := q.nextDeadline()
		if next == 0 {
			select {
			case <-q.wake:
				continue
			case <-q.done:
				return
			}
		}
		// Measured against the queue's own clock, so a test clock that does
		// not advance parks the timer instead of spinning on a past deadline.
		delay := time.UnixMilli(next).Sub(q.now())
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			if err := q.Maintain(); err != nil {
				// The only way an append fails is a dead log, and every
				// subsequent one will fail the same way.
				q.logger.Printf("queue %s: maintenance stopped: %v", q.cfg.Name, err)
				return
			}
		case <-q.wake:
			timer.Stop()
		case <-q.done:
			timer.Stop()
			return
		}
	}
}

func (q *Queue) start() {
	q.wg.Add(1)
	go q.maintain()
}

func (q *Queue) stop() {
	close(q.done)
	q.wg.Wait()
}
