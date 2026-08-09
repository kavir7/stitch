package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/kavir7/stitch/internal/wal"
)

// Options configures the manager and the log underneath it.
type Options struct {
	Dir         string
	Sync        wal.SyncMode
	SegmentSize int64
	BatchMax    int
	Logger      *log.Logger
	// Now defaults to time.Now. Tests replace it to drive delays and lease
	// expiry without sleeping.
	Now func() time.Time
}

// Manager owns the log and the set of queues on top of it.
type Manager struct {
	log    *wal.Log
	logger *log.Logger
	now    func() time.Time

	mu     sync.RWMutex
	queues map[string]*Queue
	closed bool
}

// Open replays the log at opts.Dir and returns a manager holding the state
// that fold produced. Nothing accepts writes until the replay is complete.
func Open(opts Options) (*Manager, wal.RecoveryInfo, error) {
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	m := &Manager{
		logger: opts.Logger,
		now:    opts.Now,
		queues: make(map[string]*Queue),
	}
	rs := &replayState{leased: make(map[string]map[string]struct{})}

	l, info, err := wal.Open(wal.Options{
		Dir:         opts.Dir,
		Sync:        opts.Sync,
		SegmentSize: opts.SegmentSize,
		BatchMax:    opts.BatchMax,
		Logger:      opts.Logger,
	}, func(rec wal.Record) error { return m.apply(rs, rec) })
	if err != nil {
		return nil, info, err
	}

	m.log = l
	m.finalize(rs)
	for _, q := range m.queues {
		q.log = l
		q.start()
	}
	if len(m.queues) > 0 {
		m.logger.Printf("queue: recovered %d queue(s) from %d record(s)", len(m.queues), info.Records)
	}
	return m, info, nil
}

// CreateQueue registers a queue and makes it durable before returning it.
func (m *Manager) CreateQueue(cfg Config) (*Queue, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}

	// The manager lock is held across the append on purpose. A queue that is
	// visible before its QUEUE_CREATE record is durable could take an enqueue
	// whose record replays against a queue that does not exist. Queue creation
	// is rare, so paying one fsync of exclusion for that is free.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	if _, ok := m.queues[cfg.Name]; ok {
		return nil, ErrQueueExists
	}
	if err := appendJSON(m.log, wal.RecordQueueCreate, queueCreatePayload{Config: cfg}); err != nil {
		return nil, err
	}
	q := newQueue(cfg, m.log, m.now, m.logger)
	q.start()
	m.queues[cfg.Name] = q
	return q, nil
}

// Queue looks up a queue by name.
func (m *Manager) Queue(name string) (*Queue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

// Queues returns every queue, ordered by name so listings are stable.
func (m *Manager) Queues() []*Queue {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].cfg.Name < out[j].cfg.Name })
	return out
}

// WALStats exposes the log's counters for /metrics.
func (m *Manager) WALStats() wal.Stats { return m.log.Stats() }

// Close stops every queue's maintenance goroutine and closes the log.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	queues := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		queues = append(queues, q)
	}
	m.mu.Unlock()

	for _, q := range queues {
		q.stop()
	}
	return m.log.Close()
}

// --- replay ---

// replayState is the bookkeeping that only exists while the log is being
// folded. Leases are tracked here rather than on the queue because they do not
// survive the fold: a lease is a promise to one consumer connection, and that
// connection is gone.
type replayState struct {
	leased map[string]map[string]struct{}
}

func (rs *replayState) lease(q, id string) {
	set, ok := rs.leased[q]
	if !ok {
		set = make(map[string]struct{})
		rs.leased[q] = set
	}
	set[id] = struct{}{}
}

func (rs *replayState) unlease(q, id string) {
	if set, ok := rs.leased[q]; ok {
		delete(set, id)
	}
}

func (m *Manager) apply(rs *replayState, rec wal.Record) error {
	switch rec.Type {
	case wal.RecordQueueCreate:
		var p queueCreatePayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return err
		}
		if err := p.Config.normalize(); err != nil {
			return err
		}
		// A repeated QUEUE_CREATE for the same name cannot happen, because
		// CreateQueue holds the manager lock across its append.
		if _, ok := m.queues[p.Config.Name]; ok {
			return fmt.Errorf("replay: queue %q created twice", p.Config.Name)
		}
		m.queues[p.Config.Name] = newQueue(p.Config, nil, m.now, m.logger)
		return nil

	case wal.RecordEnqueue:
		var p enqueuePayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return err
		}
		q, err := m.replayQueue(p.Queue)
		if err != nil {
			return err
		}
		msg := &Message{
			ID:         p.ID,
			Body:       p.Body,
			Priority:   p.Priority,
			Seq:        p.Seq,
			EnqueuedAt: p.EnqueuedAt,
			VisibleAt:  p.VisibleAt,
			DedupKey:   p.DedupKey,
		}
		q.msgs[msg.ID] = msg
		if p.Seq > q.seq {
			q.seq = p.Seq
		}
		if p.DedupKey != "" {
			closed := make(chan struct{})
			close(closed)
			q.dedup[p.DedupKey] = &dedupEntry{
				msg:       *msg,
				expiresAt: p.EnqueuedAt + DedupTTL.Milliseconds(),
				ready:     closed,
			}
		}
		q.cnt.enqueued++
		return nil

	case wal.RecordLease:
		var p leasePayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return err
		}
		q, err := m.replayQueue(p.Queue)
		if err != nil {
			return err
		}
		for _, it := range p.Items {
			msg, ok := q.msgs[it.ID]
			if !ok {
				return fmt.Errorf("replay: lease for unknown message %q in queue %q", it.ID, p.Queue)
			}
			msg.Attempts = it.Attempts
			msg.leaseToken = it.Token
			msg.leaseExpires = p.ExpiresAt
			rs.lease(p.Queue, it.ID)
		}
		q.cnt.dequeued += uint64(len(p.Items))
		return nil

	case wal.RecordAck:
		var p ackPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return err
		}
		q, err := m.replayQueue(p.Queue)
		if err != nil {
			return err
		}
		if _, ok := q.msgs[p.ID]; !ok {
			return fmt.Errorf("replay: ack for unknown message %q in queue %q", p.ID, p.Queue)
		}
		delete(q.msgs, p.ID)
		rs.unlease(p.Queue, p.ID)
		q.cnt.acked++
		return nil

	case wal.RecordNack:
		var p nackPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return err
		}
		q, err := m.replayQueue(p.Queue)
		if err != nil {
			return err
		}
		msg, ok := q.msgs[p.ID]
		if !ok {
			return fmt.Errorf("replay: nack for unknown message %q in queue %q", p.ID, p.Queue)
		}
		msg.VisibleAt = p.VisibleAt
		msg.leaseToken = ""
		rs.unlease(p.Queue, p.ID)
		q.cnt.nacked++
		return nil

	case wal.RecordLeaseExpire:
		var p expirePayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return err
		}
		q, err := m.replayQueue(p.Queue)
		if err != nil {
			return err
		}
		for _, it := range p.Items {
			msg, ok := q.msgs[it.ID]
			if !ok {
				return fmt.Errorf("replay: lease expiry for unknown message %q in queue %q", it.ID, p.Queue)
			}
			msg.VisibleAt = it.VisibleAt
			msg.leaseToken = ""
			rs.unlease(p.Queue, it.ID)
		}
		q.cnt.expired += uint64(len(p.Items))
		return nil

	case wal.RecordDLQMove:
		var p dlqPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return err
		}
		q, err := m.replayQueue(p.Queue)
		if err != nil {
			return err
		}
		for _, id := range p.IDs {
			msg, ok := q.msgs[id]
			if !ok {
				return fmt.Errorf("replay: dlq move for unknown message %q in queue %q", id, p.Queue)
			}
			delete(q.msgs, id)
			rs.unlease(p.Queue, id)
			msg.leaseToken = ""
			if q.cfg.DLQ {
				q.dlq = append(q.dlq, msg)
			}
			q.cnt.dlqMoved++
		}
		return nil

	default:
		return fmt.Errorf("replay: unexpected record type %s", rec.Type)
	}
}

func (m *Manager) replayQueue(name string) (*Queue, error) {
	q, ok := m.queues[name]
	if !ok {
		return nil, fmt.Errorf("replay: record references unknown queue %q", name)
	}
	return q, nil
}

// finalize turns the replayed maps into the heaps the live path uses. Messages
// that were leased when the process died go back to ready with their attempt
// count intact, which is exactly the at-least-once promise: a delivery that was
// granted but never acked gets granted again.
func (m *Manager) finalize(rs *replayState) {
	now := unixMilli(m.now())
	for name, q := range m.queues {
		leased := rs.leased[name]
		msgs := make([]*Message, 0, len(q.msgs))
		for _, msg := range q.msgs {
			msgs = append(msgs, msg)
		}
		// Insert in sequence order so the heaps are built from a stable input
		// and two replays of the same log produce byte-identical state.
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })
		for _, msg := range msgs {
			if _, wasLeased := leased[msg.ID]; wasLeased {
				msg.leaseToken = ""
				msg.leaseExpires = 0
			}
			if msg.VisibleAt > now {
				q.delayed.push(msg)
			} else {
				q.ready.push(msg)
			}
		}
		for key, e := range q.dedup {
			if e.expiresAt <= now {
				delete(q.dedup, key)
			}
		}
		if n := len(leased); n > 0 {
			m.logger.Printf("queue %s: %d message(s) were in flight at shutdown and are ready again", name, n)
		}
	}
}
