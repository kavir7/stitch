// Package wal implements the write-ahead log that is this queue's entire
// storage layer. Records are framed with a magic number, a length, a CRC32C
// and a type byte; state is rebuilt on boot by folding the framed records in
// order. There is no separate data file and no index, so the only thing that
// has to be crash-safe is the append path.
package wal

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

// SyncMode selects how often the log calls fsync.
type SyncMode string

const (
	// SyncAlways fsyncs once per record. Durable per write, slow.
	SyncAlways SyncMode = "always"
	// SyncGroup commits immediately whenever the writer is idle and batches
	// only the records that pile up behind an fsync already in flight. There
	// is no fixed delay: batch size grows with load, latency does not. Callers
	// still block until the fsync covering their record returns, so batching
	// trades throughput for nothing but a little tail latency under load.
	SyncGroup SyncMode = "group"
)

// Defaults for Options.
const (
	DefaultSegmentSize = 64 << 20
	DefaultBatchMax    = 256
)

// ErrClosed is returned by Append after Close.
var ErrClosed = errors.New("wal: log is closed")

// Options configures a Log.
type Options struct {
	Dir         string
	Sync        SyncMode
	SegmentSize int64
	// BatchMax caps how many records one fsync may cover. It bounds the worst
	// case a single caller can wait behind, nothing more.
	BatchMax int
	Logger   *log.Logger
}

func (o *Options) withDefaults() {
	if o.Sync == "" {
		o.Sync = SyncGroup
	}
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}
	if o.BatchMax <= 0 {
		o.BatchMax = DefaultBatchMax
	}
	if o.Logger == nil {
		o.Logger = log.Default()
	}
}

type appendReq struct {
	typ     RecordType
	payload []byte
	done    chan error
}

// Stats counts what the writer has done. Batches is the number of fsyncs, so
// Appends/Batches is the average group size and is the single number that says
// whether group commit is earning its keep.
type Stats struct {
	Appends uint64
	Batches uint64
	Bytes   uint64
}

// Log is an append-only write-ahead log over rotating segment files.
type Log struct {
	opts Options
	reqs chan *appendReq
	wg   sync.WaitGroup

	appends atomic.Uint64
	batches atomic.Uint64
	bytes   atomic.Uint64

	mu     sync.RWMutex
	closed bool

	// Everything below is owned exclusively by the writer goroutine after
	// Open returns, which is why the append path needs no file lock.
	f     *os.File
	index uint64
	size  int64
	buf   []byte
	fatal error
}

// Open recovers the log in opts.Dir, calling apply for every valid record in
// write order, and returns a Log positioned to append after the last good
// record. apply may be nil. No writes are accepted until recovery finishes,
// so the caller's state is always a complete fold of the log before it moves.
func Open(opts Options, apply func(Record) error) (*Log, RecoveryInfo, error) {
	opts.withDefaults()
	if opts.Dir == "" {
		return nil, RecoveryInfo{}, errors.New("wal: Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, RecoveryInfo{}, err
	}

	info, lastIndex, lastSize, err := replay(opts.Dir, apply)
	if err != nil {
		return nil, RecoveryInfo{}, err
	}
	if info.Truncated {
		opts.Logger.Printf("wal: torn tail detected, truncated segment %s at offset %d", segmentName(lastIndex), info.TruncatedAt)
	}
	opts.Logger.Printf("wal: recovered %d records from %d segment(s) in %s", info.Records, info.Segments, opts.Dir)

	l := &Log{
		opts:  opts,
		reqs:  make(chan *appendReq, opts.BatchMax),
		index: lastIndex,
		size:  lastSize,
		buf:   make([]byte, 0, 64<<10),
	}
	if info.Segments == 0 {
		f, cerr := createSegment(opts.Dir, 0)
		if cerr != nil {
			return nil, RecoveryInfo{}, cerr
		}
		l.f = f
	} else {
		f, oerr := os.OpenFile(segmentPath(opts.Dir, lastIndex), os.O_WRONLY|os.O_APPEND, 0o644)
		if oerr != nil {
			return nil, RecoveryInfo{}, oerr
		}
		l.f = f
	}

	l.wg.Add(1)
	go l.run()
	return l, info, nil
}

// Append writes one record and blocks until the fsync covering it has
// returned. That is the durability invariant the HTTP layer depends on: a
// producer is never told 200 for a record that is not already on disk.
// payload must not be mutated until Append returns.
func (l *Log) Append(typ RecordType, payload []byte) error {
	if !typ.valid() {
		return fmt.Errorf("wal: invalid record type %d", uint8(typ))
	}
	if len(payload) > maxRecordSize {
		return ErrRecordTooLarge
	}
	req := &appendReq{typ: typ, payload: payload, done: make(chan error, 1)}

	// The read lock keeps Close from closing reqs while a send is in flight.
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return ErrClosed
	}
	l.reqs <- req
	l.mu.RUnlock()

	return <-req.done
}

// Close drains the pending batch, stops the writer and closes the segment.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.reqs)
	l.mu.Unlock()

	l.wg.Wait()
	cerr := l.f.Close()
	if l.fatal != nil {
		return l.fatal
	}
	return cerr
}

// run is the single writer goroutine. It owns the file handle, so appends
// never contend on a mutex for the file itself, and it is the only place an
// fsync is issued.
//
// The batching rule is: never wait. Take whatever is queued right now, commit
// it, and take whatever queued up while that fsync was running. An idle writer
// therefore syncs a single record with no added delay, and a busy one folds
// everything that arrived during the previous sync into one sync. Batch size
// tracks load automatically, which a fixed collection window does not: a
// window that is short enough not to hurt a lone producer is too short to
// batch anything useful, and a window long enough to batch is pure latency
// when there is nobody to batch with.
func (l *Log) run() {
	defer l.wg.Done()

	batch := make([]*appendReq, 0, l.opts.BatchMax)
	for {
		first, ok := <-l.reqs
		if !ok {
			return
		}
		batch = append(batch[:0], first)
		closing := false

		if l.opts.Sync == SyncGroup {
		drain:
			for len(batch) < l.opts.BatchMax {
				select {
				case req, ok := <-l.reqs:
					if !ok {
						closing = true
						break drain
					}
					batch = append(batch, req)
				default:
					break drain
				}
			}
		}

		l.commit(batch)
		if closing {
			return
		}
	}
}

// commit writes the whole batch in one syscall, fsyncs once, and only then
// releases the waiting callers.
func (l *Log) commit(batch []*appendReq) {
	if l.fatal != nil {
		reply(batch, l.fatal)
		return
	}

	var total int64
	for _, req := range batch {
		total += encodedSize(req.payload)
	}
	// The segment size is a soft limit: a batch is never split across
	// segments, so a segment can overshoot by at most one batch.
	if l.size > 0 && l.size+total > l.opts.SegmentSize {
		if err := l.rotate(); err != nil {
			l.fatal = err
			reply(batch, err)
			return
		}
	}

	l.buf = l.buf[:0]
	for _, req := range batch {
		l.buf = appendRecord(l.buf, req.typ, req.payload)
	}
	if _, err := l.f.Write(l.buf); err != nil {
		l.fatal = err
		reply(batch, err)
		return
	}
	if err := l.f.Sync(); err != nil {
		l.fatal = err
		reply(batch, err)
		return
	}
	l.size += total
	l.appends.Add(uint64(len(batch)))
	l.batches.Add(1)
	l.bytes.Add(uint64(total))
	reply(batch, nil)
}

// Stats is safe to call from any goroutine.
func (l *Log) Stats() Stats {
	return Stats{
		Appends: l.appends.Load(),
		Batches: l.batches.Load(),
		Bytes:   l.bytes.Load(),
	}
}

func (l *Log) rotate() error {
	if err := l.f.Close(); err != nil {
		return err
	}
	f, err := createSegment(l.opts.Dir, l.index+1)
	if err != nil {
		return err
	}
	l.f = f
	l.index++
	l.size = 0
	return nil
}

func reply(batch []*appendReq, err error) {
	for _, req := range batch {
		req.done <- err
	}
}
