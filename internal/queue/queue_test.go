package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kavir7/stitch/internal/wal"
)

// testClock lets the tests move time without sleeping. The queue reads it
// through Options.Now, and the maintenance goroutine measures its waits
// against it too, so a frozen clock parks that goroutine rather than spinning.
type testClock struct{ ms atomic.Int64 }

func newTestClock() *testClock {
	c := &testClock{}
	c.ms.Store(time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC).UnixMilli())
	return c
}

func (c *testClock) Now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *testClock) Advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

func newTestManager(t *testing.T, dir string, clock *testClock) *Manager {
	t.Helper()
	opts := Options{
		Dir:    dir,
		Sync:   wal.SyncGroup,
		Logger: log.New(io.Discard, "", 0),
	}
	if clock != nil {
		opts.Now = clock.Now
	}
	m, _, err := Open(opts)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("close manager: %v", err)
		}
	})
	return m
}

func mustCreate(t *testing.T, m *Manager, cfg Config) *Queue {
	t.Helper()
	q, err := m.CreateQueue(cfg)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	return q
}

func mustEnqueue(t *testing.T, q *Queue, body string, priority int, delay time.Duration) *Message {
	t.Helper()
	msg, deduped, err := q.Enqueue([]byte(body), priority, delay, "")
	if err != nil {
		t.Fatalf("enqueue %q: %v", body, err)
	}
	if deduped {
		t.Fatalf("enqueue %q was unexpectedly deduplicated", body)
	}
	return msg
}

func drain(t *testing.T, q *Queue, n int) []Delivery {
	t.Helper()
	d, err := q.Dequeue(context.Background(), n, 0)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	return d
}

func bodies(ds []Delivery) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = string(d.Message.Body)
	}
	return out
}

func TestEnqueueDequeueAck(t *testing.T) {
	m := newTestManager(t, t.TempDir(), nil)
	q := mustCreate(t, m, Config{Name: "jobs"})

	msg := mustEnqueue(t, q, "hello", 0, 0)
	if s := q.Stats(); s.Depth != 1 || s.Total != 1 {
		t.Fatalf("after enqueue: depth=%d total=%d", s.Depth, s.Total)
	}

	got := drain(t, q, 10)
	if len(got) != 1 {
		t.Fatalf("dequeued %d messages, want 1", len(got))
	}
	if got[0].Message.ID != msg.ID || string(got[0].Message.Body) != "hello" {
		t.Fatalf("dequeued the wrong message: %+v", got[0].Message)
	}
	if got[0].Message.Attempts != 1 {
		t.Fatalf("first delivery reported %d attempts, want 1", got[0].Message.Attempts)
	}
	if s := q.Stats(); s.Depth != 0 || s.InFlight != 1 {
		t.Fatalf("after dequeue: depth=%d in_flight=%d", s.Depth, s.InFlight)
	}

	if err := q.Ack(msg.ID, got[0].LeaseToken); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if s := q.Stats(); s.Total != 0 || s.InFlight != 0 || s.Acked != 1 {
		t.Fatalf("after ack: %+v", s)
	}
	if len(drain(t, q, 1)) != 0 {
		t.Fatal("an acked message came back")
	}
}

// The eight queue types are one comparator with different config. These four
// cases cover the sequence and priority axes; the delay axis is below and the
// property test walks all eight combinations together.
func TestDeliveryOrderAcrossModes(t *testing.T) {
	cases := []struct {
		name     string
		cfg      Config
		want     []string
		priority map[string]int
	}{
		{
			name: "fifo delivers in arrival order",
			cfg:  Config{Name: "q", Mode: FIFO},
			want: []string{"a", "b", "c", "d"},
		},
		{
			name: "lifo delivers in reverse arrival order",
			cfg:  Config{Name: "q", Mode: LIFO},
			want: []string{"d", "c", "b", "a"},
		},
		{
			name:     "priority fifo breaks ties by arrival",
			cfg:      Config{Name: "q", Mode: FIFO, Priority: true},
			priority: map[string]int{"a": 1, "b": 9, "c": 9, "d": 5},
			want:     []string{"b", "c", "d", "a"},
		},
		{
			name:     "priority lifo breaks ties by reverse arrival",
			cfg:      Config{Name: "q", Mode: LIFO, Priority: true},
			priority: map[string]int{"a": 1, "b": 9, "c": 9, "d": 5},
			want:     []string{"c", "b", "d", "a"},
		},
		{
			name:     "priority disabled ignores the priority field entirely",
			cfg:      Config{Name: "q", Mode: FIFO, Priority: false},
			priority: map[string]int{"a": 1, "b": 9, "c": 9, "d": 5},
			want:     []string{"a", "b", "c", "d"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t, t.TempDir(), nil)
			q := mustCreate(t, m, tc.cfg)
			for _, body := range []string{"a", "b", "c", "d"} {
				mustEnqueue(t, q, body, tc.priority[body], 0)
			}
			got := bodies(drain(t, q, 10))
			if len(got) != len(tc.want) {
				t.Fatalf("dequeued %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("delivery order %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestDelayHidesMessagesUntilDue(t *testing.T) {
	clock := newTestClock()
	m := newTestManager(t, t.TempDir(), clock)
	q := mustCreate(t, m, Config{Name: "q", Delay: true})

	mustEnqueue(t, q, "later", 0, 500*time.Millisecond)
	mustEnqueue(t, q, "now", 0, 0)

	got := bodies(drain(t, q, 10))
	if len(got) != 1 || got[0] != "now" {
		t.Fatalf("dequeued %v before the delay elapsed, want just [now]", got)
	}
	if s := q.Stats(); s.Delayed != 1 {
		t.Fatalf("delayed count is %d, want 1", s.Delayed)
	}

	clock.Advance(499 * time.Millisecond)
	if got := drain(t, q, 10); len(got) != 0 {
		t.Fatalf("message became visible 1ms early: %v", bodies(got))
	}
	clock.Advance(time.Millisecond)
	got = bodies(drain(t, q, 10))
	if len(got) != 1 || got[0] != "later" {
		t.Fatalf("dequeued %v after the delay elapsed, want [later]", got)
	}
}

func TestDelayDisabledIgnoresTheDelay(t *testing.T) {
	clock := newTestClock()
	m := newTestManager(t, t.TempDir(), clock)
	q := mustCreate(t, m, Config{Name: "q", Delay: false})

	mustEnqueue(t, q, "immediate", 0, time.Hour)
	got := bodies(drain(t, q, 10))
	if len(got) != 1 || got[0] != "immediate" {
		t.Fatalf("a delay-disabled queue held the message back: %v", got)
	}
}

// Ordering must not depend on when a delayed message becomes visible: once it
// is promoted it takes its place by sequence, not by promotion time.
func TestPromotedMessageKeepsItsSequencePosition(t *testing.T) {
	clock := newTestClock()
	m := newTestManager(t, t.TempDir(), clock)
	q := mustCreate(t, m, Config{Name: "q", Mode: FIFO, Delay: true})

	mustEnqueue(t, q, "first-but-delayed", 0, 100*time.Millisecond)
	mustEnqueue(t, q, "second", 0, 0)
	clock.Advance(200 * time.Millisecond)

	got := bodies(drain(t, q, 10))
	if len(got) != 2 || got[0] != "first-but-delayed" || got[1] != "second" {
		t.Fatalf("delivery order %v, want the promoted message back in sequence order", got)
	}
}

func TestAckRejectsStaleAndUnknownTokens(t *testing.T) {
	clock := newTestClock()
	m := newTestManager(t, t.TempDir(), clock)
	q := mustCreate(t, m, Config{Name: "q", VisibilityTimeoutMS: 1000, MaxAttempts: 10})

	msg := mustEnqueue(t, q, "work", 0, 0)
	first := drain(t, q, 1)[0]

	if err := q.Ack(msg.ID, "not-the-token"); !errors.Is(err, ErrBadLeaseToken) {
		t.Fatalf("ack with a wrong token returned %v, want ErrBadLeaseToken", err)
	}
	if err := q.Ack("no-such-message", first.LeaseToken); !errors.Is(err, ErrNotLeased) {
		t.Fatalf("ack for an unknown message returned %v, want ErrNotLeased", err)
	}

	// Let the lease lapse and the message come back to another consumer.
	clock.Advance(1500 * time.Millisecond)
	if err := q.Maintain(); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	second := drain(t, q, 1)
	if len(second) != 1 {
		t.Fatal("the expired lease was not redelivered")
	}
	if second[0].Message.Attempts != 2 {
		t.Fatalf("redelivery reported %d attempts, want 2", second[0].Message.Attempts)
	}
	if second[0].LeaseToken == first.LeaseToken {
		t.Fatal("redelivery reused the expired lease token")
	}

	// This is the double-ack the token exists to prevent: the first consumer
	// finally finishes and acks with a token that no longer owns the message.
	if err := q.Ack(msg.ID, first.LeaseToken); !errors.Is(err, ErrBadLeaseToken) {
		t.Fatalf("stale token was accepted: %v", err)
	}
	if err := q.Ack(msg.ID, second[0].LeaseToken); err != nil {
		t.Fatalf("current token was rejected: %v", err)
	}
}

func TestNackRedeliversAndHonoursRequeueDelay(t *testing.T) {
	clock := newTestClock()
	m := newTestManager(t, t.TempDir(), clock)
	q := mustCreate(t, m, Config{Name: "q", MaxAttempts: 10})

	msg := mustEnqueue(t, q, "work", 0, 0)
	d := drain(t, q, 1)[0]
	if err := q.Nack(msg.ID, d.LeaseToken, 200*time.Millisecond); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if s := q.Stats(); s.Nacked != 1 || s.Delayed != 1 || s.InFlight != 0 {
		t.Fatalf("after nack: %+v", s)
	}
	if got := drain(t, q, 1); len(got) != 0 {
		t.Fatal("a nacked message with a requeue delay came back immediately")
	}

	clock.Advance(200 * time.Millisecond)
	got := drain(t, q, 1)
	if len(got) != 1 || got[0].Message.Attempts != 2 {
		t.Fatalf("after the requeue delay: %d deliveries", len(got))
	}
}

func TestMaxAttemptsMovesMessageToDLQ(t *testing.T) {
	for _, how := range []string{"nack", "lease expiry"} {
		t.Run(how, func(t *testing.T) {
			clock := newTestClock()
			m := newTestManager(t, t.TempDir(), clock)
			q := mustCreate(t, m, Config{Name: "q", MaxAttempts: 3, DLQ: true, VisibilityTimeoutMS: 1000})

			msg := mustEnqueue(t, q, "poison", 0, 0)
			for attempt := 1; attempt <= 3; attempt++ {
				got := drain(t, q, 1)
				if len(got) != 1 {
					t.Fatalf("attempt %d: message was not delivered", attempt)
				}
				if got[0].Message.Attempts != attempt {
					t.Fatalf("attempt %d: reported %d attempts", attempt, got[0].Message.Attempts)
				}
				if how == "nack" {
					if err := q.Nack(msg.ID, got[0].LeaseToken, 0); err != nil {
						t.Fatalf("nack: %v", err)
					}
				} else {
					clock.Advance(1500 * time.Millisecond)
					if err := q.Maintain(); err != nil {
						t.Fatalf("maintain: %v", err)
					}
				}
			}

			s := q.Stats()
			if s.DLQ != 1 || s.DLQMoved != 1 {
				t.Fatalf("after %d attempts: dlq=%d moved=%d", 3, s.DLQ, s.DLQMoved)
			}
			if s.Depth != 0 || s.InFlight != 0 {
				t.Fatalf("the poison message is still deliverable: %+v", s)
			}
			if len(drain(t, q, 1)) != 0 {
				t.Fatal("a dead-lettered message was delivered again")
			}
		})
	}
}

func TestDLQDisabledDropsExhaustedMessages(t *testing.T) {
	m := newTestManager(t, t.TempDir(), nil)
	q := mustCreate(t, m, Config{Name: "q", MaxAttempts: 1, DLQ: false})

	msg := mustEnqueue(t, q, "poison", 0, 0)
	d := drain(t, q, 1)[0]
	if err := q.Nack(msg.ID, d.LeaseToken, 0); err != nil {
		t.Fatalf("nack: %v", err)
	}
	s := q.Stats()
	if s.Total != 0 || s.DLQ != 0 || s.DLQMoved != 1 {
		t.Fatalf("with the dead-letter queue off the message should be dropped: %+v", s)
	}
}

func TestDedupCollapsesRepeatEnqueues(t *testing.T) {
	clock := newTestClock()
	m := newTestManager(t, t.TempDir(), clock)
	q := mustCreate(t, m, Config{Name: "q"})

	first, deduped, err := q.Enqueue([]byte("payload"), 0, 0, "order-42")
	if err != nil || deduped {
		t.Fatalf("first enqueue: msg=%v deduped=%v err=%v", first, deduped, err)
	}
	second, deduped, err := q.Enqueue([]byte("payload again"), 0, 0, "order-42")
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if !deduped {
		t.Fatal("the repeat enqueue was not deduplicated")
	}
	if second.ID != first.ID {
		t.Fatalf("dedup returned id %q, want the original %q", second.ID, first.ID)
	}
	if s := q.Stats(); s.Total != 1 || s.Deduped != 1 {
		t.Fatalf("after a duplicate: %+v", s)
	}

	clock.Advance(DedupTTL + time.Second)
	third, deduped, err := q.Enqueue([]byte("payload"), 0, 0, "order-42")
	if err != nil {
		t.Fatalf("third enqueue: %v", err)
	}
	if deduped || third.ID == first.ID {
		t.Fatal("the dedup key still collapsed enqueues after its TTL expired")
	}
}

// A duplicate that arrives while the original is still being fsynced must wait
// for that fsync rather than being told the message exists.
func TestDedupWaitsForTheOriginalToCommit(t *testing.T) {
	m := newTestManager(t, t.TempDir(), nil)
	q := mustCreate(t, m, Config{Name: "q"})

	const n = 16
	ids := make([]string, n)
	dups := make([]bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg, deduped, err := q.Enqueue([]byte("body"), 0, 0, "same-key")
			if err != nil {
				t.Errorf("enqueue %d: %v", i, err)
				return
			}
			ids[i], dups[i] = msg.ID, deduped
		}(i)
	}
	wg.Wait()

	originals := 0
	for i := 0; i < n; i++ {
		if !dups[i] {
			originals++
		}
		if ids[i] != ids[0] {
			t.Fatalf("concurrent enqueues with one dedup key returned different ids: %q and %q", ids[i], ids[0])
		}
	}
	if originals != 1 {
		t.Fatalf("%d of %d concurrent enqueues were treated as originals, want 1", originals, n)
	}
	if s := q.Stats(); s.Total != 1 {
		t.Fatalf("%d messages exist, want 1", s.Total)
	}
}

func TestDequeueBatchAndWait(t *testing.T) {
	m := newTestManager(t, t.TempDir(), nil)
	q := mustCreate(t, m, Config{Name: "q"})

	for i := 0; i < 25; i++ {
		mustEnqueue(t, q, fmt.Sprintf("m%02d", i), 0, 0)
	}
	got := drain(t, q, 10)
	if len(got) != 10 {
		t.Fatalf("batch dequeue returned %d, want 10", len(got))
	}
	if got[0].Message.Body[0] != 'm' || string(got[9].Message.Body) != "m09" {
		t.Fatalf("batch is out of order: %v", bodies(got))
	}

	// An empty queue with a wait must block for roughly that long and then
	// return empty rather than erroring.
	empty := mustCreate(t, m, Config{Name: "empty"})
	start := time.Now()
	out, err := empty.Dequeue(context.Background(), 5, 60*time.Millisecond)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("waiting dequeue: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("empty queue returned %d messages", len(out))
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("waiting dequeue returned after %v, well before its 60ms wait", elapsed)
	}
}

func TestDequeueWaitReturnsAsSoonAsAMessageArrives(t *testing.T) {
	m := newTestManager(t, t.TempDir(), nil)
	q := mustCreate(t, m, Config{Name: "q"})

	done := make(chan []Delivery, 1)
	go func() {
		out, err := q.Dequeue(context.Background(), 5, 2*time.Second)
		if err != nil {
			t.Errorf("dequeue: %v", err)
		}
		done <- out
	}()

	time.Sleep(20 * time.Millisecond)
	mustEnqueue(t, q, "late arrival", 0, 0)

	select {
	case out := <-done:
		if len(out) != 1 || string(out[0].Message.Body) != "late arrival" {
			t.Fatalf("waiting consumer got %v", bodies(out))
		}
	case <-time.After(time.Second):
		t.Fatal("a waiting consumer did not pick up a message enqueued during its wait")
	}
}

func TestDequeueRespectsContextCancellation(t *testing.T) {
	m := newTestManager(t, t.TempDir(), nil)
	q := mustCreate(t, m, Config{Name: "q"})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := q.Dequeue(ctx, 1, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled dequeue returned %v, want context.Canceled", err)
	}
}

// Every message must be delivered to exactly one consumer even when producers
// and consumers run flat out against the same queue.
func TestConcurrentProducersAndConsumers(t *testing.T) {
	const (
		producers = 8
		perProd   = 250
		consumers = 8
	)

	m := newTestManager(t, t.TempDir(), nil)
	q := mustCreate(t, m, Config{Name: "q", VisibilityTimeoutMS: 60000, MaxAttempts: 10})

	var produce sync.WaitGroup
	for p := 0; p < producers; p++ {
		produce.Add(1)
		go func(p int) {
			defer produce.Done()
			for i := 0; i < perProd; i++ {
				if _, _, err := q.Enqueue([]byte(fmt.Sprintf("p%d-%d", p, i)), 0, 0, ""); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	var (
		mu        sync.Mutex
		acked     = make(map[string]int)
		consume   sync.WaitGroup
		producing atomic.Bool
	)
	producing.Store(true)
	for c := 0; c < consumers; c++ {
		consume.Add(1)
		go func() {
			defer consume.Done()
			for {
				out, err := q.Dequeue(context.Background(), 7, 0)
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				if len(out) == 0 {
					if !producing.Load() {
						return
					}
					time.Sleep(time.Millisecond)
					continue
				}
				for _, d := range out {
					if err := q.Ack(d.Message.ID, d.LeaseToken); err != nil {
						t.Errorf("ack: %v", err)
						return
					}
					mu.Lock()
					acked[string(d.Message.Body)]++
					mu.Unlock()
				}
			}
		}()
	}

	produce.Wait()
	producing.Store(false)
	consume.Wait()

	if len(acked) != producers*perProd {
		t.Fatalf("%d distinct messages were acked, want %d", len(acked), producers*perProd)
	}
	for body, n := range acked {
		if n != 1 {
			t.Fatalf("message %q was delivered and acked %d times", body, n)
		}
	}
	if s := q.Stats(); s.Total != 0 || s.InFlight != 0 {
		t.Fatalf("queue is not empty after every message was acked: %+v", s)
	}
}

func TestQueueLifecycleErrors(t *testing.T) {
	m := newTestManager(t, t.TempDir(), nil)
	mustCreate(t, m, Config{Name: "dup"})

	if _, err := m.CreateQueue(Config{Name: "dup"}); !errors.Is(err, ErrQueueExists) {
		t.Fatalf("duplicate create returned %v, want ErrQueueExists", err)
	}
	if _, err := m.Queue("missing"); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("lookup of a missing queue returned %v", err)
	}
	for _, bad := range []Config{
		{Name: ""},
		{Name: "has spaces"},
		{Name: "ok", Mode: "sideways"},
		{Name: "ok", MaxAttempts: -1},
		{Name: "ok", VisibilityTimeoutMS: -5},
	} {
		if _, err := m.CreateQueue(bad); !errors.Is(err, ErrInvalidQueueCfg) {
			t.Fatalf("config %+v was accepted (err=%v)", bad, err)
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	m := newTestManager(t, t.TempDir(), nil)
	q := mustCreate(t, m, Config{Name: "defaults"})
	cfg := q.Config()
	if cfg.Mode != FIFO {
		t.Fatalf("default mode is %q, want fifo", cfg.Mode)
	}
	if cfg.VisibilityTimeoutMS != DefaultVisibilityTimeout.Milliseconds() {
		t.Fatalf("default visibility timeout is %d", cfg.VisibilityTimeoutMS)
	}
	if cfg.MaxAttempts != DefaultMaxAttempts {
		t.Fatalf("default max attempts is %d", cfg.MaxAttempts)
	}
}
