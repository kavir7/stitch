package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kavir7/stitch/internal/wal"
)

// fingerprint renders the whole manager as a canonical string. Two recoveries
// of the same log must produce the same one, byte for byte.
func fingerprint(m *Manager) string {
	var b strings.Builder
	for _, q := range m.Queues() {
		q.mu.Lock()
		fmt.Fprintf(&b, "queue=%s cfg=%+v seq=%d\n", q.cfg.Name, q.cfg, q.seq)

		ids := make([]string, 0, len(q.msgs))
		for id := range q.msgs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			msg := q.msgs[id]
			fmt.Fprintf(&b, "  msg %s seq=%d prio=%d vis=%d att=%d body=%s dedup=%s\n",
				msg.ID, msg.Seq, msg.Priority, msg.VisibleAt, msg.Attempts, msg.Body, msg.DedupKey)
		}
		dlq := make([]string, 0, len(q.dlq))
		for _, msg := range q.dlq {
			dlq = append(dlq, msg.ID)
		}
		sort.Strings(dlq)
		fmt.Fprintf(&b, "  dlq %v\n", dlq)

		keys := make([]string, 0, len(q.dedup))
		for k := range q.dedup {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "  dedup %s -> %s\n", k, q.dedup[k].msg.ID)
		}
		fmt.Fprintf(&b, "  ready=%d delayed=%d\n", q.ready.Len(), q.delayed.Len())
		q.mu.Unlock()
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func reopen(t *testing.T, dir string, clock *testClock) *Manager {
	t.Helper()
	opts := Options{Dir: dir, Sync: wal.SyncGroup, Logger: log.New(io.Discard, "", 0)}
	if clock != nil {
		opts.Now = clock.Now
	}
	m, _, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return m
}

// The central claim: after a restart, every acked message is gone and every
// un-acked one is still there.
func TestRecoveryKeepsUnackedAndDropsAcked(t *testing.T) {
	dir := t.TempDir()
	clock := newTestClock()
	m := newTestManager(t, dir, clock)
	q := mustCreate(t, m, Config{Name: "jobs", Mode: FIFO, VisibilityTimeoutMS: 600000, MaxAttempts: 10})

	var all []string
	for i := 0; i < 200; i++ {
		all = append(all, mustEnqueue(t, q, fmt.Sprintf("body-%03d", i), 0, 0).ID)
	}

	leased := drain(t, q, 60)
	acked := make(map[string]bool)
	for i, d := range leased {
		switch {
		case i < 30:
			if err := q.Ack(d.Message.ID, d.LeaseToken); err != nil {
				t.Fatalf("ack: %v", err)
			}
			acked[d.Message.ID] = true
		case i < 45:
			if err := q.Nack(d.Message.ID, d.LeaseToken, 0); err != nil {
				t.Fatalf("nack: %v", err)
			}
		}
		// The remaining 15 stay leased, which is what a crash would leave.
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m2 := reopen(t, dir, clock)
	defer m2.Close()
	q2, err := m2.Queue("jobs")
	if err != nil {
		t.Fatalf("queue after recovery: %v", err)
	}

	got := make(map[string]bool)
	for _, id := range q2.IDs() {
		got[id] = true
	}
	for _, id := range all {
		if acked[id] && got[id] {
			t.Fatalf("acked message %s came back after recovery", id)
		}
		if !acked[id] && !got[id] {
			t.Fatalf("un-acked message %s did not survive recovery", id)
		}
	}
	if len(got) != len(all)-len(acked) {
		t.Fatalf("recovered %d messages, want %d", len(got), len(all)-len(acked))
	}
	if s := q2.Stats(); s.InFlight != 0 || s.Depth != 170 {
		t.Fatalf("leases should not survive a restart: %+v", s)
	}
}

// A message that was leased when the process died comes back for redelivery
// with its attempt count intact, which is the at-least-once contract.
func TestRecoveryReturnsInFlightMessagesWithAttempts(t *testing.T) {
	dir := t.TempDir()
	clock := newTestClock()
	m := newTestManager(t, dir, clock)
	q := mustCreate(t, m, Config{Name: "q", VisibilityTimeoutMS: 600000, MaxAttempts: 10})

	msg := mustEnqueue(t, q, "work", 0, 0)
	first := drain(t, q, 1)[0]
	if first.Message.Attempts != 1 {
		t.Fatalf("first delivery reported %d attempts", first.Message.Attempts)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m2 := reopen(t, dir, clock)
	defer m2.Close()
	q2, _ := m2.Queue("q")

	again, err := q2.Dequeue(context.Background(), 1, 0)
	if err != nil {
		t.Fatalf("dequeue after recovery: %v", err)
	}
	if len(again) != 1 || again[0].Message.ID != msg.ID {
		t.Fatal("the in-flight message was not redelivered after recovery")
	}
	if again[0].Message.Attempts != 2 {
		t.Fatalf("redelivery after recovery reported %d attempts, want 2", again[0].Message.Attempts)
	}
	if again[0].LeaseToken == first.LeaseToken {
		t.Fatal("the pre-crash lease token was reused")
	}
	if err := q2.Ack(msg.ID, first.LeaseToken); err == nil {
		t.Fatal("a lease token from before the restart was accepted")
	}
}

func TestRecoveryIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	clock := newTestClock()
	m := newTestManager(t, dir, clock)

	q := mustCreate(t, m, Config{Name: "mixed", Mode: LIFO, Priority: true, Delay: true, MaxAttempts: 2, DLQ: true, VisibilityTimeoutMS: 1000})
	for i := 0; i < 80; i++ {
		if _, _, err := q.Enqueue([]byte(fmt.Sprintf("b%02d", i)), i%5, time.Duration(i%3)*time.Second, fmt.Sprintf("key-%d", i%20)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	for _, d := range drain(t, q, 20) {
		if err := q.Nack(d.Message.ID, d.LeaseToken, 0); err != nil {
			t.Fatalf("nack: %v", err)
		}
	}
	// Push a few messages all the way into the dead-letter queue.
	for round := 0; round < 2; round++ {
		for _, d := range drain(t, q, 5) {
			if err := q.Nack(d.Message.ID, d.LeaseToken, 0); err != nil {
				t.Fatalf("nack: %v", err)
			}
		}
	}
	for _, d := range drain(t, q, 10) {
		if err := q.Ack(d.Message.ID, d.LeaseToken); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	first := reopen(t, dir, clock)
	fp1 := fingerprint(first)
	if err := first.Close(); err != nil {
		t.Fatalf("close first replay: %v", err)
	}

	second := reopen(t, dir, clock)
	fp2 := fingerprint(second)
	if err := second.Close(); err != nil {
		t.Fatalf("close second replay: %v", err)
	}

	if fp1 != fp2 {
		t.Fatalf("two replays of the same log produced different state:\n  %s\n  %s", fp1, fp2)
	}

	third := reopen(t, dir, clock)
	defer third.Close()
	if fp3 := fingerprint(third); fp3 != fp1 {
		t.Fatalf("a third replay diverged: %s", fp3)
	}
	q3, _ := third.Queue("mixed")
	if s := q3.Stats(); s.DLQ == 0 {
		t.Fatal("the dead-letter queue did not survive recovery")
	}
}

// Ordering has to survive a restart too: the sequence counter continues where
// it left off, so messages enqueued after recovery sort after the old ones.
func TestRecoveryPreservesOrdering(t *testing.T) {
	for _, mode := range []Mode{FIFO, LIFO} {
		t.Run(string(mode), func(t *testing.T) {
			dir := t.TempDir()
			m := newTestManager(t, dir, nil)
			q := mustCreate(t, m, Config{Name: "q", Mode: mode})
			for _, body := range []string{"a", "b", "c"} {
				mustEnqueue(t, q, body, 0, 0)
			}
			if err := m.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			m2 := reopen(t, dir, nil)
			defer m2.Close()
			q2, _ := m2.Queue("q")
			mustEnqueue(t, q2, "d", 0, 0)

			got := bodies(drain(t, q2, 10))
			want := []string{"a", "b", "c", "d"}
			if mode == LIFO {
				want = []string{"d", "c", "b", "a"}
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("%s order after recovery: %v, want %v", mode, got, want)
				}
			}
		})
	}
}

func TestRecoveryPreservesDedupKeys(t *testing.T) {
	dir := t.TempDir()
	clock := newTestClock()
	m := newTestManager(t, dir, clock)
	q := mustCreate(t, m, Config{Name: "q"})

	original, _, err := q.Enqueue([]byte("once"), 0, 0, "invoice-7")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m2 := reopen(t, dir, clock)
	defer m2.Close()
	q2, _ := m2.Queue("q")

	again, deduped, err := q2.Enqueue([]byte("once"), 0, 0, "invoice-7")
	if err != nil {
		t.Fatalf("enqueue after recovery: %v", err)
	}
	if !deduped || again.ID != original.ID {
		t.Fatalf("dedup key did not survive recovery: deduped=%v id=%s want %s", deduped, again.ID, original.ID)
	}
}

func TestRecoveryOfAnEmptyLog(t *testing.T) {
	dir := t.TempDir()
	m := newTestManager(t, dir, nil)
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	m2 := reopen(t, dir, nil)
	defer m2.Close()
	if len(m2.Queues()) != 0 {
		t.Fatalf("an empty log recovered %d queues", len(m2.Queues()))
	}
}

// A record naming a queue that was never created means the log is
// inconsistent, not merely torn. Recovery must refuse rather than invent a
// queue and carry on.
func TestRecoveryRejectsRecordsForUnknownQueues(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	l, _, err := wal.Open(wal.Options{Dir: walDir, Sync: wal.SyncAlways, Logger: log.New(io.Discard, "", 0)}, nil)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := appendJSON(l, wal.RecordEnqueue, enqueuePayload{Queue: "ghost", ID: "abc", Seq: 1}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	_, _, err = Open(Options{Dir: walDir, Sync: wal.SyncGroup, Logger: log.New(io.Discard, "", 0)})
	if err == nil {
		t.Fatal("recovery accepted a record for a queue that does not exist")
	}
	if !strings.Contains(err.Error(), "unknown queue") {
		t.Fatalf("error does not name the problem: %v", err)
	}
}

// The torn-tail path is covered in the wal package; this checks that the queue
// on top of it comes back consistent when the last record is lost, which is
// exactly what a kill mid-write produces.
func TestRecoveryAfterTornTail(t *testing.T) {
	dir := t.TempDir()
	m := newTestManager(t, dir, nil)
	q := mustCreate(t, m, Config{Name: "q"})
	for i := 0; i < 40; i++ {
		mustEnqueue(t, q, fmt.Sprintf("m%02d", i), 0, 0)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Chop the last record in half, the way a crash between the write and the
	// fsync would.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var last string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			last = filepath.Join(dir, e.Name())
		}
	}
	data, err := os.ReadFile(last)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(last, data[:len(data)-20], 0o644); err != nil {
		t.Fatal(err)
	}

	m2 := reopen(t, dir, nil)
	defer m2.Close()
	q2, _ := m2.Queue("q")
	s := q2.Stats()
	if s.Total != 39 {
		t.Fatalf("after losing the last record the queue holds %d messages, want 39", s.Total)
	}
	got := bodies(drain(t, q2, 100))
	for i, body := range got {
		if body != fmt.Sprintf("m%02d", i) {
			t.Fatalf("message %d after a torn tail is %q", i, body)
		}
	}
}
