package wal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testOptions(dir string, mode SyncMode) Options {
	return Options{
		Dir:    dir,
		Sync:   mode,
		Logger: log.New(io.Discard, "", 0),
	}
}

// openAndCollect replays dir and returns every record the log yields, plus the
// recovery report. The returned log is closed before the function returns.
func openAndCollect(t *testing.T, opts Options) ([]Record, RecoveryInfo) {
	t.Helper()
	var got []Record
	l, info, err := Open(opts, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return got, info
}

func TestAppendThenRecover(t *testing.T) {
	for _, mode := range []SyncMode{SyncAlways, SyncGroup} {
		t.Run(string(mode), func(t *testing.T) {
			dir := t.TempDir()
			opts := testOptions(dir, mode)

			l, info, err := Open(opts, nil)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if info.Records != 0 || info.Segments != 0 {
				t.Fatalf("fresh directory reported %+v", info)
			}
			for i := 0; i < 200; i++ {
				if err := l.Append(RecordEnqueue, []byte(fmt.Sprintf("msg-%03d", i))); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
			}
			if err := l.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			got, info := openAndCollect(t, opts)
			if len(got) != 200 {
				t.Fatalf("recovered %d records, want 200", len(got))
			}
			if info.Truncated {
				t.Fatal("clean shutdown reported a torn tail")
			}
			for i, rec := range got {
				want := fmt.Sprintf("msg-%03d", i)
				if rec.Type != RecordEnqueue || string(rec.Payload) != want {
					t.Fatalf("record %d = (%v, %q), want (ENQUEUE, %q)", i, rec.Type, rec.Payload, want)
				}
			}
		})
	}
}

func TestAppendAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir, SyncGroup)

	for round := 0; round < 3; round++ {
		l, _, err := Open(opts, nil)
		if err != nil {
			t.Fatalf("round %d open: %v", round, err)
		}
		for i := 0; i < 5; i++ {
			if err := l.Append(RecordAck, []byte(fmt.Sprintf("r%d-%d", round, i))); err != nil {
				t.Fatalf("round %d append: %v", round, err)
			}
		}
		if err := l.Close(); err != nil {
			t.Fatalf("round %d close: %v", round, err)
		}
	}

	got, _ := openAndCollect(t, opts)
	if len(got) != 15 {
		t.Fatalf("recovered %d records across three sessions, want 15", len(got))
	}
	if string(got[0].Payload) != "r0-0" || string(got[14].Payload) != "r2-4" {
		t.Fatalf("records out of order: first %q last %q", got[0].Payload, got[14].Payload)
	}
}

func TestConcurrentAppends(t *testing.T) {
	const writers, perWriter = 32, 100

	dir := t.TempDir()
	opts := testOptions(dir, SyncGroup)
	l, _, err := Open(opts, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := l.Append(RecordEnqueue, []byte(fmt.Sprintf("w%02d-i%03d", w, i))); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, _ := openAndCollect(t, opts)
	if len(got) != writers*perWriter {
		t.Fatalf("recovered %d records, want %d", len(got), writers*perWriter)
	}
	seen := make(map[string]int, len(got))
	for _, rec := range got {
		seen[string(rec.Payload)]++
	}
	if len(seen) != writers*perWriter {
		t.Fatalf("%d distinct payloads recovered, want %d", len(seen), writers*perWriter)
	}
	for payload, n := range seen {
		if n != 1 {
			t.Fatalf("payload %q appears %d times", payload, n)
		}
	}
	// Each writer's own records must stay in the order that writer issued
	// them, even though the batch interleaves records from all writers.
	last := make(map[string]int, writers)
	for _, rec := range got {
		var w, i int
		if _, err := fmt.Sscanf(string(rec.Payload), "w%d-i%d", &w, &i); err != nil {
			t.Fatalf("unparseable payload %q", rec.Payload)
		}
		key := fmt.Sprintf("w%02d", w)
		if prev, ok := last[key]; ok && i != prev+1 {
			t.Fatalf("writer %d records out of order: %d followed %d", w, i, prev)
		}
		last[key] = i
	}
}

// A batch is never split across segments, so the segment size is a soft limit;
// what matters is that rotation happens and that replay stitches the segments
// back together in order.
func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir, SyncAlways)
	opts.SegmentSize = 1 << 10

	payload := make([]byte, 100)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	l, _, err := Open(opts, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 100; i++ {
		rec := make([]byte, len(payload))
		copy(rec, payload)
		rec[0] = byte(i)
		if err := l.Append(RecordEnqueue, rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	indexes, err := listSegments(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(indexes) < 5 {
		t.Fatalf("expected rotation into several segments, got %d", len(indexes))
	}
	for i, idx := range indexes {
		if idx != uint64(i) {
			t.Fatalf("segment indexes not contiguous: %v", indexes)
		}
		st, err := os.Stat(filepath.Join(dir, segmentName(idx)))
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() > opts.SegmentSize+encodedSize(payload) {
			t.Fatalf("segment %d is %d bytes, over the soft limit by more than one record", idx, st.Size())
		}
	}

	got, info := openAndCollect(t, opts)
	if len(got) != 100 {
		t.Fatalf("recovered %d records, want 100", len(got))
	}
	if info.Segments != len(indexes) {
		t.Fatalf("recovery saw %d segments, directory has %d", info.Segments, len(indexes))
	}
	for i, rec := range got {
		if rec.Payload[0] != byte(i) {
			t.Fatalf("record %d out of order after rotation", i)
		}
	}
}

// Under concurrency the writer must fold the records that queue up behind an
// in-flight fsync into the next batch, so fsyncs come out well below appends.
func TestGroupCommitBatchesUnderConcurrency(t *testing.T) {
	const n = 256

	dir := t.TempDir()
	l, _, err := Open(testOptions(dir, SyncGroup), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer l.Close()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := l.Append(RecordEnqueue, []byte(fmt.Sprintf("concurrent-%03d", i))); err != nil {
				t.Errorf("append: %v", err)
			}
		}(i)
	}
	wg.Wait()

	st := l.Stats()
	if st.Appends != n {
		t.Fatalf("counted %d appends, issued %d", st.Appends, n)
	}
	if st.Batches >= n {
		t.Fatalf("%d appends cost %d fsyncs; nothing was grouped", n, st.Batches)
	}
	t.Logf("%d appends in %d fsyncs (%.1f records per sync)", st.Appends, st.Batches, float64(st.Appends)/float64(st.Batches))
}

// An idle writer must not wait for company. Sequential appends in group mode
// have to cost the same as fsync=always, one sync each and no added delay.
// This is the regression test for the fixed batch window that used to make
// group commit slower than fsync=always for a single producer.
func TestGroupCommitDoesNotDelayIdleWriter(t *testing.T) {
	const n = 20

	always := timeSequentialAppends(t, SyncAlways, n)
	group := timeSequentialAppends(t, SyncGroup, n)
	t.Logf("sequential %d appends: always=%v group=%v", n, always, group)

	if group > always*2 {
		t.Fatalf("group commit took %v for %d sequential appends against %v for fsync=always; the writer is waiting for a batch that will never arrive", group, n, always)
	}
}

func timeSequentialAppends(t *testing.T, mode SyncMode, n int) time.Duration {
	t.Helper()
	dir := t.TempDir()
	l, _, err := Open(testOptions(dir, mode), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer l.Close()

	start := time.Now()
	for i := 0; i < n; i++ {
		if err := l.Append(RecordEnqueue, []byte(fmt.Sprintf("seq-%03d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	elapsed := time.Since(start)

	if st := l.Stats(); st.Batches != uint64(n) {
		t.Fatalf("%s: %d sequential appends produced %d fsyncs, want one each", mode, n, st.Batches)
	}
	return elapsed
}

func TestAppendAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(testOptions(dir, SyncGroup), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.Append(RecordEnqueue, []byte("before")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := l.Append(RecordEnqueue, []byte("after")); err != ErrClosed {
		t.Fatalf("append after close returned %v, want ErrClosed", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestAppendRejectsBadInput(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(testOptions(dir, SyncGroup), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer l.Close()

	if err := l.Append(RecordType(0), []byte("x")); err == nil {
		t.Fatal("accepted an invalid record type")
	}
	if err := l.Append(RecordEnqueue, make([]byte, maxRecordSize+1)); err != ErrRecordTooLarge {
		t.Fatalf("oversize append returned %v, want ErrRecordTooLarge", err)
	}
}

// Open must not accept writes until the caller's fold has consumed every
// record, otherwise recovered state could be observed mid-replay.
func TestApplyErrorAbortsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir, SyncAlways)

	l, _, err := Open(opts, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := l.Append(RecordEnqueue, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	applied := 0
	boom := fmt.Errorf("state machine rejected the record")
	if _, _, err := Open(opts, func(Record) error {
		applied++
		if applied == 2 {
			return boom
		}
		return nil
	}); err == nil {
		t.Fatal("Open succeeded even though apply failed")
	}
}
