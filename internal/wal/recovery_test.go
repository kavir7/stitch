package wal

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stateHash folds a record stream into a digest. Recovery is only useful if
// the fold is deterministic, so two replays of the same bytes must produce the
// same hash.
type stateHash struct {
	h     [32]byte
	count int
}

func (s *stateHash) apply(r Record) error {
	sum := sha256.New()
	sum.Write(s.h[:])
	sum.Write([]byte{byte(r.Type)})
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(r.Payload)))
	sum.Write(n[:])
	sum.Write(r.Payload)
	copy(s.h[:], sum.Sum(nil))
	s.count++
	return nil
}

func (s *stateHash) String() string { return hex.EncodeToString(s.h[:]) }

// writeRecords fills a fresh log with n records and closes it cleanly.
func writeRecords(t *testing.T, opts Options, n int) {
	t.Helper()
	l, _, err := Open(opts, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := l.Append(RecordEnqueue, []byte(fmt.Sprintf("record-%04d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func lastSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	indexes, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) == 0 {
		t.Fatal("no segments in directory")
	}
	return filepath.Join(dir, segmentName(indexes[len(indexes)-1]))
}

// damage rewrites the tail of the last segment, simulating what a crash mid
// write leaves behind.
func damage(t *testing.T, dir string, fn func(data []byte) []byte) {
	t.Helper()
	path := lastSegmentPath(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, fn(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryTruncatesTornTail(t *testing.T) {
	recordLen := int(encodedSize([]byte("record-0000")))

	cases := []struct {
		name    string
		damage  func([]byte) []byte
		survive int
	}{
		{
			name:    "record cut in half",
			damage:  func(b []byte) []byte { return b[:len(b)-recordLen/2] },
			survive: 49,
		},
		{
			name:    "only the magic made it to disk",
			damage:  func(b []byte) []byte { return b[:len(b)-recordLen+4] },
			survive: 49,
		},
		{
			name:    "one byte of the next header",
			damage:  func(b []byte) []byte { return append(b, 0x51) },
			survive: 50,
		},
		{
			name:    "trailing zeroes from a partial block write",
			damage:  func(b []byte) []byte { return append(b, make([]byte, 300)...) },
			survive: 50,
		},
		{
			name: "bit flip in the last record",
			damage: func(b []byte) []byte {
				b[len(b)-1] ^= 0x08
				return b
			},
			survive: 49,
		},
		{
			name: "corrupt length in the last header",
			damage: func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[len(b)-recordLen+4:], 1<<30)
				return b
			},
			survive: 49,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := testOptions(dir, SyncAlways)
			writeRecords(t, opts, 50)
			damage(t, dir, tc.damage)

			var st stateHash
			l, info, err := Open(opts, st.apply)
			if err != nil {
				t.Fatalf("open after damage: %v", err)
			}
			if st.count != tc.survive {
				t.Fatalf("recovered %d records, want %d", st.count, tc.survive)
			}
			if !info.Truncated {
				t.Fatal("recovery did not report a truncation")
			}

			// The torn bytes must be gone from disk, not merely skipped in
			// memory, or the next append would land after them and the log
			// would never parse again.
			st2, err := os.Stat(lastSegmentPath(t, dir))
			if err != nil {
				t.Fatal(err)
			}
			if st2.Size() != info.TruncatedAt {
				t.Fatalf("segment is %d bytes after recovery, expected truncation to %d", st2.Size(), info.TruncatedAt)
			}

			// And the log must still be writable afterwards.
			if err := l.Append(RecordAck, []byte("after-recovery")); err != nil {
				t.Fatalf("append after recovery: %v", err)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			got, info2 := openAndCollect(t, opts)
			if info2.Truncated {
				t.Fatal("second recovery still saw a torn tail")
			}
			if len(got) != tc.survive+1 {
				t.Fatalf("after re-append, recovered %d records, want %d", len(got), tc.survive+1)
			}
			if string(got[len(got)-1].Payload) != "after-recovery" {
				t.Fatalf("last record is %q", got[len(got)-1].Payload)
			}
		})
	}
}

// Replaying the same bytes twice has to produce the same state, and a replay
// that follows a truncation has to agree with the one that performed it.
func TestRecoveryIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir, SyncGroup)
	writeRecords(t, opts, 500)

	var first, second stateHash
	l1, _, err := Open(opts, first.apply)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}
	l2, _, err := Open(opts, second.apply)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}

	if first.count != 500 || second.count != 500 {
		t.Fatalf("replays saw %d and %d records, want 500 each", first.count, second.count)
	}
	if first.String() != second.String() {
		t.Fatalf("state hash differs between replays:\n  %s\n  %s", first.String(), second.String())
	}
}

func TestRecoveryIsIdempotentAfterTruncation(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir, SyncGroup)
	writeRecords(t, opts, 200)
	damage(t, dir, func(b []byte) []byte { return b[:len(b)-7] })

	var first, second stateHash
	l1, info1, err := Open(opts, first.apply)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}
	if !info1.Truncated {
		t.Fatal("first recovery did not truncate")
	}

	l2, info2, err := Open(opts, second.apply)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	if info2.Truncated {
		t.Fatal("second recovery truncated again; the first one did not persist")
	}
	if first.String() != second.String() {
		t.Fatalf("state hash differs after truncation:\n  %s\n  %s", first.String(), second.String())
	}
	if first.count != 199 {
		t.Fatalf("recovered %d records, want 199", first.count)
	}
}

func TestRecoverySpansSegments(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir, SyncAlways)
	opts.SegmentSize = 512
	writeRecords(t, opts, 300)

	indexes, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) < 3 {
		t.Fatalf("expected several segments, got %d", len(indexes))
	}

	got, info := openAndCollect(t, opts)
	if len(got) != 300 {
		t.Fatalf("recovered %d records across %d segments, want 300", len(got), info.Segments)
	}
	for i, rec := range got {
		if want := fmt.Sprintf("record-%04d", i); string(rec.Payload) != want {
			t.Fatalf("record %d is %q, want %q", i, rec.Payload, want)
		}
	}
}

// A crash can only damage the segment being appended to. Damage anywhere else
// is real corruption, and silently truncating there would throw away good
// records that follow it.
func TestRecoveryRefusesCorruptionBeforeTheFinalSegment(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir, SyncAlways)
	opts.SegmentSize = 512
	writeRecords(t, opts, 300)

	path := filepath.Join(dir, segmentName(0))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-3] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err = Open(opts, nil)
	if err == nil {
		t.Fatal("Open accepted corruption in a non-final segment")
	}
	if !strings.Contains(err.Error(), "not the final segment") {
		t.Fatalf("error does not identify the problem: %v", err)
	}
}

func TestRecoveryFromEmptyAndFreshDirectories(t *testing.T) {
	t.Run("nonexistent directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nested", "wal")
		got, info := openAndCollect(t, testOptions(dir, SyncGroup))
		if len(got) != 0 || info.Records != 0 {
			t.Fatalf("fresh directory yielded %d records", len(got))
		}
		if _, err := os.Stat(filepath.Join(dir, segmentName(0))); err != nil {
			t.Fatalf("first segment was not created: %v", err)
		}
	})

	t.Run("empty segment left by a crash before the first write", func(t *testing.T) {
		dir := t.TempDir()
		opts := testOptions(dir, SyncGroup)
		l, _, err := Open(opts, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		got, info := openAndCollect(t, opts)
		if len(got) != 0 || info.Truncated {
			t.Fatalf("empty segment mishandled: %d records, truncated=%v", len(got), info.Truncated)
		}
	})
}
