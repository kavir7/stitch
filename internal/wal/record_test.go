package wal

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		typ     RecordType
		payload []byte
	}{
		{"empty payload", RecordCheckpoint, []byte{}},
		{"small", RecordEnqueue, []byte("hello")},
		{"binary with nulls", RecordLease, []byte{0, 1, 2, 0, 0, 255, 0}},
		{"large", RecordAck, bytes.Repeat([]byte("x"), 100000)},
		{"nack", RecordNack, []byte(`{"id":"abc"}`)},
		{"lease expire", RecordLeaseExpire, []byte("le")},
		{"dlq move", RecordDLQMove, []byte("dlq")},
		{"queue create", RecordQueueCreate, []byte("q")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := appendRecord(nil, tc.typ, tc.payload)
			if int64(len(buf)) != encodedSize(tc.payload) {
				t.Fatalf("encoded size %d, encodedSize said %d", len(buf), encodedSize(tc.payload))
			}
			rec, n, err := decodeRecord(buf)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if n != len(buf) {
				t.Fatalf("consumed %d bytes, wrote %d", n, len(buf))
			}
			if rec.Type != tc.typ {
				t.Fatalf("type %v, want %v", rec.Type, tc.typ)
			}
			if !bytes.Equal(rec.Payload, tc.payload) {
				t.Fatalf("payload mismatch: got %d bytes, want %d", len(rec.Payload), len(tc.payload))
			}
		})
	}
}

func TestRecordRoundTripSequence(t *testing.T) {
	payloads := [][]byte{[]byte("a"), {}, []byte("ccc"), bytes.Repeat([]byte("d"), 5000)}
	var buf []byte
	for i, p := range payloads {
		buf = appendRecord(buf, RecordType(i%8+1), p)
	}

	off := 0
	for i, want := range payloads {
		rec, n, err := decodeRecord(buf[off:])
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if rec.Type != RecordType(i%8+1) {
			t.Fatalf("record %d: type %v", i, rec.Type)
		}
		if !bytes.Equal(rec.Payload, want) {
			t.Fatalf("record %d: payload mismatch", i)
		}
		off += n
	}
	if off != len(buf) {
		t.Fatalf("consumed %d of %d bytes", off, len(buf))
	}
}

// decodeRecord must copy out of the source buffer; the writer reuses its
// scratch space between batches and recovery reuses the mmap-like file slice.
func TestDecodeCopiesPayload(t *testing.T) {
	buf := appendRecord(nil, RecordEnqueue, []byte("original"))
	rec, _, err := decodeRecord(buf)
	if err != nil {
		t.Fatal(err)
	}
	copy(buf[headerSize:], "MANGLED!")
	if string(rec.Payload) != "original" {
		t.Fatalf("payload aliased the source buffer: %q", rec.Payload)
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	good := appendRecord(nil, RecordEnqueue, []byte("payload-under-test"))

	corrupt := map[string]func([]byte) []byte{
		"truncated header": func(b []byte) []byte { return b[:headerSize-1] },
		"empty":            func(b []byte) []byte { return b[:0] },
		"truncated payload": func(b []byte) []byte {
			return b[:len(b)-1]
		},
		"bad magic": func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[0:4], 0xDEADBEEF)
			return b
		},
		"flipped payload bit": func(b []byte) []byte {
			b[headerSize] ^= 0x01
			return b
		},
		"flipped type": func(b []byte) []byte {
			b[12] = byte(RecordAck)
			return b
		},
		"invalid type": func(b []byte) []byte {
			b[12] = 99
			return b
		},
		"length beyond buffer": func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[4:8], uint32(len(b)))
			return b
		},
		"absurd length": func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[4:8], maxRecordSize+1)
			return b
		},
		"zeroed": func(b []byte) []byte {
			for i := range b {
				b[i] = 0
			}
			return b
		},
	}

	for name, mangle := range corrupt {
		t.Run(name, func(t *testing.T) {
			b := make([]byte, len(good))
			copy(b, good)
			if _, _, err := decodeRecord(mangle(b)); err == nil {
				t.Fatal("decoded a corrupt record without error")
			}
		})
	}
}

// A flipped bit anywhere in the frame must be caught. This is the property the
// torn-tail recovery path relies on.
func TestChecksumCatchesEverySingleBitFlip(t *testing.T) {
	good := appendRecord(nil, RecordEnqueue, []byte("the quick brown fox"))
	for i := range good {
		for bit := 0; bit < 8; bit++ {
			b := make([]byte, len(good))
			copy(b, good)
			b[i] ^= 1 << bit
			if _, _, err := decodeRecord(b); err == nil {
				t.Fatalf("bit %d of byte %d flipped undetected", bit, i)
			}
		}
	}
}

func TestRecordTypeString(t *testing.T) {
	if RecordEnqueue.String() != "ENQUEUE" || RecordDLQMove.String() != "DLQ_MOVE" {
		t.Fatal("record type names do not match the on-disk vocabulary")
	}
	if RecordType(0).valid() || RecordType(200).valid() {
		t.Fatal("out-of-range record type reported valid")
	}
}
