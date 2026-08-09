package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// RecordType identifies what a payload means to the layer above the log. The
// WAL itself never interprets payloads; it only frames, checksums and orders
// them.
type RecordType uint8

const (
	RecordEnqueue RecordType = iota + 1
	RecordLease
	RecordAck
	RecordNack
	RecordLeaseExpire
	RecordDLQMove
	RecordQueueCreate
	RecordCheckpoint
)

func (t RecordType) String() string {
	switch t {
	case RecordEnqueue:
		return "ENQUEUE"
	case RecordLease:
		return "LEASE"
	case RecordAck:
		return "ACK"
	case RecordNack:
		return "NACK"
	case RecordLeaseExpire:
		return "LEASE_EXPIRE"
	case RecordDLQMove:
		return "DLQ_MOVE"
	case RecordQueueCreate:
		return "QUEUE_CREATE"
	case RecordCheckpoint:
		return "CHECKPOINT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(t))
	}
}

func (t RecordType) valid() bool {
	return t >= RecordEnqueue && t <= RecordCheckpoint
}

// Record is one framed entry in the log.
type Record struct {
	Type    RecordType
	Payload []byte
}

// Frame layout: [magic u32][payload_len u32][crc32c u32][type u8][payload...]
//
// The CRC covers the type byte and the payload but not the length, so a
// corrupted length is caught by bounds checks rather than by the checksum.
// That is deliberate: the length has to be trusted before there is anything to
// checksum, so it is validated against a hard ceiling and against the bytes
// actually remaining in the segment.
const (
	recordMagic   uint32 = 0x5157414C // "QWAL"
	headerSize           = 13
	maxRecordSize        = 16 << 20
)

var (
	// errTornRecord means the tail of the segment is incomplete or damaged in
	// a way consistent with a crash mid-write. Recovery truncates there.
	errTornRecord = errors.New("wal: torn record")

	// ErrRecordTooLarge is returned by Append for payloads over the frame limit.
	ErrRecordTooLarge = errors.New("wal: record exceeds maximum size")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// checksum computes the CRC32C over the type byte followed by the payload.
func checksum(typ RecordType, payload []byte) uint32 {
	c := crc32.Update(0, castagnoli, []byte{byte(typ)})
	return crc32.Update(c, castagnoli, payload)
}

// encodedSize is the number of bytes appendRecord will add for this payload.
func encodedSize(payload []byte) int64 {
	return int64(headerSize + len(payload))
}

// appendRecord appends the framed record to dst and returns the extended slice.
// It exists in this shape so the group-commit writer can pack a whole batch
// into one buffer and issue a single write syscall.
func appendRecord(dst []byte, typ RecordType, payload []byte) []byte {
	var h [headerSize]byte
	binary.BigEndian.PutUint32(h[0:4], recordMagic)
	binary.BigEndian.PutUint32(h[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(h[8:12], checksum(typ, payload))
	h[12] = byte(typ)
	dst = append(dst, h[:]...)
	return append(dst, payload...)
}

// decodeRecord reads one record from the front of buf, which must be the
// remaining bytes of a segment. It returns the record and the number of bytes
// consumed. A torn or corrupt frame yields errTornRecord.
func decodeRecord(buf []byte) (Record, int, error) {
	if len(buf) < headerSize {
		return Record{}, 0, errTornRecord
	}
	if binary.BigEndian.Uint32(buf[0:4]) != recordMagic {
		return Record{}, 0, errTornRecord
	}
	length := int(binary.BigEndian.Uint32(buf[4:8]))
	if length > maxRecordSize || headerSize+length > len(buf) {
		return Record{}, 0, errTornRecord
	}
	typ := RecordType(buf[12])
	payload := buf[headerSize : headerSize+length]
	if !typ.valid() {
		return Record{}, 0, errTornRecord
	}
	if binary.BigEndian.Uint32(buf[8:12]) != checksum(typ, payload) {
		return Record{}, 0, errTornRecord
	}
	out := make([]byte, length)
	copy(out, payload)
	return Record{Type: typ, Payload: out}, headerSize + length, nil
}
