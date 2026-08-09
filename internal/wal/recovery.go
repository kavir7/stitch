package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

// RecoveryInfo describes what Open found on disk.
type RecoveryInfo struct {
	Segments  int
	Records   int
	Truncated bool
	// TruncatedAt is the byte offset within the final segment where the torn
	// tail began, valid only when Truncated is true.
	TruncatedAt int64
}

// replay walks every segment in order, applying valid records. A frame that
// fails its bounds or CRC check in the final segment is treated as a torn tail
// from a crash: the segment is truncated at that offset. The same failure in
// any earlier segment is real corruption, because a crash can only ever damage
// the file that was being appended to, so replay refuses to continue.
func replay(dir string, apply func(Record) error) (info RecoveryInfo, lastIndex uint64, lastSize int64, err error) {
	indexes, err := listSegments(dir)
	if err != nil {
		return info, 0, 0, err
	}
	info.Segments = len(indexes)
	for i, idx := range indexes {
		path := filepath.Join(dir, segmentName(idx))
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return info, 0, 0, rerr
		}
		off := 0
		for off < len(data) {
			rec, n, derr := decodeRecord(data[off:])
			if derr != nil {
				if i != len(indexes)-1 {
					return info, 0, 0, fmt.Errorf("wal: corrupt record in segment %s at offset %d, and it is not the final segment", segmentName(idx), off)
				}
				if terr := truncateSegment(path, int64(off)); terr != nil {
					return info, 0, 0, terr
				}
				info.Truncated = true
				info.TruncatedAt = int64(off)
				data = data[:off]
				break
			}
			if apply != nil {
				if aerr := apply(rec); aerr != nil {
					return info, 0, 0, fmt.Errorf("wal: apply %s at %s+%d: %w", rec.Type, segmentName(idx), off, aerr)
				}
			}
			info.Records++
			off += n
		}
		lastIndex, lastSize = idx, int64(len(data))
	}
	return info, lastIndex, lastSize, nil
}

// truncateSegment cuts the segment back to size and fsyncs it, so the torn
// bytes cannot reappear after a second crash.
func truncateSegment(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}
