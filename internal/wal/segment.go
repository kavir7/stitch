package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const segmentSuffix = ".log"

func segmentName(index uint64) string {
	return fmt.Sprintf("%08d%s", index, segmentSuffix)
}

func segmentPath(dir string, index uint64) string {
	return filepath.Join(dir, segmentName(index))
}

// listSegments returns the indexes of every segment file in dir, ascending.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var indexes []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentSuffix) {
			continue
		}
		base := strings.TrimSuffix(e.Name(), segmentSuffix)
		idx, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	return indexes, nil
}

// createSegment creates a new empty segment and fsyncs the directory so the
// new directory entry survives a crash. Without the directory sync the file
// contents can be durable while the name that points at them is not.
func createSegment(dir string, index uint64) (*os.File, error) {
	path := filepath.Join(dir, segmentName(index))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
