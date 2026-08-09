package queue

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kavir7/stitch/internal/wal"
)

// The benchmarks measure the whole path a client pays for: build the record,
// append it, wait for the fsync that covers it, then apply it in memory. There
// is no mode here that skips the fsync, because there is no mode in the server
// that skips it either.
//
// Run them with a fixed iteration count so the percentiles are comparable:
//
//	go test -run '^$' -bench . -benchtime=1000x ./internal/queue/

func benchConcurrencies() []int { return []int{1, 8, 32} }

func newBenchManager(b *testing.B, mode wal.SyncMode) *Manager {
	b.Helper()
	m, _, err := Open(Options{
		Dir:    b.TempDir(),
		Sync:   mode,
		Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { m.Close() })
	return m
}

// runConcurrent spreads n operations over the given number of goroutines and
// returns every individual latency.
func runConcurrent(n, concurrency int, op func(i int) error) ([]time.Duration, time.Duration, error) {
	lat := make([]time.Duration, n)
	var (
		next atomic.Int64
		wg   sync.WaitGroup
		fail atomic.Value
	)
	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				t0 := time.Now()
				if err := op(i); err != nil {
					fail.Store(err)
					return
				}
				lat[i] = time.Since(t0)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if err, ok := fail.Load().(error); ok {
		return nil, 0, err
	}
	return lat, elapsed, nil
}

// report emits throughput, latency percentiles and the average group commit
// size. base subtracts any log activity from the setup phase, so a benchmark
// that pre-fills a queue does not report the fill's batching as its own.
func report(b *testing.B, m *Manager, base wal.Stats, lat []time.Duration, elapsed time.Duration) {
	b.ReportMetric(float64(len(lat))/elapsed.Seconds(), "msg/s")
	b.ReportMetric(percentileMS(lat, 0.50), "p50_ms")
	b.ReportMetric(percentileMS(lat, 0.99), "p99_ms")

	st := m.WALStats()
	perSync := 0.0
	if st.Batches > base.Batches {
		perSync = float64(st.Appends-base.Appends) / float64(st.Batches-base.Batches)
	}
	b.ReportMetric(perSync, "rec/fsync")
}

func percentileMS(lat []time.Duration, p float64) float64 {
	if len(lat) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(lat))
	copy(sorted, lat)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(p * float64(len(sorted)-1))
	return float64(sorted[idx]) / float64(time.Millisecond)
}

func BenchmarkEnqueue(b *testing.B) {
	for _, mode := range []wal.SyncMode{wal.SyncAlways, wal.SyncGroup} {
		for _, c := range benchConcurrencies() {
			b.Run(fmt.Sprintf("%s/c%d", mode, c), func(b *testing.B) {
				m := newBenchManager(b, mode)
				q, err := m.CreateQueue(Config{Name: "bench", VisibilityTimeoutMS: 600000, MaxAttempts: 100})
				if err != nil {
					b.Fatal(err)
				}
				body := []byte("a benchmark message body of roughly realistic size, 96 bytes give or take a few")

				b.ResetTimer()
				lat, elapsed, err := runConcurrent(b.N, c, func(int) error {
					_, _, err := q.Enqueue(body, 0, 0, "")
					return err
				})
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				report(b, m, wal.Stats{}, lat, elapsed)
			})
		}
	}
}

func BenchmarkDequeue(b *testing.B) {
	for _, mode := range []wal.SyncMode{wal.SyncAlways, wal.SyncGroup} {
		for _, c := range benchConcurrencies() {
			b.Run(fmt.Sprintf("%s/c%d", mode, c), func(b *testing.B) {
				m := newBenchManager(b, mode)
				q, err := m.CreateQueue(Config{Name: "bench", VisibilityTimeoutMS: 600000, MaxAttempts: 100})
				if err != nil {
					b.Fatal(err)
				}
				body := []byte("a benchmark message body of roughly realistic size, 96 bytes give or take a few")
				for i := 0; i < b.N; i++ {
					if _, _, err := q.Enqueue(body, 0, 0, ""); err != nil {
						b.Fatal(err)
					}
				}
				// Only the dequeues count; the fill above is setup.
				fillStats := m.WALStats()

				ctx := context.Background()
				b.ResetTimer()
				lat, elapsed, err := runConcurrent(b.N, c, func(int) error {
					out, err := q.Dequeue(ctx, 1, 0)
					if err != nil {
						return err
					}
					if len(out) == 0 {
						return fmt.Errorf("the queue ran dry before %d dequeues completed", b.N)
					}
					return nil
				})
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}

				report(b, m, fillStats, lat, elapsed)
			})
		}
	}
}
