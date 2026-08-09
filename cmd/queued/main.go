// Command queued runs the queue server: one process, one write-ahead log
// directory, one HTTP port, no external dependencies.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kavir7/stitch/internal/api"
	"github.com/kavir7/stitch/internal/queue"
	"github.com/kavir7/stitch/internal/wal"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dir := flag.String("data", "data/wal", "directory holding the write-ahead log")
	fsyncMode := flag.String("fsync", "group", "fsync policy: always (one fsync per record) or group (one per batch)")
	segmentSize := flag.Int64("segment-size", wal.DefaultSegmentSize, "rotate the log after this many bytes")
	unsafeDemo := flag.Bool("unsafe-demo", false, "enable POST /debug/crash, which SIGKILLs this process")
	flag.Parse()

	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)

	var sync wal.SyncMode
	switch *fsyncMode {
	case string(wal.SyncAlways):
		sync = wal.SyncAlways
	case string(wal.SyncGroup):
		sync = wal.SyncGroup
	default:
		logger.Fatalf("queued: -fsync must be always or group, got %q", *fsyncMode)
	}

	start := time.Now()
	mgr, info, err := queue.Open(queue.Options{
		Dir:         *dir,
		Sync:        sync,
		SegmentSize: *segmentSize,
		Logger:      logger,
	})
	if err != nil {
		logger.Fatalf("queued: recovery failed: %v", err)
	}
	logger.Printf("queued: replayed %d record(s) in %v (torn tail truncated: %v)", info.Records, time.Since(start).Round(time.Millisecond), info.Truncated)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.New(mgr, api.Options{UnsafeDemo: *unsafeDemo, Logger: logger}),
		ReadHeaderTimeout: 10 * time.Second,
		// No write timeout: a dequeue with wait_ms legitimately holds the
		// connection open while it waits for work.
	}
	if *unsafeDemo {
		logger.Print("queued: --unsafe-demo is on, POST /debug/crash will SIGKILL this process")
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	errc := make(chan error, 1)
	go func() {
		logger.Printf("queued: listening on %s, log in %s, fsync=%s", *addr, *dir, sync)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("queued: %v", err)
		}
	case sig := <-stop:
		logger.Printf("queued: %v received, shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Printf("queued: shutdown: %v", err)
		}
	}

	// Closing the manager drains the writer and closes the segment. Skipping
	// this would still be safe, which is the point of the log.
	if err := mgr.Close(); err != nil {
		logger.Fatalf("queued: closing the log: %v", err)
	}
	logger.Print("queued: stopped")
}
