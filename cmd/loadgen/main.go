// Command loadgen drives the crash test. It runs in two phases against a
// server it does not control: "crash" produces load and records exactly what
// the server confirmed, then dies when the server does; "verify" runs after
// the restart and reconciles what came back against those records.
//
// The distinction that makes the result meaningful is between an operation the
// server confirmed and one whose answer was lost with the connection. A
// confirmed ack must be gone after recovery. An ack whose response never
// arrived may legitimately be either, and is counted separately rather than
// quietly assumed one way.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: loadgen crash|verify [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "crash":
		runCrash(os.Args[2:])
	case "verify":
		runVerify(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "loadgen: unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

// --- record files ---

// idFile is an append-only list of message IDs, one per line. The loadgen is
// not the process being killed, so a flush at exit is enough; it flushes as it
// goes anyway so a truncated run still yields usable evidence.
type idFile struct {
	mu sync.Mutex
	f  *os.File
	n  int
}

func createIDFile(path string) (*idFile, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &idFile{f: f}, nil
}

func (w *idFile) add(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fmt.Fprintln(w.f, id)
	w.n++
}

func (w *idFile) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func (w *idFile) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

func readIDs(path string) (map[string]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool)
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out[line] = true
		}
	}
	return out, nil
}

// --- HTTP plumbing ---

type client struct {
	base string
	http *http.Client
	// dead is set the first time a request fails at the network level, which
	// is how the workers learn the server was killed.
	dead atomic.Bool
}

// errServerGone marks a network-level failure, as opposed to an HTTP error.
var errServerGone = errors.New("server is gone")

func newClient(addr string) *client {
	return &client{
		base: "http://" + addr,
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 256,
			},
		},
	}
}

// post returns the decoded response, or errServerGone if the connection
// failed. A non-2xx reply is an error but not a death.
func (c *client) post(path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	resp, err := c.http.Post(c.base+path, "application/json", r)
	if err != nil {
		c.dead.Store(true)
		return fmt.Errorf("%w: %v", errServerGone, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *client) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := c.http.Get(c.base + "/healthz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("server at %s did not become healthy within %v", c.base, timeout)
}

type enqueueResponse struct {
	ID      string `json:"id"`
	Deduped bool   `json:"deduped"`
}

type message struct {
	ID         string `json:"id"`
	Body       string `json:"body"`
	Attempts   int    `json:"attempts"`
	LeaseToken string `json:"lease_token"`
}

type dequeueResponse struct {
	Messages []message `json:"messages"`
}

// --- crash phase ---

func runCrash(args []string) {
	fs := flag.NewFlagSet("crash", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:18123", "server address")
	name := fs.String("queue", "crashtest", "queue name")
	count := fs.Int("n", 100000, "messages to enqueue before consumption starts")
	producers := fs.Int("producers", 32, "concurrent producers")
	consumers := fs.Int("consumers", 16, "concurrent consumers")
	batch := fs.Int("batch", 25, "messages per dequeue call")
	out := fs.String("out", ".crashtest", "directory for the record files")
	fs.Parse(args)

	c := newClient(*addr)
	if err := c.waitReady(30 * time.Second); err != nil {
		fatal(err)
	}

	// A long visibility timeout keeps leases from lapsing during the run, so
	// anything redelivered was redelivered because of the crash.
	err := c.post("/queues", map[string]any{
		"name":                  *name,
		"mode":                  "fifo",
		"visibility_timeout_ms": 600000,
		"max_attempts":          100,
		"dlq":                   true,
	}, nil)
	if err != nil {
		fatal(fmt.Errorf("creating queue: %w", err))
	}

	enqueued, err := createIDFile(filepath.Join(*out, "enqueued.ids"))
	if err != nil {
		fatal(err)
	}
	acked, err := createIDFile(filepath.Join(*out, "acked.ids"))
	if err != nil {
		fatal(err)
	}
	pending, err := createIDFile(filepath.Join(*out, "pending.ids"))
	if err != nil {
		fatal(err)
	}

	// Phase one: fill the queue. This has to finish before the kill window
	// opens, so that a lost enqueue response cannot be confused with a lost
	// message.
	start := time.Now()
	var (
		wg   sync.WaitGroup
		next atomic.Int64
	)
	for p := 0; p < *producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !c.dead.Load() {
				i := next.Add(1) - 1
				if i >= int64(*count) {
					return
				}
				var resp enqueueResponse
				if err := c.post("/queues/"+*name+"/messages", map[string]any{
					"body": fmt.Sprintf("msg-%06d", i),
				}, &resp); err != nil {
					fmt.Fprintf(os.Stderr, "loadgen: enqueue stopped: %v\n", err)
					return
				}
				enqueued.add(resp.ID)
			}
		}()
	}
	wg.Wait()
	rate := float64(enqueued.count()) / time.Since(start).Seconds()
	fmt.Printf("loadgen: enqueued %d messages in %v (%.0f/s)\n", enqueued.count(), time.Since(start).Round(time.Millisecond), rate)

	if c.dead.Load() {
		fmt.Fprintln(os.Stderr, "loadgen: the server died during the fill phase; the kill landed too early to prove anything")
		closeAll(enqueued, acked, pending)
		os.Exit(1)
	}

	// Tell the script the kill window is open.
	if err := os.WriteFile(filepath.Join(*out, "ready"), []byte("go\n"), 0o644); err != nil {
		fatal(err)
	}

	// Phase two: consume and ack until the server stops answering.
	var consumed sync.WaitGroup
	for w := 0; w < *consumers; w++ {
		consumed.Add(1)
		go func() {
			defer consumed.Done()
			for !c.dead.Load() {
				var deq dequeueResponse
				if err := c.post(fmt.Sprintf("/queues/%s/messages/dequeue?n=%d&wait_ms=100", *name, *batch), nil, &deq); err != nil {
					return
				}
				if len(deq.Messages) == 0 {
					return // drained; the kill will land on an idle server
				}
				for _, msg := range deq.Messages {
					// Recorded before the request goes out: if the answer never
					// comes back, this ID is genuinely ambiguous and has to be
					// treated as such rather than as un-acked.
					pending.add(msg.ID)
					err := c.post("/queues/"+*name+"/messages/"+msg.ID+"/ack",
						map[string]any{"lease_token": msg.LeaseToken}, nil)
					if err != nil {
						return
					}
					acked.add(msg.ID)
				}
			}
		}()
	}
	consumed.Wait()

	fmt.Printf("loadgen: server stopped answering after %d confirmed acks\n", acked.count())
	closeAll(enqueued, acked, pending)
}

func closeAll(files ...*idFile) {
	for _, f := range files {
		if err := f.close(); err != nil {
			fatal(err)
		}
	}
}

// --- verify phase ---

func runVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:18123", "server address")
	name := fs.String("queue", "crashtest", "queue name")
	out := fs.String("out", ".crashtest", "directory holding the record files")
	fs.Parse(args)

	enqueued, err := readIDs(filepath.Join(*out, "enqueued.ids"))
	if err != nil {
		fatal(err)
	}
	ackedConfirmed, err := readIDs(filepath.Join(*out, "acked.ids"))
	if err != nil {
		fatal(err)
	}
	ackSent, err := readIDs(filepath.Join(*out, "pending.ids"))
	if err != nil {
		fatal(err)
	}
	// An ack that came back is not ambiguous.
	for id := range ackedConfirmed {
		delete(ackSent, id)
	}

	c := newClient(*addr)
	if err := c.waitReady(60 * time.Second); err != nil {
		fatal(err)
	}

	// Drain the recovered queue to find out what actually survived. The
	// visibility timeout is ten minutes, so nothing drained here comes back
	// around during the drain itself.
	survivors := make(map[string]int) // id -> attempts
	for {
		var deq dequeueResponse
		if err := c.post(fmt.Sprintf("/queues/%s/messages/dequeue?n=500&wait_ms=200", *name), nil, &deq); err != nil {
			fatal(fmt.Errorf("draining after restart: %w", err))
		}
		if len(deq.Messages) == 0 {
			break
		}
		for _, msg := range deq.Messages {
			survivors[msg.ID] = msg.Attempts
		}
	}

	var (
		resurrected []string // acked before the crash, back after it
		lost        []string // confirmed enqueued, never acked, gone anyway
		unknown     []string // present but never confirmed to the producer
		ambiguous   int      // ack request sent, answer lost with the socket
		redelivered int      // survived a lease that the crash cut short
	)
	for id := range enqueued {
		_, present := survivors[id]
		switch {
		case ackedConfirmed[id] && present:
			resurrected = append(resurrected, id)
		case ackedConfirmed[id]:
			// Correct: a confirmed ack means the record was fsynced.
		case present:
			// Correct: un-acked messages have to survive.
		case ackSent[id]:
			ambiguous++
		default:
			lost = append(lost, id)
		}
	}
	for id := range survivors {
		if !enqueued[id] {
			unknown = append(unknown, id)
		}
		if survivors[id] >= 2 {
			redelivered++
		}
	}
	sort.Strings(lost)
	sort.Strings(resurrected)

	fmt.Println()
	fmt.Println("crash test result")
	fmt.Println("-----------------")
	fmt.Printf("  enqueued (confirmed 201)      %7d\n", len(enqueued))
	fmt.Printf("  acked    (confirmed 204)      %7d\n", len(ackedConfirmed))
	fmt.Printf("  present after restart         %7d\n", len(survivors))
	fmt.Printf("  redelivered (attempts >= 2)   %7d\n", redelivered)
	fmt.Printf("  ack in flight when killed     %7d   (may be either, not counted as loss)\n", ambiguous)
	fmt.Println()
	fmt.Printf("  LOST         (un-acked and gone)      %7d\n", len(lost))
	fmt.Printf("  RESURRECTED  (acked and back)         %7d\n", len(resurrected))
	fmt.Printf("  UNKNOWN      (present, never confirmed) %5d\n", len(unknown))
	fmt.Println()

	failed := false
	if len(lost) > 0 {
		failed = true
		fmt.Printf("FAIL: %d message(s) were confirmed to the producer and are not in the recovered log\n", len(lost))
		fmt.Printf("      first few: %v\n", firstN(lost, 5))
	}
	if len(resurrected) > 0 {
		failed = true
		fmt.Printf("FAIL: %d message(s) were acknowledged and came back after recovery\n", len(resurrected))
		fmt.Printf("      first few: %v\n", firstN(resurrected, 5))
	}
	if len(enqueued) == 0 || len(ackedConfirmed) == 0 {
		failed = true
		fmt.Println("FAIL: the run produced no work, so this proves nothing")
	}
	if failed {
		os.Exit(1)
	}
	fmt.Println("PASS: every acknowledged message stayed gone and every un-acknowledged message came back")
}

func firstN(s []string, n int) []string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
	os.Exit(1)
}
