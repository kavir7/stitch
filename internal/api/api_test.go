package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kavir7/stitch/internal/queue"
	"github.com/kavir7/stitch/internal/wal"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	mgr, _, err := queue.Open(queue.Options{
		Dir:    t.TempDir(),
		Sync:   wal.SyncGroup,
		Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Errorf("close manager: %v", err)
		}
	})
	return New(mgr, Options{Logger: log.New(io.Discard, "", 0)})
}

// do runs one request against the router and returns the recorder.
func do(t *testing.T, s *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func decodeInto(t *testing.T, rec *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
}

func createQueueOrFail(t *testing.T, s *Server, req createQueueRequest) {
	t.Helper()
	rec := do(t, s, "POST", "/queues", req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create queue: %d %s", rec.Code, rec.Body.String())
	}
}

func TestCreateListAndStatQueues(t *testing.T) {
	s := newTestServer(t)

	rec := do(t, s, "POST", "/queues", createQueueRequest{Name: "jobs", Mode: "lifo", Priority: true, MaxAttempts: 3, DLQ: true})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var cfg queue.Config
	decodeInto(t, rec, &cfg)
	if cfg.Mode != queue.LIFO || !cfg.Priority || cfg.MaxAttempts != 3 || !cfg.DLQ {
		t.Fatalf("created config came back as %+v", cfg)
	}
	if cfg.VisibilityTimeoutMS != queue.DefaultVisibilityTimeout.Milliseconds() {
		t.Fatalf("visibility timeout defaulted to %d", cfg.VisibilityTimeoutMS)
	}

	createQueueOrFail(t, s, createQueueRequest{Name: "alerts"})

	rec = do(t, s, "GET", "/queues", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d", rec.Code)
	}
	var list []queue.Stats
	decodeInto(t, rec, &list)
	if len(list) != 2 || list[0].Name != "alerts" || list[1].Name != "jobs" {
		t.Fatalf("listing is wrong or unsorted: %+v", list)
	}

	rec = do(t, s, "GET", "/queues/jobs/stats", nil)
	var st queue.Stats
	decodeInto(t, rec, &st)
	if st.Name != "jobs" || st.Depth != 0 || st.Total != 0 {
		t.Fatalf("stats for an empty queue: %+v", st)
	}
}

func TestMessageLifecycleOverHTTP(t *testing.T) {
	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "jobs", VisibilityTimeoutMS: 60000, MaxAttempts: 5})

	rec := do(t, s, "POST", "/queues/jobs/messages", enqueueRequest{Body: "do the thing"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("enqueue: %d %s", rec.Code, rec.Body.String())
	}
	var enq enqueueResponse
	decodeInto(t, rec, &enq)
	if enq.ID == "" || enq.Seq != 1 || enq.Deduped {
		t.Fatalf("enqueue response: %+v", enq)
	}

	rec = do(t, s, "POST", "/queues/jobs/messages/dequeue?n=10", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("dequeue: %d %s", rec.Code, rec.Body.String())
	}
	var deq dequeueResponse
	decodeInto(t, rec, &deq)
	if len(deq.Messages) != 1 {
		t.Fatalf("dequeued %d messages", len(deq.Messages))
	}
	msg := deq.Messages[0]
	if msg.ID != enq.ID || msg.Body != "do the thing" || msg.Attempts != 1 || msg.LeaseToken == "" {
		t.Fatalf("dequeued message: %+v", msg)
	}
	if msg.LeaseExpiresAt <= msg.EnqueuedAt {
		t.Fatalf("lease expires at %d, which is not after the enqueue at %d", msg.LeaseExpiresAt, msg.EnqueuedAt)
	}

	rec = do(t, s, "POST", "/queues/jobs/messages/"+msg.ID+"/ack", ackRequest{LeaseToken: msg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("ack: %d %s", rec.Code, rec.Body.String())
	}

	rec = do(t, s, "GET", "/queues/jobs/stats", nil)
	var st queue.Stats
	decodeInto(t, rec, &st)
	if st.Total != 0 || st.Acked != 1 {
		t.Fatalf("after ack: %+v", st)
	}
}

func TestNackReturnsMessageAndEventuallyDeadLetters(t *testing.T) {
	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "jobs", MaxAttempts: 2, DLQ: true})

	rec := do(t, s, "POST", "/queues/jobs/messages", enqueueRequest{Body: "poison"})
	var enq enqueueResponse
	decodeInto(t, rec, &enq)

	for attempt := 1; attempt <= 2; attempt++ {
		rec = do(t, s, "POST", "/queues/jobs/messages/dequeue", nil)
		var deq dequeueResponse
		decodeInto(t, rec, &deq)
		if len(deq.Messages) != 1 {
			t.Fatalf("attempt %d: dequeued %d messages", attempt, len(deq.Messages))
		}
		if deq.Messages[0].Attempts != attempt {
			t.Fatalf("attempt %d reported attempts=%d", attempt, deq.Messages[0].Attempts)
		}
		rec = do(t, s, "POST", "/queues/jobs/messages/"+enq.ID+"/nack", nackRequest{LeaseToken: deq.Messages[0].LeaseToken})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("nack: %d %s", rec.Code, rec.Body.String())
		}
	}

	rec = do(t, s, "GET", "/queues/jobs/stats", nil)
	var st queue.Stats
	decodeInto(t, rec, &st)
	if st.DLQ != 1 || st.Depth != 0 {
		t.Fatalf("after exhausting attempts: %+v", st)
	}
}

func TestDedupOverHTTP(t *testing.T) {
	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "jobs"})

	var first, second enqueueResponse
	decodeInto(t, do(t, s, "POST", "/queues/jobs/messages", enqueueRequest{Body: "a", DedupKey: "k"}), &first)
	decodeInto(t, do(t, s, "POST", "/queues/jobs/messages", enqueueRequest{Body: "b", DedupKey: "k"}), &second)

	if !second.Deduped || second.ID != first.ID {
		t.Fatalf("second enqueue: %+v, want a dedup onto %s", second, first.ID)
	}
}

func TestDelayAndPriorityOverHTTP(t *testing.T) {
	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "q", Priority: true, Delay: true})

	do(t, s, "POST", "/queues/q/messages", enqueueRequest{Body: "low"})
	do(t, s, "POST", "/queues/q/messages", enqueueRequest{Body: "high", Priority: 9})
	do(t, s, "POST", "/queues/q/messages", enqueueRequest{Body: "delayed", Priority: 100, DelayMS: 60000})

	var deq dequeueResponse
	decodeInto(t, do(t, s, "POST", "/queues/q/messages/dequeue?n=10", nil), &deq)
	if len(deq.Messages) != 2 {
		t.Fatalf("dequeued %d messages, want the two visible ones", len(deq.Messages))
	}
	if deq.Messages[0].Body != "high" || deq.Messages[1].Body != "low" {
		t.Fatalf("priority order over HTTP: %s then %s", deq.Messages[0].Body, deq.Messages[1].Body)
	}

	var st queue.Stats
	decodeInto(t, do(t, s, "GET", "/queues/q/stats", nil), &st)
	if st.Delayed != 1 {
		t.Fatalf("the delayed message is not being held: %+v", st)
	}
}

func TestDequeueWaitParameter(t *testing.T) {
	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "q"})

	start := time.Now()
	rec := do(t, s, "POST", "/queues/q/messages/dequeue?n=5&wait_ms=80", nil)
	elapsed := time.Since(start)
	if rec.Code != http.StatusOK {
		t.Fatalf("waiting dequeue: %d", rec.Code)
	}
	var deq dequeueResponse
	decodeInto(t, rec, &deq)
	if len(deq.Messages) != 0 {
		t.Fatalf("empty queue returned %d messages", len(deq.Messages))
	}
	if elapsed < 60*time.Millisecond {
		t.Fatalf("wait_ms=80 returned after %v", elapsed)
	}
	if rec.Body.String() == "" || !strings.Contains(rec.Body.String(), `"messages":[]`) {
		t.Fatalf("empty result should be an empty array, got %q", rec.Body.String())
	}
}

func TestErrorsMapToStatusCodes(t *testing.T) {
	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "jobs", MaxAttempts: 5})

	var enq enqueueResponse
	decodeInto(t, do(t, s, "POST", "/queues/jobs/messages", enqueueRequest{Body: "x"}), &enq)
	var deq dequeueResponse
	decodeInto(t, do(t, s, "POST", "/queues/jobs/messages/dequeue", nil), &deq)
	token := deq.Messages[0].LeaseToken

	cases := []struct {
		name   string
		method string
		path   string
		body   any
		want   int
	}{
		{"duplicate queue", "POST", "/queues", createQueueRequest{Name: "jobs"}, http.StatusConflict},
		{"invalid queue name", "POST", "/queues", createQueueRequest{Name: "not a name"}, http.StatusBadRequest},
		{"invalid mode", "POST", "/queues", createQueueRequest{Name: "ok", Mode: "sideways"}, http.StatusBadRequest},
		{"unknown queue stats", "GET", "/queues/nope/stats", nil, http.StatusNotFound},
		{"enqueue to unknown queue", "POST", "/queues/nope/messages", enqueueRequest{Body: "x"}, http.StatusNotFound},
		{"dequeue from unknown queue", "POST", "/queues/nope/messages/dequeue", nil, http.StatusNotFound},
		{"ack unknown message", "POST", "/queues/jobs/messages/nosuch/ack", ackRequest{LeaseToken: token}, http.StatusNotFound},
		{"ack with wrong token", "POST", "/queues/jobs/messages/" + enq.ID + "/ack", ackRequest{LeaseToken: "wrong"}, http.StatusConflict},
		{"nack with wrong token", "POST", "/queues/jobs/messages/" + enq.ID + "/nack", nackRequest{LeaseToken: "wrong"}, http.StatusConflict},
		{"bad n", "POST", "/queues/jobs/messages/dequeue?n=abc", nil, http.StatusBadRequest},
		{"negative n", "POST", "/queues/jobs/messages/dequeue?n=-4", nil, http.StatusBadRequest},
		{"crash endpoint is not routed", "POST", "/debug/crash", nil, http.StatusNotFound},
		{"wrong method", "GET", "/queues/jobs/messages/dequeue", nil, http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, s, tc.method, tc.path, tc.body)
			if rec.Code != tc.want {
				t.Fatalf("%s %s returned %d, want %d (%s)", tc.method, tc.path, rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestMalformedBodiesAreRejected(t *testing.T) {
	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "jobs"})

	for _, body := range []string{"", "{", `{"body": 7}`, `{"body":"x","nonsense":true}`} {
		req := httptest.NewRequest("POST", "/queues/jobs/messages", strings.NewReader(body))
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %q returned %d, want 400", body, rec.Code)
		}
	}
}

func TestHealthz(t *testing.T) {
	s := newTestServer(t)
	rec := do(t, s, "GET", "/healthz", nil)
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Body.String(), "ok") {
		t.Fatalf("healthz returned %d %q", rec.Code, rec.Body.String())
	}
}

func TestMetricsExposition(t *testing.T) {
	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "jobs"})
	do(t, s, "POST", "/queues/jobs/messages", enqueueRequest{Body: "one"})
	do(t, s, "POST", "/queues/jobs/messages", enqueueRequest{Body: "two"})
	var deq dequeueResponse
	decodeInto(t, do(t, s, "POST", "/queues/jobs/messages/dequeue", nil), &deq)

	rec := do(t, s, "GET", "/metrics", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics: %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("metrics content type is %q", ct)
	}
	body := rec.Body.String()

	for _, want := range []string{
		"# TYPE stitch_queue_depth gauge",
		`stitch_queue_depth{queue="jobs"} 1`,
		`stitch_queue_in_flight{queue="jobs"} 1`,
		"# TYPE stitch_queue_enqueued_total counter",
		`stitch_queue_enqueued_total{queue="jobs"} 2`,
		`stitch_queue_dequeued_total{queue="jobs"} 1`,
		"stitch_wal_records_total ",
		"stitch_wal_fsyncs_total ",
		"stitch_wal_records_per_fsync ",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output is missing %q:\n%s", want, body)
		}
	}

	// Every non-comment line must be "name value" or "name{labels} value".
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if fields := strings.Fields(line); len(fields) != 2 {
			t.Fatalf("malformed metric line %q", line)
		}
	}
}

func TestCrashEndpointRoutedOnlyWithUnsafeDemo(t *testing.T) {
	mgr, _, err := queue.Open(queue.Options{Dir: t.TempDir(), Sync: wal.SyncGroup, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	off := New(mgr, Options{Logger: log.New(io.Discard, "", 0)})
	if rec := do(t, off, "POST", "/debug/crash", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("crash endpoint answered %d without --unsafe-demo", rec.Code)
	}

	// With the flag on the route exists. It is not called here for the obvious
	// reason; scripts/crash_test.sh is where it gets used.
	on := New(mgr, Options{UnsafeDemo: true, Logger: log.New(io.Discard, "", 0)})
	if _, pattern := on.mux.Handler(httptest.NewRequest("POST", "/debug/crash", nil)); pattern != "POST /debug/crash" {
		t.Fatalf("with --unsafe-demo the crash route resolved to %q", pattern)
	}
}

// Producers and consumers hammering the same queue over real HTTP: every
// message must be delivered to exactly one consumer and acked exactly once.
func TestConcurrentProducersAndConsumersOverHTTP(t *testing.T) {
	const (
		producers   = 6
		perProducer = 120
		consumers   = 6
		total       = producers * perProducer
	)

	s := newTestServer(t)
	createQueueOrFail(t, s, createQueueRequest{Name: "load", VisibilityTimeoutMS: 60000, MaxAttempts: 5})

	srv := httptest.NewServer(s)
	defer srv.Close()
	client := &http.Client{Timeout: 30 * time.Second}

	post := func(t *testing.T, path string, body any) *http.Response {
		t.Helper()
		var r io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			r = bytes.NewReader(b)
		}
		resp, err := client.Post(srv.URL+path, "application/json", r)
		if err != nil {
			t.Errorf("POST %s: %v", path, err)
			return nil
		}
		return resp
	}

	var produced sync.WaitGroup
	for p := 0; p < producers; p++ {
		produced.Add(1)
		go func(p int) {
			defer produced.Done()
			for i := 0; i < perProducer; i++ {
				resp := post(t, "/queues/load/messages", enqueueRequest{Body: fmt.Sprintf("p%d-%d", p, i)})
				if resp == nil {
					return
				}
				if resp.StatusCode != http.StatusCreated {
					t.Errorf("enqueue returned %d", resp.StatusCode)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}(p)
	}

	var (
		mu      sync.Mutex
		acked   = make(map[string]int)
		drained sync.WaitGroup
		done    = make(chan struct{})
	)
	for c := 0; c < consumers; c++ {
		drained.Add(1)
		go func() {
			defer drained.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				resp := post(t, "/queues/load/messages/dequeue?n=8&wait_ms=50", nil)
				if resp == nil {
					return
				}
				var deq dequeueResponse
				if err := json.NewDecoder(resp.Body).Decode(&deq); err != nil {
					t.Errorf("decode dequeue: %v", err)
					resp.Body.Close()
					return
				}
				resp.Body.Close()
				for _, msg := range deq.Messages {
					ack := post(t, "/queues/load/messages/"+msg.ID+"/ack", ackRequest{LeaseToken: msg.LeaseToken})
					if ack == nil {
						return
					}
					if ack.StatusCode != http.StatusNoContent {
						t.Errorf("ack returned %d", ack.StatusCode)
					}
					io.Copy(io.Discard, ack.Body)
					ack.Body.Close()
					mu.Lock()
					acked[msg.Body]++
					mu.Unlock()
				}
			}
		}()
	}

	produced.Wait()
	deadline := time.After(60 * time.Second)
	for {
		mu.Lock()
		n := len(acked)
		mu.Unlock()
		if n == total {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of %d messages were acked before the deadline", n, total)
		case <-time.After(20 * time.Millisecond):
		}
	}
	close(done)
	drained.Wait()

	mu.Lock()
	defer mu.Unlock()
	for body, n := range acked {
		if n != 1 {
			t.Fatalf("message %q was acked %d times", body, n)
		}
	}

	var st queue.Stats
	decodeInto(t, do(t, s, "GET", "/queues/load/stats", nil), &st)
	if st.Total != 0 || st.InFlight != 0 {
		t.Fatalf("queue is not empty after every message was acked: %+v", st)
	}
	if st.Acked != total {
		t.Fatalf("server counted %d acks, client counted %d", st.Acked, total)
	}
}
