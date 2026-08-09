// Package api exposes the queue over HTTP. The handlers are thin: they parse,
// call one queue method, and translate the error. All of the interesting
// behaviour lives a layer down, which is deliberate.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/kavir7/stitch/internal/queue"
	"github.com/kavir7/stitch/web"
)

const (
	maxBodyBytes   = 1 << 20
	maxDequeueN    = 1000
	maxDequeueWait = 60 * time.Second
)

// Options configures the server.
type Options struct {
	// UnsafeDemo enables POST /debug/crash, which SIGKILLs the process. It is
	// off unless the operator asks for it on the command line.
	UnsafeDemo bool
	Logger     *log.Logger
}

// Server routes HTTP requests to a queue manager.
type Server struct {
	mgr     *queue.Manager
	mux     *http.ServeMux
	logger  *log.Logger
	started time.Time
}

// New builds the router. Go 1.22's method-and-wildcard patterns mean there is
// no hand-rolled path parsing anywhere in this package.
func New(mgr *queue.Manager, opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	s := &Server{
		mgr:     mgr,
		mux:     http.NewServeMux(),
		logger:  opts.Logger,
		started: time.Now(),
	}

	s.mux.HandleFunc("POST /queues", s.createQueue)
	s.mux.HandleFunc("GET /queues", s.listQueues)
	s.mux.HandleFunc("GET /queues/{q}/stats", s.queueStats)
	s.mux.HandleFunc("POST /queues/{q}/messages", s.enqueue)
	s.mux.HandleFunc("POST /queues/{q}/messages/dequeue", s.dequeue)
	s.mux.HandleFunc("POST /queues/{q}/messages/{id}/ack", s.ack)
	s.mux.HandleFunc("POST /queues/{q}/messages/{id}/nack", s.nack)
	s.mux.HandleFunc("GET /{$}", s.dashboard)
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /metrics", s.metrics)
	if opts.UnsafeDemo {
		s.mux.HandleFunc("POST /debug/crash", s.crash)
	}
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// --- request and response shapes ---

type createQueueRequest struct {
	Name                string `json:"name"`
	Mode                string `json:"mode"`
	Priority            bool   `json:"priority"`
	Delay               bool   `json:"delay"`
	VisibilityTimeoutMS int64  `json:"visibility_timeout_ms"`
	MaxAttempts         int    `json:"max_attempts"`
	DLQ                 bool   `json:"dlq"`
}

type enqueueRequest struct {
	Body     string `json:"body"`
	Priority int    `json:"priority"`
	DelayMS  int64  `json:"delay_ms"`
	DedupKey string `json:"dedup_key"`
}

type enqueueResponse struct {
	ID        string `json:"id"`
	Seq       uint64 `json:"seq"`
	VisibleAt int64  `json:"visible_at"`
	Deduped   bool   `json:"deduped"`
}

type messageJSON struct {
	ID             string `json:"id"`
	Body           string `json:"body"`
	Priority       int    `json:"priority"`
	Seq            uint64 `json:"seq"`
	Attempts       int    `json:"attempts"`
	EnqueuedAt     int64  `json:"enqueued_at"`
	LeaseToken     string `json:"lease_token"`
	LeaseExpiresAt int64  `json:"lease_expires_at"`
}

type dequeueResponse struct {
	Messages []messageJSON `json:"messages"`
}

type ackRequest struct {
	LeaseToken string `json:"lease_token"`
}

type nackRequest struct {
	LeaseToken     string `json:"lease_token"`
	RequeueDelayMS int64  `json:"requeue_delay_ms"`
}

// --- handlers ---

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	var req createQueueRequest
	if !decode(w, r, &req) {
		return
	}
	q, err := s.mgr.CreateQueue(queue.Config{
		Name:                req.Name,
		Mode:                queue.Mode(req.Mode),
		Priority:            req.Priority,
		Delay:               req.Delay,
		VisibilityTimeoutMS: req.VisibilityTimeoutMS,
		MaxAttempts:         req.MaxAttempts,
		DLQ:                 req.DLQ,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, q.Config())
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	queues := s.mgr.Queues()
	out := make([]queue.Stats, 0, len(queues))
	for _, q := range queues {
		out = append(out, q.Stats())
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) queueStats(w http.ResponseWriter, r *http.Request) {
	q, err := s.mgr.Queue(r.PathValue("q"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, q.Stats())
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	q, err := s.mgr.Queue(r.PathValue("q"))
	if err != nil {
		s.fail(w, err)
		return
	}
	var req enqueueRequest
	if !decode(w, r, &req) {
		return
	}
	// This call returns once the fsync covering the record has completed, so
	// the 201 below is a durability claim and not a hopeful one.
	msg, deduped, err := q.Enqueue([]byte(req.Body), req.Priority, time.Duration(req.DelayMS)*time.Millisecond, req.DedupKey)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, enqueueResponse{
		ID:        msg.ID,
		Seq:       msg.Seq,
		VisibleAt: msg.VisibleAt,
		Deduped:   deduped,
	})
}

func (s *Server) dequeue(w http.ResponseWriter, r *http.Request) {
	q, err := s.mgr.Queue(r.PathValue("q"))
	if err != nil {
		s.fail(w, err)
		return
	}
	n, err := intParam(r, "n", 1, 1, maxDequeueN)
	if err != nil {
		s.badRequest(w, err)
		return
	}
	waitMS, err := intParam(r, "wait_ms", 0, 0, int(maxDequeueWait.Milliseconds()))
	if err != nil {
		s.badRequest(w, err)
		return
	}

	deliveries, err := q.Dequeue(r.Context(), n, time.Duration(waitMS)*time.Millisecond)
	if err != nil {
		if errors.Is(err, r.Context().Err()) {
			// The client hung up mid-wait; there is nobody to answer.
			return
		}
		s.fail(w, err)
		return
	}
	out := dequeueResponse{Messages: make([]messageJSON, 0, len(deliveries))}
	for _, d := range deliveries {
		out.Messages = append(out.Messages, messageJSON{
			ID:             d.Message.ID,
			Body:           string(d.Message.Body),
			Priority:       d.Message.Priority,
			Seq:            d.Message.Seq,
			Attempts:       d.Message.Attempts,
			EnqueuedAt:     d.Message.EnqueuedAt,
			LeaseToken:     d.LeaseToken,
			LeaseExpiresAt: d.ExpiresAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	q, err := s.mgr.Queue(r.PathValue("q"))
	if err != nil {
		s.fail(w, err)
		return
	}
	var req ackRequest
	if !decode(w, r, &req) {
		return
	}
	if err := q.Ack(r.PathValue("id"), req.LeaseToken); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) nack(w http.ResponseWriter, r *http.Request) {
	q, err := s.mgr.Queue(r.PathValue("q"))
	if err != nil {
		s.fail(w, err)
		return
	}
	var req nackRequest
	if !decode(w, r, &req) {
		return
	}
	if err := q.Nack(r.PathValue("id"), req.LeaseToken, time.Duration(req.RequeueDelayMS)*time.Millisecond); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// dashboard serves the demo page, which is compiled into the binary.
func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(web.Index)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "ok\nuptime_seconds %d\n", int(time.Since(s.started).Seconds()))
}

// --- helpers ---

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body: " + err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already out; all that is left is to stop writing.
		return
	}
}

func intParam(r *http.Request, name string, def, min, max int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	if v < min {
		return 0, fmt.Errorf("%s must be at least %d", name, min)
	}
	if v > max {
		v = max
	}
	return v, nil
}

func (s *Server) badRequest(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
}

// fail maps a queue error onto a status code. Anything unrecognised is a 500,
// because the only unrecognised errors this layer can see come from the log.
func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrQueueNotFound), errors.Is(err, queue.ErrNotLeased):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, queue.ErrQueueExists):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, queue.ErrBadLeaseToken):
		// The lease moved on: expired and redelivered, or already settled.
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, queue.ErrInvalidQueueCfg):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, queue.ErrClosed):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
	default:
		s.logger.Printf("api: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}
