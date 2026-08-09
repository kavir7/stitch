package api

import (
	"fmt"
	"net/http"
	"strings"
)

// metrics renders Prometheus text format by hand. The client library is a
// dependency and this is forty lines; queue names are validated to letters,
// digits, dash and underscore on creation, so no label escaping is needed.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder

	gauges := []struct {
		name, help string
		value      func(st queueSample) int64
	}{
		{"stitch_queue_depth", "Messages ready for delivery right now.", func(st queueSample) int64 { return int64(st.Depth) }},
		{"stitch_queue_delayed", "Messages waiting for their visibility time.", func(st queueSample) int64 { return int64(st.Delayed) }},
		{"stitch_queue_in_flight", "Messages leased to a consumer and not yet settled.", func(st queueSample) int64 { return int64(st.InFlight) }},
		{"stitch_queue_dlq", "Messages in the dead-letter queue.", func(st queueSample) int64 { return int64(st.DLQ) }},
		{"stitch_queue_total", "Live messages in any state, dead-lettered included.", func(st queueSample) int64 { return int64(st.Total) }},
		{"stitch_queue_oldest_age_ms", "Age of the oldest live message in milliseconds.", func(st queueSample) int64 { return st.OldestAgeMS }},
	}
	counters := []struct {
		name, help string
		value      func(st queueSample) uint64
	}{
		{"stitch_queue_enqueued_total", "Messages accepted and fsynced.", func(st queueSample) uint64 { return st.Enqueued }},
		{"stitch_queue_dequeued_total", "Leases granted.", func(st queueSample) uint64 { return st.Dequeued }},
		{"stitch_queue_acked_total", "Messages acknowledged and removed.", func(st queueSample) uint64 { return st.Acked }},
		{"stitch_queue_nacked_total", "Messages returned by a consumer.", func(st queueSample) uint64 { return st.Nacked }},
		{"stitch_queue_lease_expired_total", "Leases reclaimed after their visibility timeout.", func(st queueSample) uint64 { return st.Expired }},
		{"stitch_queue_dlq_moved_total", "Messages that ran out of attempts.", func(st queueSample) uint64 { return st.DLQMoved }},
		{"stitch_queue_deduped_total", "Enqueues collapsed onto an earlier message.", func(st queueSample) uint64 { return st.Deduped }},
	}

	samples := s.sampleQueues()

	for _, g := range gauges {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name)
		for _, st := range samples {
			fmt.Fprintf(&b, "%s{queue=%q} %d\n", g.name, st.Name, g.value(st))
		}
	}
	for _, c := range counters {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
		for _, st := range samples {
			fmt.Fprintf(&b, "%s{queue=%q} %d\n", c.name, st.Name, c.value(st))
		}
	}

	ws := s.mgr.WALStats()
	fmt.Fprintf(&b, "# HELP stitch_wal_records_total Records appended to the write-ahead log.\n# TYPE stitch_wal_records_total counter\nstitch_wal_records_total %d\n", ws.Appends)
	fmt.Fprintf(&b, "# HELP stitch_wal_fsyncs_total Batches committed, one fsync each.\n# TYPE stitch_wal_fsyncs_total counter\nstitch_wal_fsyncs_total %d\n", ws.Batches)
	fmt.Fprintf(&b, "# HELP stitch_wal_bytes_total Bytes written to the log.\n# TYPE stitch_wal_bytes_total counter\nstitch_wal_bytes_total %d\n", ws.Bytes)

	// Records per fsync is the number that says whether group commit is
	// earning anything on this workload.
	perSync := 0.0
	if ws.Batches > 0 {
		perSync = float64(ws.Appends) / float64(ws.Batches)
	}
	fmt.Fprintf(&b, "# HELP stitch_wal_records_per_fsync Average group commit size.\n# TYPE stitch_wal_records_per_fsync gauge\nstitch_wal_records_per_fsync %.3f\n", perSync)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprint(w, b.String())
}

// queueSample is a stats snapshot flattened for rendering.
type queueSample struct {
	Name        string
	Depth       int
	Delayed     int
	InFlight    int
	DLQ         int
	Total       int
	OldestAgeMS int64
	Enqueued    uint64
	Dequeued    uint64
	Acked       uint64
	Nacked      uint64
	Expired     uint64
	DLQMoved    uint64
	Deduped     uint64
}

func (s *Server) sampleQueues() []queueSample {
	queues := s.mgr.Queues()
	out := make([]queueSample, 0, len(queues))
	for _, q := range queues {
		st := q.Stats()
		out = append(out, queueSample{
			Name:        st.Name,
			Depth:       st.Depth,
			Delayed:     st.Delayed,
			InFlight:    st.InFlight,
			DLQ:         st.DLQ,
			Total:       st.Total,
			OldestAgeMS: st.OldestAgeMS,
			Enqueued:    st.Enqueued,
			Dequeued:    st.Dequeued,
			Acked:       st.Acked,
			Nacked:      st.Nacked,
			Expired:     st.Expired,
			DLQMoved:    st.DLQMoved,
			Deduped:     st.Deduped,
		})
	}
	return out
}
