package queue

import (
	"encoding/json"

	"github.com/kavir7/stitch/internal/wal"
)

// The payloads below are the on-disk vocabulary. Two rules govern them.
//
// First, every value a replay needs must be *in* the record. Nothing is
// recomputed from the clock at replay time, so a requeue after a lease expiry
// carries the visible_at it was given, not the backoff that produced it. That
// is what makes the fold deterministic.
//
// Second, the operations that can touch many messages at once carry a list, so
// leasing a batch of ten costs one record and one fsync rather than ten.
//
// They are JSON. A packed binary encoding would be maybe twice as fast to
// marshal, and at three milliseconds per fsync that is not the number worth
// optimising. Being able to read a corrupt log with od and jq is worth more.

type queueCreatePayload struct {
	Config Config `json:"config"`
}

type enqueuePayload struct {
	Queue      string `json:"q"`
	ID         string `json:"id"`
	Body       []byte `json:"body"`
	Priority   int    `json:"prio,omitempty"`
	Seq        uint64 `json:"seq"`
	EnqueuedAt int64  `json:"enq_at"`
	VisibleAt  int64  `json:"vis_at"`
	DedupKey   string `json:"dedup,omitempty"`
}

type leaseItem struct {
	ID       string `json:"id"`
	Token    string `json:"token"`
	Attempts int    `json:"attempts"`
}

type leasePayload struct {
	Queue     string      `json:"q"`
	ExpiresAt int64       `json:"exp"`
	Items     []leaseItem `json:"items"`
}

type ackPayload struct {
	Queue string `json:"q"`
	ID    string `json:"id"`
}

type nackPayload struct {
	Queue     string `json:"q"`
	ID        string `json:"id"`
	VisibleAt int64  `json:"vis_at"`
}

type expireItem struct {
	ID        string `json:"id"`
	VisibleAt int64  `json:"vis_at"`
}

type expirePayload struct {
	Queue string       `json:"q"`
	Items []expireItem `json:"items"`
}

type dlqPayload struct {
	Queue  string   `json:"q"`
	IDs    []string `json:"ids"`
	Reason string   `json:"reason"`
}

// appendJSON marshals a payload and blocks until the fsync covering it has
// returned. Every caller of this function is on the request path, which is why
// nothing is acknowledged to a client before it is on disk.
func appendJSON(l *wal.Log, typ wal.RecordType, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return l.Append(typ, b)
}
