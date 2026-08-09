package queue

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Mode selects which way the sequence comparator points.
type Mode string

const (
	FIFO Mode = "fifo"
	LIFO Mode = "lifo"
)

// Defaults applied to a Config with zero values.
const (
	DefaultVisibilityTimeout = 30 * time.Second
	DefaultMaxAttempts       = 5
	// DedupTTL is how long an enqueue's dedup key keeps collapsing repeats.
	// It is deliberately not configurable; one knob less to get wrong.
	DedupTTL = 5 * time.Minute
)

var (
	ErrQueueExists     = errors.New("queue already exists")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrNotLeased       = errors.New("message is not leased")
	ErrBadLeaseToken   = errors.New("lease token does not match the current lease")
	ErrClosed          = errors.New("queue manager is closed")
	ErrInvalidQueueCfg = errors.New("invalid queue configuration")
)

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// Config is everything that distinguishes one queue from another. The eight
// queue types the assessment asks for are the eight combinations of Mode,
// Priority and Delay over one implementation.
type Config struct {
	Name                string `json:"name"`
	Mode                Mode   `json:"mode"`
	Priority            bool   `json:"priority"`
	Delay               bool   `json:"delay"`
	VisibilityTimeoutMS int64  `json:"visibility_timeout_ms"`
	MaxAttempts         int    `json:"max_attempts"`
	DLQ                 bool   `json:"dlq"`
}

func (c *Config) normalize() error {
	if !queueNamePattern.MatchString(c.Name) {
		return fmt.Errorf("%w: name %q must be 1-64 characters of letters, digits, dash or underscore", ErrInvalidQueueCfg, c.Name)
	}
	switch c.Mode {
	case "":
		c.Mode = FIFO
	case FIFO, LIFO:
	default:
		return fmt.Errorf("%w: mode %q must be fifo or lifo", ErrInvalidQueueCfg, c.Mode)
	}
	if c.VisibilityTimeoutMS == 0 {
		c.VisibilityTimeoutMS = DefaultVisibilityTimeout.Milliseconds()
	}
	if c.VisibilityTimeoutMS < 0 {
		return fmt.Errorf("%w: visibility_timeout_ms must be positive", ErrInvalidQueueCfg)
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = DefaultMaxAttempts
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("%w: max_attempts must be at least 1", ErrInvalidQueueCfg)
	}
	return nil
}

// Message is one queued item. Priority is always present and always compared;
// when a queue has priority disabled every message simply carries zero, which
// is why there is no separate priority code path.
type Message struct {
	ID         string
	Body       []byte
	Priority   int
	Seq        uint64
	EnqueuedAt int64 // unix milliseconds
	VisibleAt  int64 // unix milliseconds
	Attempts   int
	DedupKey   string

	// Set while the message is leased to a consumer.
	leaseToken   string
	leaseExpires int64
}

func (m *Message) key() Key { return Key{Priority: m.Priority, Seq: m.Seq} }

// Delivery is what a consumer receives: the message plus the token it must
// present to ack or nack it.
type Delivery struct {
	Message    *Message
	LeaseToken string
	ExpiresAt  int64
}

// Stats is the queue's observable state.
type Stats struct {
	Name        string `json:"name"`
	Config      Config `json:"config"`
	Depth       int    `json:"depth"`
	Delayed     int    `json:"delayed"`
	InFlight    int    `json:"in_flight"`
	DLQ         int    `json:"dlq"`
	Total       int    `json:"total"`
	OldestAgeMS int64  `json:"oldest_age_ms"`

	Enqueued uint64 `json:"enqueued"`
	Dequeued uint64 `json:"dequeued"`
	Acked    uint64 `json:"acked"`
	Nacked   uint64 `json:"nacked"`
	Expired  uint64 `json:"expired"`
	DLQMoved uint64 `json:"dlq_moved"`
	Deduped  uint64 `json:"deduped"`
}

type counters struct {
	enqueued uint64
	dequeued uint64
	acked    uint64
	nacked   uint64
	expired  uint64
	dlqMoved uint64
	deduped  uint64
}

func randomID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing means the process has no usable entropy source.
		// Handing out colliding message IDs after that would corrupt state
		// silently, so stop instead.
		panic("queue: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b)
}

func unixMilli(t time.Time) int64 { return t.UnixNano() / int64(time.Millisecond) }
