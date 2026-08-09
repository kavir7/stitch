package queue

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// allConfigs is the eight queue types the assessment asks for: two sequence
// directions times priority on/off times delay on/off. They are eight rows of
// config over one implementation, which is the point of the exercise.
func allConfigs() []Config {
	var out []Config
	for _, mode := range []Mode{FIFO, LIFO} {
		for _, priority := range []bool{false, true} {
			for _, delay := range []bool{false, true} {
				out = append(out, Config{
					Name:                fmt.Sprintf("%s-p%v-d%v", mode, priority, delay),
					Mode:                mode,
					Priority:            priority,
					Delay:               delay,
					VisibilityTimeoutMS: 3600000, // leases do not lapse on their own here
					MaxAttempts:         1000,    // nothing reaches the dead-letter queue
				})
			}
		}
	}
	return out
}

// modelMsg mirrors what the queue should be holding. The model picks the next
// delivery with a plain linear scan; the queue uses three heaps, a lease map
// and a promotion step. The scan is the independent second opinion.
type modelMsg struct {
	id        string
	priority  int
	seq       uint64
	visibleAt int64
	inflight  bool
	token     string
}

type model struct {
	cfg  Config
	msgs []*modelMsg
	seq  uint64
}

func (mo *model) enqueue(id string, priority int, delayMS int64, now int64) {
	mo.seq++
	m := &modelMsg{id: id, seq: mo.seq, visibleAt: now}
	if mo.cfg.Priority {
		m.priority = priority
	}
	if mo.cfg.Delay && delayMS > 0 {
		m.visibleAt = now + delayMS
	}
	mo.msgs = append(mo.msgs, m)
}

// best is the message the queue must deliver next, or nil if nothing is
// visible. Deliberately written as an exhaustive scan rather than by calling
// less(), so a bug in the comparator's use does not cancel itself out.
func (mo *model) best(now int64) *modelMsg {
	var best *modelMsg
	for _, m := range mo.msgs {
		if m.inflight || m.visibleAt > now {
			continue
		}
		if best == nil {
			best = m
			continue
		}
		if m.priority != best.priority {
			if m.priority > best.priority {
				best = m
			}
			continue
		}
		if mo.cfg.Mode == LIFO {
			if m.seq > best.seq {
				best = m
			}
		} else if m.seq < best.seq {
			best = m
		}
	}
	return best
}

func (mo *model) find(id string) *modelMsg {
	for _, m := range mo.msgs {
		if m.id == id {
			return m
		}
	}
	return nil
}

func (mo *model) remove(id string) {
	for i, m := range mo.msgs {
		if m.id == id {
			mo.msgs = append(mo.msgs[:i], mo.msgs[i+1:]...)
			return
		}
	}
}

func (mo *model) inflightIDs() []string {
	var out []string
	for _, m := range mo.msgs {
		if m.inflight {
			out = append(out, m.id)
		}
	}
	return out
}

// TestOrderingInvariantAcrossAllConfigs drives a few hundred random
// operations against each of the eight queue types and asserts after every
// single dequeue that the message handed out is the one the model says is
// next. It also checks at the end that no message was lost or duplicated.
func TestOrderingInvariantAcrossAllConfigs(t *testing.T) {
	for _, cfg := range allConfigs() {
		cfg := cfg
		t.Run(cfg.Name, func(t *testing.T) {
			t.Parallel()
			runOrderingProperty(t, cfg, 1)
		})
	}
}

func runOrderingProperty(t *testing.T, cfg Config, seed int64) {
	t.Helper()

	const ops = 220

	rng := rand.New(rand.NewSource(seed))
	clock := newTestClock()
	m := newTestManager(t, t.TempDir(), clock)
	q := mustCreate(t, m, cfg)
	mo := &model{cfg: cfg}

	enqueued := make(map[string]bool)
	delivered := make(map[string]int)
	tokens := make(map[string]string)

	for step := 0; step < ops; step++ {
		now := clock.Now().UnixMilli()

		switch pickOp(rng, len(mo.inflightIDs()) > 0) {
		case opEnqueue:
			priority := rng.Intn(4)
			var delayMS int64
			if rng.Intn(3) == 0 {
				delayMS = int64(1 + rng.Intn(300))
			}
			msg, deduped, err := q.Enqueue([]byte(fmt.Sprintf("step-%d", step)), priority, time.Duration(delayMS)*time.Millisecond, "")
			if err != nil || deduped {
				t.Fatalf("step %d: enqueue returned deduped=%v err=%v", step, deduped, err)
			}
			mo.enqueue(msg.ID, priority, delayMS, now)
			enqueued[msg.ID] = true

		case opDequeue:
			want := mo.best(now)
			got, err := q.Dequeue(context.Background(), 1, 0)
			if err != nil {
				t.Fatalf("step %d: dequeue: %v", step, err)
			}
			if want == nil {
				if len(got) != 0 {
					t.Fatalf("step %d: queue delivered %q with nothing visible at %d", step, got[0].Message.ID, now)
				}
				break
			}
			if len(got) == 0 {
				t.Fatalf("step %d: queue delivered nothing; %q was visible (prio=%d seq=%d visible_at=%d now=%d)",
					step, want.id, want.priority, want.seq, want.visibleAt, now)
			}
			if got[0].Message.ID != want.id {
				gotM := mo.find(got[0].Message.ID)
				t.Fatalf("step %d: %s delivered prio=%d seq=%d, but prio=%d seq=%d should have come first",
					step, cfg.Name, gotM.priority, gotM.seq, want.priority, want.seq)
			}
			want.inflight = true
			want.token = got[0].LeaseToken
			tokens[want.id] = got[0].LeaseToken
			delivered[want.id]++

		case opAck:
			ids := mo.inflightIDs()
			id := ids[rng.Intn(len(ids))]
			if err := q.Ack(id, tokens[id]); err != nil {
				t.Fatalf("step %d: ack %s: %v", step, id, err)
			}
			mo.remove(id)

		case opNack:
			ids := mo.inflightIDs()
			id := ids[rng.Intn(len(ids))]
			var requeueMS int64
			if rng.Intn(2) == 0 {
				requeueMS = int64(1 + rng.Intn(200))
			}
			if err := q.Nack(id, tokens[id], time.Duration(requeueMS)*time.Millisecond); err != nil {
				t.Fatalf("step %d: nack %s: %v", step, id, err)
			}
			mm := mo.find(id)
			mm.inflight = false
			// A nack's requeue delay is lease backoff, not the queue's delay
			// feature, so it applies whether or not delays are enabled.
			if requeueMS > 0 {
				mm.visibleAt = now + requeueMS
			}

		case opAdvance:
			clock.Advance(time.Duration(rng.Intn(150)) * time.Millisecond)
		}
	}

	// Nothing may be lost. Ack what is still leased, then jump far enough
	// forward that every delayed message is due, and account for every
	// message that was ever enqueued.
	for _, id := range mo.inflightIDs() {
		if err := q.Ack(id, tokens[id]); err != nil {
			t.Fatalf("final ack %s: %v", id, err)
		}
		mo.remove(id)
	}
	clock.Advance(time.Hour)
	for {
		got, err := q.Dequeue(context.Background(), 32, 0)
		if err != nil {
			t.Fatalf("final drain: %v", err)
		}
		if len(got) == 0 {
			break
		}
		for _, d := range got {
			delivered[d.Message.ID]++
			if err := q.Ack(d.Message.ID, d.LeaseToken); err != nil {
				t.Fatalf("final drain ack: %v", err)
			}
			mo.remove(d.Message.ID)
		}
	}

	if len(mo.msgs) != 0 {
		t.Fatalf("%d messages left in the model after draining the queue", len(mo.msgs))
	}
	for id := range enqueued {
		if delivered[id] == 0 {
			t.Fatalf("message %s was enqueued and never delivered", id)
		}
	}
	if s := q.Stats(); s.Total != 0 {
		t.Fatalf("queue reports %d messages left after draining: %+v", s.Total, s)
	}
	t.Logf("%s: %d messages enqueued, %d deliveries", cfg.Name, len(enqueued), len(delivered))
}

type op int

const (
	opEnqueue op = iota
	opDequeue
	opAck
	opNack
	opAdvance
)

func pickOp(rng *rand.Rand, hasInflight bool) op {
	switch rng.Intn(10) {
	case 0, 1, 2, 3:
		return opEnqueue
	case 4, 5, 6:
		return opDequeue
	case 7:
		if hasInflight {
			return opAck
		}
		return opEnqueue
	case 8:
		if hasInflight {
			return opNack
		}
		return opDequeue
	default:
		return opAdvance
	}
}

// The same random walk must hold under many different seeds, not just the one
// that happened to be checked in.
func TestOrderingInvariantAcrossSeeds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the multi-seed walk in short mode")
	}
	cfgs := allConfigs()
	for seed := int64(2); seed <= 5; seed++ {
		cfg := cfgs[int(seed)%len(cfgs)]
		cfg.Name = fmt.Sprintf("%s-seed%d", cfg.Name, seed)
		seed := seed
		t.Run(cfg.Name, func(t *testing.T) {
			t.Parallel()
			runOrderingProperty(t, cfg, seed)
		})
	}
}
