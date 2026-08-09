# Design notes

This is the reasoning behind stitch, and the four written answers the brief
asks for. The README covers how to run it.

## The shape of the thing

There are three layers and no more.

`internal/wal` is the storage engine. It frames records, checksums them,
rotates segments, batches fsyncs and replays the log on boot. It knows nothing
about queues; it stores opaque payloads with a type byte and hands them back in
write order.

`internal/queue` is the state machine. It holds the ordering structures, the
leases and the dead-letter lists, and it turns every operation into a record
before applying it. Recovery is the same state machine fed from the log instead
of from HTTP.

`internal/api` is a translation layer. Parse, call one queue method, map the
error to a status code. There is nothing in it worth reading twice, which is
the intent.

## Why the log is the database

I was told not to use an embedded key-value store, and I would have made the
same choice anyway for this workload. A queue's state is small enough to hold
in memory and its history is a sequence of small mutations, which is exactly
the shape a write-ahead log is good at. Adding a B-tree underneath would mean
maintaining two representations of the same facts and reconciling them after a
crash.

State is a deterministic fold over the record stream. That single property is
what makes recovery arguable rather than hopeful: if the fold is deterministic
and the log is intact up to the last good record, the recovered state is the
state that existed when that record was written. There is nothing else to check.

Determinism took real care. The rule I ended up with is that no replay may read
a clock. When a lease expires and the message is requeued, the record carries
the `visible_at` the live path computed, not the backoff parameters that
produced it. When a message is dead-lettered, the record names the message
rather than saying "the ones that ran out of attempts". Two replays of one log
produce identical state, and there is a test that asserts exactly that by
hashing the recovered state twice.

## Ordering

One comparator, in `internal/queue/order.go`:

```go
func less(a, b Key, lifo bool) bool {
	if a.Priority != b.Priority { return a.Priority > b.Priority }
	if lifo { return a.Seq > b.Seq }
	return a.Seq < b.Seq
}
```

Priority disabled means every message carries priority zero and the first
comparison never fires. Delay disabled means `visible_at` is always the enqueue
time. LIFO inverts the sequence half. Eight queue types, one function, and the
property tests walk all eight of them.

Three heaps sit on it. Ready messages are ordered by the comparator. Delayed
messages are ordered by `visible_at`, with the comparator breaking ties so that
two messages becoming visible in the same millisecond are promoted in the order
they will be delivered. Live leases are ordered by expiry, so the maintenance
timer only ever has to look at one of them.

Cancelled leases are not removed from the lease heap. An ack deletes the
message from the in-flight map and leaves the heap entry behind; when the entry
surfaces, its token no longer matches anything and it is skipped. That keeps
ack off any O(n) path, at the cost of a heap that can hold entries for messages
that are long gone. It drains itself as those entries reach the front.

## Concurrency and lock ordering

Each queue has one mutex. Different queues never contend, and the manager's map
is behind a separate read-write lock that is only taken for lookups and
creation.

The queue mutex is never held across an fsync. Every operation follows the same
three steps: take the lock and reserve, release the lock and append, take the
lock again and commit. Reserving means removing the message from anywhere it
could be picked up again, so a dequeue pops from the ready heap and holds the
message in a local slice, and an ack removes it from the in-flight map. If the
append fails, the reservation is undone. Nothing else can observe the message
in between, and the fsync does not block the other twenty producers.

Enqueue is the one operation that applies strictly after its fsync rather than
reserving first. That is deliberate. If a message became visible before its
ENQUEUE record was durable, a consumer could lease it and the LEASE record
could reach disk describing a message that does not exist in the log. Recovery
refuses records for unknown messages, so that would turn a crash into a failure
to start.

Queue creation holds the manager lock across its append for the same reason,
one level up.

The WAL's writer goroutine owns the file handle outright. Appends are sent to
it over a channel, so there is no mutex on the file at all, and it is the only
place in the program that calls `Sync`.

## Group commit

The writer never waits. It commits whatever is queued the moment it is idle,
then picks up whatever arrived while that fsync was running. Batch size follows
load: one client gets one record per fsync and no added latency, thirty-two
clients get fifteen or so records per fsync.

The plan I started from specified a 2ms collection window instead. That turned
out to be worse than no batching at all for a single producer, and the README
has the numbers. The failure is instructive: a window short enough not to hurt
a lone producer is too short to collect anything useful, and a window long
enough to collect is pure latency when there is nobody to collect from. Letting
the in-flight fsync be the window removes the constant entirely.

One thing the current design gives up: when a batch completes, the writer
releases all its callers and immediately takes whatever is in the channel. The
goroutines it just woke have not been rescheduled yet, so some of them miss the
next batch and wait for the one after. That is why records per fsync comes out
near half the client count rather than equal to it. A brief spin before
committing would recover it. I did not add one, because it reintroduces a
tuning constant to buy throughput that is already sufficient.

## Delivery semantics

At-least-once. Not exactly-once, and the difference matters to anyone doing
change data capture.

A dequeue grants a lease with a token and a visibility deadline. Attempts
increment when the lease is granted, which makes `attempts` the number of
deliveries and puts the max-attempts check in exactly one place: the path that
returns a message to the queue. Ack requires the token of the lease that
currently owns the message, so a consumer that surfaces after its lease lapsed
and the message was redelivered gets a 409 rather than acking somebody else's
delivery.

Leases do not survive a restart. A lease is a promise to a consumer connection
that no longer exists, so recovery returns in-flight messages to the ready set
with their attempt counts intact. A consumer that had a message when the
process died will see it again, which is the at-least-once contract restated as
a recovery rule.

## Not built, and why

**Compaction.** The log grows without bound. This is the most serious
limitation and I am not going to dress it up: run this long enough and you run
out of disk, and recovery time grows linearly with everything that ever
happened rather than with what is currently queued.

The design is not hard. Serialize live state to `snapshot-<lsn>.tmp`, fsync it,
`rename` it to `snapshot-<lsn>.bin` because rename within a directory is
atomic, fsync the directory, then delete every segment fully covered by that
LSN. Recovery becomes load-snapshot-then-replay-the-tail, and a CHECKPOINT
record joins the vocabulary to mark where the snapshot cut. I left it out
because it complicates the one part of the system I most wanted to be obviously
correct, and a half-finished compactor that corrupts state on restart is worse
than an honest unbounded log. Knowing the answer and choosing the deadline over
it seemed like the better signal.

**Replication.** Single node. There is no leader election, no quorum, no
follower. If the disk dies, the queue dies with it. Durability here means
surviving process death, not hardware death.

**Authentication and authorization.** None. Anyone who can reach the port can
drain your queue.

**Backpressure and quotas.** A producer can enqueue until the process runs out
of memory. There is no maximum queue depth and no rejection path.

**Dead-letter redrive.** Messages that exhaust their attempts are moved to a
per-queue dead-letter list and counted in stats, but there is no endpoint to
read them back out or replay them. The bodies are kept so that endpoint stays
easy to add.

**Message bodies are strings.** The HTTP layer takes `body` as a JSON string,
so binary payloads need base64 from the caller. The storage layer is
byte-oriented and does not care; this is an API shortcut.

**`oldest_age_ms` costs a scan.** Stats walks the live message map to find the
oldest enqueue time, which is O(n) under the queue lock. A fourth heap would
fix it. At a one-second poll interval and a hundred thousand messages it costs
about a millisecond, and I would rather have three heaps to keep in sync than
four.

---

# The four answers

## How replay messages are handled

Delivery is at-least-once, and every mechanism here follows from admitting
that up front.

A dequeue grants a lease rather than removing the message. The message goes
invisible for the visibility timeout and the consumer gets a token. If the
consumer acks with that token, the message is gone once the ACK record is
fsynced. If the consumer nacks, or the lease expires, or the process dies, the
message comes back for redelivery with its attempt count incremented. Once
attempts reach the queue's maximum, the next return sends it to the
dead-letter queue instead of back into rotation.

Duplicates get collapsed at two different points. On the way in, an optional
`dedup_key` holds a five-minute claim on the message ID it created, so a
producer that retries after a timeout gets the original ID back instead of
creating a second message. The claim is published only when the original
enqueue's fsync completes, so a retry that arrives mid-fsync waits for the
original rather than being told about a message that might still fail to reach
disk. On the way back from a crash, replay is a fold over an append-only log:
applying the same records twice yields the same state, so recovery cannot
double-apply anything no matter how many times it runs.

What none of that gives you is exactly-once processing, and claiming otherwise
would be dishonest. The gap is not in the queue. It is that acking and doing
the work are two separate actions on two separate machines, and there is no
ordering of them that survives a consumer dying in the window between. Ack
first and you can lose work. Ack second and you can repeat it. This queue
chooses to repeat it, and the consumer has to be idempotent for the system as a
whole to be correct. For a CDC pipeline that usually means an upsert keyed on
something stable rather than an insert, which is a discipline most such
pipelines already have.

## Refactoring into Pub/Sub

Most of the work is already done, and not by accident. The WAL is an immutable
ordered log, which is the substrate a publish-subscribe system wants. What
makes this a queue rather than a topic is that the mutable state sits in one
place, shared by all consumers: one ready heap, one in-flight set, and an ack
that deletes the message for everybody.

The refactor moves that mutable state per subscription. Add a subscription
registry, persisted with its own record type. Give each subscription a cursor
into the log and its own in-flight set and attempt counts. Delivery becomes
reading forward from the cursor instead of popping a shared heap, and an ack
advances one subscriber's cursor without affecting anyone else's. Fan-out is
then N independent cursors over one log, and the log is written once no matter
how many subscribers exist.

The ordering configuration becomes per-subscription policy rather than
per-queue state, which is a small win: two subscribers on the same topic could
consume it in different orders. Priority and LIFO get more awkward, because
they want a heap of pending work while a cursor wants a position, so a
subscription with non-FIFO ordering needs a materialized window rather than a
plain offset.

The real cost is retention. Today the answer to "when can I delete this
message" is "when it is acked", and that is why segments could in principle be
reclaimed by compaction alone. With subscriptions, a message is needed until
every subscriber has passed it, and subscribers can be offline, slow, or gone
forever. So you inherit a retention policy: time-based, size-based, or
committed-offset-based, plus a decision about what happens to a subscriber that
falls behind the retention window. That is a problem a queue simply does not
have, and it is the part I would budget time for.

## What I would add with more time

Compaction first, because unbounded log growth is the limitation that most
constrains where this could actually run. The design is above and it is a day
of work done carefully.

Replication second. A single-node queue that survives process death but not
disk death is a narrow guarantee. The log makes this tractable: ship records to
followers, have the writer wait for one follower to acknowledge before
releasing callers, and the durability invariant extends from "on this disk" to
"on two disks" without changing the shape of anything. Leader election is the
part that would take real time, and I would not write my own consensus.

Then backpressure. Right now a producer can enqueue until memory runs out. A
max-depth per queue and a 429 with a retry hint is not much code and turns an
outage into a slowdown.

A batch ack endpoint is an easy throughput win, since acks are currently one
record and one fsync each while a batch dequeue already costs only one. A
consumer that takes ten messages should be able to settle them in one call.

After that: OpenTelemetry spans threaded through enqueue, lease and ack, so a
message's life shows up in a trace; a transactional outbox helper for consumers
that want end-to-end exactly-once against a database they control; and gRPC
alongside HTTP, mostly to drop per-request JSON parsing from the hot path.

## Why choose this over SQS, RabbitMQ or Pulsar

For most production workloads at scale, you should not. SQS, RabbitMQ and
Pulsar have replication, operational tooling, years of production exposure and
teams paid to keep them running. This is a two-day project with one node and no
replication. Anywhere durability really matters and the operational burden is
acceptable, pick one of them.

The niche is where the operational burden is the problem.

It is a single static binary with no dependencies, and its entire state is one
directory. That makes it embeddable in places a broker cannot go: integration
tests that need real queue semantics instead of an in-memory fake, an edge
deployment with no operator, a local development stack where the alternative is
a container and a compose file. Start it, point it at a directory, kill it,
restart it, delete the directory. There is no cluster to bootstrap.

The second thing is composable ordering. Priority, LIFO and delay are three
independent switches on one comparator, so a delayed priority LIFO queue is a
configuration rather than a feature request. The incumbents each cover part of
this, and to my knowledge none exposes all three as orthogonal options on one
queue, but I would verify the specifics before repeating them in an interview:

- SQS has delay queues and message timers, and I believe it has no priority
  ordering in either standard or FIFO queues. Worth confirming.
- RabbitMQ supports priority queues via `x-max-priority`. My understanding is
  that priority interacts badly with consumer prefetch, because messages already
  pushed to a consumer are not reordered when something more urgent arrives.
  Confirm the current behaviour before relying on it.
- Pulsar has delayed and scheduled delivery. I am not confident about its
  priority story and would check rather than assert.

The third thing is that the durability contract is small enough to state in one
sentence and verify in one script, which is worth something when the reason you
are evaluating a queue is that you do not trust the one you have.

That is the honest case. It is a narrow one.
