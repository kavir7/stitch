# stitch

A durable message queue in Go. One binary, no dependencies, and a write-ahead
log that is the entire storage layer.

## Sixty seconds

```bash
git clone https://github.com/kavir7/stitch.git
cd stitch
make run
```

That builds the server and starts it on `:8080` with its log in `data/wal`.
In another shell:

```bash
curl -X POST localhost:8080/queues \
  -d '{"name":"jobs","mode":"fifo","priority":true,"dlq":true}'

curl -X POST localhost:8080/queues/jobs/messages \
  -d '{"body":"hello","priority":5}'

curl -X POST 'localhost:8080/queues/jobs/messages/dequeue?n=10&wait_ms=5000'
```

The dequeue gives you a `lease_token`. Ack with it:

```bash
curl -X POST localhost:8080/queues/jobs/messages/<id>/ack \
  -d '{"lease_token":"<token>"}'
```

`make run` also serves a dashboard at <http://localhost:8080/> with a producer,
a worker pool and a button that SIGKILLs the server so you can watch it come
back with nothing lost.

Go 1.22 or newer. Standard library only. `go.mod` has no `require` block
because there is nothing to require.

## One comparator, eight queues

The assessment asks for FIFO, LIFO, priority and delayed queues. I did not
build four features. I built one comparator and derived all eight combinations
from it:

```go
func less(a, b Key, lifo bool) bool {
	if a.Priority != b.Priority { return a.Priority > b.Priority }
	if lifo { return a.Seq > b.Seq }
	return a.Seq < b.Seq
}
```

A `Key` is a priority and a monotonic sequence number. Priority wins first;
when it ties, the sequence number decides. Turn priority off and every message
carries zero, so the first comparison never fires and the order falls through
to arrival. Turn delay off and `visible_at` is always the enqueue time, so
nothing is ever held back.

LIFO is FIFO with the sequence comparator inverted. That is the whole trick.
There is no LIFO code path, no priority code path and no delay code path
anywhere below that function. A delayed priority LIFO queue is three booleans,
not three features, and the property tests walk all eight configurations
against the same implementation.

Delay is the one axis that is not part of the comparison, because it decides
*when* a message enters the order rather than where it sits once it is there.
Delayed messages live in a second heap keyed by visibility time and get
promoted into the ready heap when due, keeping the sequence position they were
given at enqueue. A message delayed by ten seconds does not go to the back of a
FIFO queue when it wakes up. It goes where it always belonged.

## The durability invariant

An HTTP 2xx is returned to the producer only after the fsync covering that
record has completed.

Everything else follows. Group commit batches records so that one fsync covers
many of them, but no caller is released until the fsync that covers its own
record returns. Batching therefore costs latency and never durability. Nothing
is ever acknowledged that is not already on disk.

The corollary is the one the crash test checks: if the producer got a 201, the
message is in the log, and if the consumer got a 204 for its ack, the message
is gone from the log. Kill the process at any point and those two statements
still hold.

Two fsync policies are available. `--fsync=always` syncs once per record.
`--fsync=group` commits whatever is queued the moment the writer is idle and
folds in whatever arrives while that fsync runs. Group commit is the default
and it never loses to `always`, which took a rewrite to become true. See below.

## How it works

Records are framed as `[magic u32][payload_len u32][crc32c u32][type u8][payload]`
and appended to segment files that rotate at 64 MB. The CRC covers the type
byte and the payload. It cannot cover the length, since the length has to be
trusted before there is anything to checksum, so the length is instead
bounds-checked against a hard ceiling and against the bytes remaining in the
segment. A flipped bit anywhere in a frame is caught either way, and there is a
test that flips every single bit of a record to prove it.

On boot the server replays every segment in order and folds the records into
memory. If the last record in the final segment fails its bounds or CRC check,
that is a torn tail from a crash: the segment is truncated there and the
truncation is fsynced, so the damaged bytes cannot come back. The same failure
in an earlier segment is not a torn tail, because a crash can only ever damage
the file being appended to, so replay refuses to start rather than silently
discarding every good record that follows.

State is a deterministic fold over the log, and replay never reads a clock.
When a lease expires and its message is requeued, the record carries the
`visible_at` that the live path computed rather than the backoff that produced
it. Two replays of the same log therefore produce identical state, which is
asserted by hashing the recovered state twice and comparing.

One goroutine owns each log file, so the append path has no lock on the file at
all. Each queue has its own mutex, and it is never held across an fsync: every
operation reserves under the lock, appends without it, then commits under it
again. `go test -race ./...` is clean and is in the Makefile.

## API

```
POST   /queues                              {name, mode, priority, delay,
                                             visibility_timeout_ms, max_attempts, dlq}
GET    /queues
GET    /queues/{q}/stats
POST   /queues/{q}/messages                 {body, priority, delay_ms, dedup_key}
POST   /queues/{q}/messages/dequeue         ?n=10&wait_ms=5000
POST   /queues/{q}/messages/{id}/ack        {lease_token}
POST   /queues/{q}/messages/{id}/nack       {lease_token, requeue_delay_ms}
GET    /healthz
GET    /metrics                             Prometheus text format
POST   /debug/crash                         only routed with --unsafe-demo
```

Delivery is at-least-once. A dequeue grants a lease with a token and a
visibility deadline; an ack requires the token of the lease that currently owns
the message, so a consumer that surfaces after its lease expired and the
message was redelivered gets a 409 instead of acking somebody else's delivery.
Attempts increment when a lease is granted, and a message that runs out of
attempts goes to the dead-letter queue. Consumers still have to be idempotent,
and `docs/DESIGN.md` explains why that is not something the queue can fix for
them.

## Benchmarks

Apple M5 Pro, macOS, `go test -bench` with a fixed thousand iterations per
case. Reproduce with `make bench`. The latencies include the wait for the fsync
covering the record, because that is what the server charges a client.

| operation | fsync | clients | msg/s | p50 ms | p99 ms | records per fsync |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Enqueue | always | 1 | 318 | 3.01 | 4.13 | 1.0 |
| Enqueue | always | 8 | 321 | 24.12 | 31.94 | 1.0 |
| Enqueue | always | 32 | 323 | 97.88 | 114.50 | 1.0 |
| Enqueue | group | 1 | 310 | 3.01 | 5.00 | 1.0 |
| Enqueue | group | 8 | 1304 | 6.01 | 7.28 | 4.0 |
| Enqueue | group | 32 | 4856 | 6.37 | 8.01 | 15.6 |
| Dequeue | always | 1 | 297 | 3.03 | 6.05 | 1.0 |
| Dequeue | always | 8 | 315 | 25.00 | 31.20 | 1.0 |
| Dequeue | always | 32 | 321 | 99.02 | 109.90 | 1.0 |
| Dequeue | group | 1 | 323 | 3.01 | 4.10 | 1.0 |
| Dequeue | group | 8 | 940 | 8.05 | 12.03 | 4.0 |
| Dequeue | group | 32 | 5104 | 6.03 | 8.32 | 15.6 |

Read the `always` rows first. Throughput is flat at roughly 320 messages a
second no matter how many clients push, because every client waits behind its
own fsync, and the p50 grows linearly with concurrency: 3ms at one client, 98ms
at thirty-two. That is not a queue being slow. That is one fsync costing 3.1ms
and nothing amortizing it.

Group commit matches `always` exactly at one client and reaches 4,856
messages a second at thirty-two, fifteen times the throughput, with p99 falling
from 114ms to 8ms. The absolute numbers are modest because Go's `File.Sync` on
darwin issues `fcntl(F_FULLFSYNC)`, which asks the drive to flush its write
cache and costs about 3.1ms here. On Linux, where `Sync` is `fdatasync`, the
per-fsync cost is far lower and every row moves up together.

The interesting column is the last one. At thirty-two clients each fsync covers
15.6 records rather than 32, because when a batch completes the writer releases
its callers and immediately takes whatever is already in the channel; the
goroutines it just woke have not been rescheduled yet, so about half of them
miss the next batch. Over HTTP with 256 concurrent producers I measured 28.4
records per fsync and 8,316 messages a second, so it keeps scaling. A brief
spin before committing would recover the difference. I did not add one, for
reasons in the next section.

## The crash test

`scripts/crash_test.sh` enqueues 100,000 messages, starts sixteen consumers
acking as fast as they can, and sends `kill -9` to the server at a random point
between 0.3 and 3.0 seconds into consumption. No flush, no close, no shutdown
hook. It then restarts the server on the same log and reconciles.

The reconciliation is the part worth reading. The load generator records only
what the server confirmed: an ID goes into `enqueued.ids` on a 201 and into
`acked.ids` on a 204. Before every ack it also writes the ID to `pending.ids`,
so an ack whose reply died with the socket is provably ambiguous rather than
being quietly counted as a loss in either direction. After the restart the
verifier drains the recovered queue to find out what actually survived, then
checks two things: no confirmed ack came back, and no confirmed enqueue went
missing.

```
building
starting server on 127.0.0.1:18123 with fsync=group
running load: 100000 messages, 32 producers, 16 consumers
loadgen: enqueued 100000 messages in 38.073s (2627/s)
consumers are running; killing the server in 2.170s
SIGKILL to pid 32101 (about 3784 acks confirmed so far)
loadgen: server stopped answering after 3791 confirmed acks
scripts/crash_test.sh: line 74: 32101 Killed: 9               "$OUT/queued" -addr "$ADDR" -data "$DATA" -fsync "$FSYNC" >> "$1" 2>&1
server is dead; restarting on the same log

crash test result
-----------------
  enqueued (confirmed 201)       100000
  acked    (confirmed 204)         3791
  present after restart           96205
  redelivered (attempts >= 2)       205
  ack in flight when killed           4   (may be either, not counted as loss)

  LOST         (un-acked and gone)            0
  RESURRECTED  (acked and back)               0
  UNKNOWN      (present, never confirmed)     0

PASS: every acknowledged message stayed gone and every un-acknowledged message came back
recovery and drain took 2s
server logs: .crashtest/server-before.log and .crashtest/server-after.log
2026/08/09 18:16:34.855292 wal: recovered 103956 records from 1 segment(s) in .crashtest/data
2026/08/09 18:16:34.868288 queue: recovered 1 queue(s) from 103956 record(s)

CRASH TEST PASSED
```

The arithmetic closes exactly: 3,791 acked plus 96,205 recovered plus 4
ambiguous is 100,000. The 205 redeliveries are the leases that were open at the
instant of the kill, handed back out because a lease is a promise to a
connection that no longer exists.

One thing this test does not prove, and I would rather say so than let it be
discovered: `kill -9` does not test the fsync. It kills the process, but every
`write` is already in the kernel page cache and the kernel is still running, so
the data survives whether or not it was synced. I checked by deleting the
`Sync` call and running the crash test again, and it still passed. What this
test actually proves is the ordering: that a record is written before its
operation is acknowledged, that recovery folds the log back into the same
state, and that a torn tail is truncated rather than loaded. Proving the fsync
itself needs power loss or a block layer that can drop unsynced writes, neither
of which a test on a laptop can produce. The invariant that the response comes
after the `Sync` call returns is enforced by the structure of the writer, in
`commit`, where the batch is only released after the sync: it is not something
the test suite can catch a regression in.

## What surprised me

I built group commit the way I had planned it: collect records arriving within
a 2ms window, or up to 256 of them, then fsync once. Then I ran the tests and
the same 200 sequential appends took 1.21 seconds in `fsync=group` against 0.64
seconds in `fsync=always`. Group commit, the whole point of which is to be
faster, was nearly twice as slow as syncing every record individually.

The reason is obvious in hindsight and I had not thought about it at all. With
one producer in flight there is nobody to batch with, so the window is not
amortizing anything. It is 2ms of pure latency stacked on top of an fsync that
already costs 3.1ms because `File.Sync` on darwin is `F_FULLFSYNC` rather than
`fdatasync`. Every append paid for a party nobody came to. And the window can
never be tuned out of this: short enough not to hurt a lone producer means too
short to collect anything, long enough to collect means unacceptable when
traffic is thin.

The fix was to delete the constant. The writer now commits the instant it is
idle and batches only what queues up behind an fsync that is already in flight,
so the in-flight fsync *is* the window and it sizes itself. Same 200 appends,
0.63 seconds, and 256 concurrent appends now cost two fsyncs. That experience is
also why I left the scheduling gap in the last benchmark column alone: I had
just removed one tuning constant that looked reasonable and was not, and adding
another to chase 2x on a number that already scales seemed like the wrong
lesson to take.

The race detector also earned its place. It caught `Enqueue` returning a
pointer to the message it had just inserted into the ready heap, which a
consumer could lease and mutate while the HTTP handler was still reading it.
Everything looked correct and single-threaded testing would never have found
it. The fix was to snapshot inside the lock.

## What I did not build, and why

**Compaction.** The log grows without bound. Run this for long enough and you
run out of disk, and recovery time grows with everything that has ever happened
rather than with what is currently queued. That is a real limitation of this
submission and not a stylistic choice.

I know how to fix it: write live state to `snapshot-<lsn>.tmp`, fsync, `rename`
it into place because rename within a directory is atomic, fsync the directory,
then delete every segment the snapshot fully covers. Recovery becomes load the
snapshot and replay the tail. I left it out because it complicates the one part
of the system I most wanted to be obviously correct, and a half-written
compactor that corrupts state on restart is worse than an honest unbounded log.

Also absent: replication and leader election, so this survives process death
but not disk death; authentication of any kind; backpressure, so a producer can
enqueue until memory runs out; a redrive endpoint for the dead-letter queue,
though the messages are kept; and binary message bodies, since the HTTP layer
takes `body` as a JSON string. `docs/DESIGN.md` has the full list with the
reasoning for each.

## Layout and testing

```
cmd/queued        server binary
cmd/loadgen       crash-test producer and verifier
internal/wal      record codec, segments, group commit, recovery
internal/queue    comparator, heaps, leases, dead-letter, dedup, replay
internal/api      HTTP handlers and metrics
web/index.html    dashboard, compiled into the binary
scripts/          crash_test.sh, bench.sh
docs/DESIGN.md    tradeoffs and the four written answers
```

```bash
make test        # go test -race ./...
make vet
make bench
make crash-test
```

Sixty-seven test functions, thirteen of which are table-driven and fan out into
subtests. The ones that matter are the property tests that walk
all eight queue configurations and check every delivery against a model that
picks by linear scan instead of by heap, the recovery tests that damage a
segment six different ways and assert the torn bytes are removed from disk
rather than skipped in memory, and the crash test above.
