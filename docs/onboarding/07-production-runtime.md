# Chapter 7. One goroutine owns the broker state in production

Chapters 1 to 6 stay inside the deterministic world. `core/` defines Events and Actions as plain data. `broker/` is a pure state machine. `sim/` schedules every completion on a simulated clock. None of that code opens a socket, reads a wall clock, or starts a goroutine. This chapter explains the one layer that does real input and output. That layer is `sys/` and `cmd/manibroker/`.

You keep the machine pure for one reason. One goroutine owns the broker state, so the machine sees one event order and needs no lock for that state. The loop goroutine calls `Step`, and no other goroutine calls `Step`. The cost is strict. The loop goroutine must never wait for a disk, a network, or S3. Real input and output belongs in `sys/prod` adapters behind the `sys` interfaces. The executor does that work on other goroutines, and the work returns as one completion in the inbox.

## 7.1 The `sys/` layer holds every real input and output

`sys/sys.go` opens with a description of the layer. The layer gives you narrow interfaces over real input and output. The production Executor calls the interfaces from goroutines. SDK types and HTTP types never pass an adapter. The simulator does not implement the interfaces. The simulator implements `sim.Engine` over the same Action and Event vocabulary.

| Path | Role |
|---|---|
| `sys/sys.go` | Interfaces. `Clock`, `ObjectStore`, `ControlPlane`, `Conns`, `Classifier`, `Sink`, `Executor`, and the `Bundle` carrying them |
| `sys/run.go` | `sys.Run`, the single loop goroutine that owns one machine incarnation |
| `sys/inbox.go` | `Inbox`, the two queues (control, data) behind `Sink` |
| `sys/signal.go` | `ShutdownOnSignal` turns SIGTERM/SIGINT into a closed channel |
| `sys/prod/` | Production runtime. `Executor`, `Net` (TCP), `Clock`, `Env`, `Metrics`, `SegmentLog`, memory sizing |
| `sys/s3store/` | `sys.ObjectStore` over AWS SDK v2 S3, plus its `Classifier` |
| `sys/httppool/` | HTTP pool sizing shared by the S3 and CP clients |
| `sys/awsobs/` | Smithy middleware that measures every AWS SDK HTTP attempt |
| `sys/runinfo/` | Run identity. Logger, `build_info`, run manifest, `/metrics`+`/healthz` mux |
| `cmd/manibroker/` | The broker binary. Flags (`main.go`), tuning (`tuning.go`), size parsing (`units.go`) |

The package doc for `sys/prod/` calls the package "nondeterministic by design and is not in the determinism gate." The gate protects `core/`, `broker/`, and `sim/`. This chapter covers the code outside that gate.

## 7.2 One loop goroutine owns the machine in `sys.Run`

`sys.Run(ctx, b Bundle, exec Executor, f core.Factory, self core.NodeRef)` in `sys/run.go` drives the machine in production. The simulator kernel drives the same machine in tests. Two drivers run one machine, and the drivers differ only outside the machine.

The single owner buys you a plain order of events. The loop goroutine dequeues one event, stamps the event, and calls `Step` once. The executor never calls `Step`, and a connection reader never calls `Step`. You can compare the prod event order with a sim event order because both drivers use `core.ControlFirst`. The cost sits in `sys.Run`. The loop goroutine must keep moving, so `sys.Run` gives every blocking call to the executor or to a timer.

`sys.Run` works in this order:

1. The loop goroutine refuses a nil `Bundle.Inbox`. A private inbox stays unreachable for the Executor and for connection readers. Completions vanish in a private inbox.
2. The loop goroutine builds the machine with `f(b.Env, self)`. The loop goroutine delivers the first event with `step(core.Started{Self: self})`.
3. The loop goroutine checks `ctx.Err()` on each turn. The loop goroutine also checks the graceful-stop kill deadline before the loop goroutine picks work. The code states the reason in plain words. A busy inbox cannot starve cancel, and a busy inbox cannot starve the deadline. `select` never decides priority. The pure `core.ControlFirst` policy in `core/policy.go` picks the control head if the control queue is non-empty. The policy picks the data head if the control queue is empty. The loop goroutine sleeps on the inbox wake channel, `ctx.Done()`, shutdown, or the kill timer if both queues are empty.
4. The loop goroutine stamps the event at dequeue and only at dequeue. The stamp is `core.Event{At: b.Clock.Mono(), Wall: b.Clock.Wall(), Body: body}`. `sys.Clock` is "read only by the runtime, at inbox dequeue, to stamp Event.At/Wall." A fact duration is a difference of event times. That rule makes metrics identical in sim and prod (§7.6).
5. `m.Step(event)` returns `[]core.Action`. `runner.dispatch` dispatches the actions in slice order.

You read the return value as the end of one incarnation:

| `sys.Run` returns | Meaning |
|---|---|
| `nil` | Machine emitted `core.Exit` (graceful stop completed) |
| `ErrTripped` (wrapped) | Machine emitted `core.Trip`. Its memory can no longer be trusted. The supervisor should restart the process |
| `ErrStopped` | `ctx` cancelled (`Node.Kill`) |
| `ErrKilled` | `Bundle.Shutdown` fired, `StopGrace` elapsed, machine still had not emitted `Exit` |

On every return, `Run` closes the inbox with `defer in.Close()`. That close is the incarnation boundary. Late completions drop at the close. Late completions never block. Late completions never reach the next incarnation.

### `dispatch` keeps non-input and non-output actions on the loop goroutine

`runner.dispatch` in `sys/run.go` handles non-I/O actions in the loop goroutine. `runner.dispatch` gives each `core.IOAction` to the Executor.

| Action | Runtime behaviour |
|---|---|
| `core.IOAction` (Send, Dial, Obj*, CPCall) | `exec.Execute(a)` (§7.3) |
| `core.Emit` | `b.Facts(a.Fact)` if a facts sink is wired (§7.6) |
| `core.SetTimer` | `time.AfterFunc(a.After, …)` → `DeliverControl(core.TimerFired{ID, Gen})`. Replacing stops the old timer. The machine ignores stale generations |
| `core.CancelTimer` | Stops and deletes the `time.Timer` |
| `core.CloseConn`, `PauseReads`, and `ResumeReads` | Called directly on `b.Conns` (in prod, `*prod.Net`) |
| `core.Trip` | Returns `ErrTripped`. Actions before it in the slice were already started |
| `core.Exit` | Returns `nil` |
| anything else | `panic("sys: unknown action …")`. This is a programming error, not data |

## 7.3 The executor moves real input and output off the loop goroutine

The `sys.Executor` contract in `sys/sys.go` has three rules. The executor "runs I/O only." The executor gives you "exactly one Completion per IOAction, with the same ReqID, delivered to the Sink and never on the caller's goroutine." "Execute MUST NOT block: it hands work to goroutines/queues." In this chapter, the executor means `sys/prod.Executor` in `sys/prod/executor.go`. The executor implements the contract in this way:

| Action | Becomes | Completion |
|---|---|---|
| `core.ObjPut`, `ObjGet`, `ObjDelete`, `ObjList` | `go func() { e.deliver(e.object(a)) }()`. One goroutine per action. `context.WithTimeout` of `ObjTimeout + len/ObjTransferRate`. Then `objs.Put/Get/Delete/List` | `ObjPutDone`/`ObjGetDone`/`ObjDeleteDone`/`ObjListDone{Req: a.Req, …}` |
| `core.CPCall` | `go func() { e.deliver(e.control(a)) }()`. `cp.Call(ctx, a.Op)` under `CPTimeout` (default 10 s) | `CPDone{Req, Resp, Outcome}` |
| `core.Send` | Not a fresh goroutine. `conns.WriteAsync(conn, data, done)` queues onto that connection's single ordered writer goroutine (§7.5). Only if queueing fails is `done` run via `go done(err)` | `SendDone{Req, Conn, Err}` from the writer goroutine, once bytes left |
| `core.Dial` | Goroutine delivering `DialDone{Err: core.ErrInvalid}` immediately. "brokers reach the CP through CPCall," so prod has no raw dials. Dial exists for sim workloads | `DialDone` |

Errors become data before the core sees the errors. On failure the executor calls `e.class.Classify(a, err)`. The call returns plain `(core.ErrCode, core.Outcome)` values. `prod.Classifier` in `sys/prod/classify.go` routes by action type. The Classifier sends CPCall to the CP classifier. The Classifier sends each object action to the object classifier. `sys/s3store/classify.go` maps SlowDown, 503, and 429 to `ErrUnavailable`+`NotApplied`. The Classifier maps a PUT timeout to `ErrTimeout`+`MaybeApplied`. The PUT may have landed before the timeout. The Classifier maps a GET failure to `NotApplied` in all cases. "Getting Outcome right here is what makes acks honest." The broker may ack a produce only after a commit with an outcome of `Applied`.

`Executor.deliver` checks `sink.Closed()` first. `Executor.deliver` drops the completion if the sink has closed. A goroutine that finishes after `sys.Run` returns cannot block on a dead inbox.

The object-store timeout takes size into account. The formula is `objTimeout(n) = ObjTimeout + n × (1s / ObjTransferRate)`. The defaults are 30 s and 2 MiB/s. A 16 MiB segment PUT gets about 38 s. A flat 30 s limit does not end a 16 MiB PUT on a slow link at 30 s.

## 7.4 Control completions run before new data in the two-queue inbox

`sys.Inbox` in `sys/inbox.go` gives the node two queues. `core.ClassOf` in `core/inbox.go` classifies each item.

Control covers everything the node caused itself. The control items are all `*Done` completions, `TimerFired`, `Started`, and `Stop`. `DeliverControl` never blocks. The queue is an unbounded slice. The slice is "bounded by construction: the machine caps in-flight work." The queue has nothing to drop.

Data covers `ConnOpened`, `BytesReceived`, and `ConnClosed` from connection readers. `DeliverData` first takes a slot from a semaphore channel of capacity `dataCap`. The prod default is `DataInbox = 1024`. `DeliverData` blocks when the semaphore has no slot. `ConnClosed` is Data. The classification means a client end of file can never pass the last request from that client.

`ControlFirst` drains control before data. Under a produce flood, completions free buffer space and generate acks. Completions run before the loop goroutine admits new bytes. Prod and sim run the same `core.InboxPolicy` code. `core/inbox.go` states the rule in one sentence. "Go's select must never decide priority." The flow-control decision sits in `docs/flow-control.md` §3.

### Backpressure reaches the client in three steps

Three mechanisms apply in sequence:

1. The broker sets the limit. At `MaxBufferedBytes` of unacked produce bytes the broker machine emits `backpressure_on`. The broker machine also emits a `PauseReads` action for every open connection. The broker machine resumes at `ResumeBufferedBytes`. The default resume value is half of the max value. This rule sits in `broker/broker.go` and `docs/flow-control.md` §2.
2. The network applies the pause. `prod.Net.PauseReads` parks that connection reader goroutine on a `resume` channel in `sys/prod/net.go`. The socket buffer fills after the reader parks. TCP window pressure then reaches the client. The loop goroutine still processes bytes that the reader received before the pause.
3. The inbox limits deliveries. Readers can still outrun the loop goroutine after the pause. `DeliverData` then blocks on the 1024-slot semaphore. The `sys.Sink` doc calls the semaphore a safety net and states that real backpressure is PauseReads.

### The buffer cap is `clamp(memory/16, 128MiB, 1GiB)`

The `-max-buffered` flag help says `auto = clamp(memory/16, 128MiB, 1GiB)`. `prod.AutoBuffered` in `sys/prod/memory.go` implements that rule.

```go
func AutoBuffered(limit uint64) (max, resume int) {
	m := uint64(MinBufferedBytes) // 128 << 20
	if limit != 0 {
		m = min(max64(limit/16, MinBufferedBytes), MaxBufferedCap) // 1 << 30
	}
	return int(m), int(m / 2)
}
```

Unknown memory gives 128 MiB. A known limit gives `limit/16` clamped into [128 MiB, 1 GiB]. Resume is half of max. A 1 GiB container gives 128 MiB at the floor. An 8 GiB container gives 512 MiB. A 64 GiB container gives 1 GiB at the cap. The comment gives the reason for the 1/16 share. The broker holds about 2-3 copies of buffered record data. The copies are the request frame, the admitted batch, and the encoded segment. A 1/16 share of RAM keeps the total use near a fifth of memory.

`prod.MemoryLimit()` finds `limit`. `prod.MemoryLimit()` prefers the container limit over the host total. `prod.MemoryLimit()` checks cgroup v2 at `/sys/fs/cgroup/memory.max`. `prod.MemoryLimit()` checks cgroup v1 at `memory.limit_in_bytes`. `prod.MemoryLimit()` checks `/proc/meminfo` `MemTotal`. `prod.MemoryLimit()` then uses the OS fallback. The fallback is `sysctl hw.memsize` on Darwin. Other systems report "unknown." The code rejects cgroup `max` as no limit. The code also rejects the ~2⁶³ "unlimited" values as no limit. `cmd/manibroker/tuning.go` wires the result into the broker config. `auto` calls `AutoBuffered(mem)`. An explicit size overrides the auto value. Resume is half of an explicit size. An explicit `-resume-buffered` must be positive and ≤ max.

## 7.5 Real adapters sit under the `sys` interfaces

### One reader and one writer serve each TCP connection

One accept goroutine delivers `ConnOpened` to the data inbox. The accept goroutine then starts one reader goroutine and one writer goroutine for each connection. Readers read 64 KiB chunks. Readers call `DeliverData(BytesReceived)`. Writers drain a queue for one connection in call order. Each write has a 30 s deadline. Writers report each write through `SendDone`. A graceful close queues a FIN item. The close then drains the peer for a 2 s linger. Unread input cannot turn the close into a RST that removes final acks. An abort sets linger 0 and resets the connection. `OpenConns()` feeds the `manibroker_open_connections` gauge. You find this code in `sys/prod/net.go`.

### The S3 store lets the broker own retries

The store uses AWS SDK v2 over the pooled transport. The store sets `aws.NopRetryer{}`. The broker owns retries, and the store does not retry. The machine sees a retry as a new Action with backoff. The SDK does not hide a retry from the machine. The store prepends the prefix to every key. `List` strips the prefix from each key. `Get` issues a `Range` header from `core.SegmentRef{Off, Len}`. `Get` translates a range at the end of the object into an empty result. `Get` does not return an error for that range. `Delete` sends a HEAD request first. Deleting an absent key returns `ErrNotFound`. The store passes conformance check C3 against `veritas`. The simulator fake passes the same check. You find this code in `sys/s3store/store.go`.

### The HTTP pool keeps 256 idle connections for each host

Go's default keeps 2 idle connections per host. A caught-up consumer fleet and hundreds of in-flight S3 requests would dial fresh connections at every burst. Those connections would stay in TIME_WAIT. The comment cites one measurement. "16k sockets in six seconds were measured." The pool sets `MaxIdleConnsPerHost = 256`. The pool sets `MaxConnsPerHost = 1024`. The pool sets `IdleConnTimeout = 60s`. The pool sets `ForceAttemptHTTP2 = false`. The S3 client uses the pool through `httppool.AWS()`. The CP client uses the same pool. You find this code in `sys/httppool/httppool.go`.

### Smithy middleware records each AWS attempt

Smithy middleware on the S3 client records one histogram observation for each HTTP attempt. SDK retries stay visible in the attempt data. The middleware records one histogram observation for each operation. The middleware also records retry counters, request-byte counters, API-error counters, and throttle counters. `manibroker` wires the counters as `manibroker_s3_*`. The CP store wires the counters as `manicp_dynamo_*`. Every method accepts a nil `*Recorder`. A nil recorder records no metrics and adds no cost. You find this code in `sys/awsobs/awsobs.go`.

### The prod clock and the prod env remove chance from the machine

`Clock.Mono` counts from clock creation. `Clock.Mono` uses the Go monotonic reading. `Wall` is UTC UnixNano. `prod.Env` is the production `core.Env`. ChaCha8 gets a seed from `crypto/rand`. `Buggify` does no work in prod. `Maybe` returns false in all cases, and `Fault` returns false in all cases. Assertions log an Error line and do not stop the process. In prod a violated invariant becomes telemetry. In sim the same call fails the run. You find this code in `sys/prod/clock.go` and `env.go`.

## 7.6 Metrics are facts, so sim and prod report the same durations

The broker core imports no Prometheus. The broker core imports no logger. The broker core imports no metrics type. The broker core emits `core.Emit{Fact}` actions. Decision 36 uses the name "metrics-are-facts" for this rule. A `Fact` has a stable name. A `Fact` has ordered plain-data fields. Fact durations are differences of event times. The runtime stamps those event times at dequeue (§7.2). An invariant in sim reads the same values that prod reports.

`sys.Run` turns `Emit` into `Bundle.Facts(f)`. In `manibroker`, `nodeConfig` in `cmd/manibroker/main.go` sets that function. The function calls `prod.Metrics.Observe` if `-metrics-addr` is non-empty. The function also calls `prod.SegmentLog.Observe`. `prod.SegmentLog.Observe` runs in all cases. The same facts feed two consumers.

`sys/prod/metrics.go` aggregates facts into a private Prometheus registry. The registry uses the namespace `manibroker`.

  | Fact | Collectors |
  |---|---|
  | `segment_sealed` | `segments_sealed_total{reason}`, `segment_sealed_batch_bytes_total`, `segment_open_age_seconds`, `segments_in_flight_at_seal`, `segment_partitions`, gauges `segments_in_flight`, `buffered_bytes` |
  | `segment_put_done` | `segment_put_seconds`, `segment_put_attempts_total` |
  | `segment_committed` | `segment_commit_seconds`, `segment_queue_wait_seconds`, `segment_seal_to_ack_seconds`, `segments_committed_total`, `segment_committed_bytes_total` |
  | `segment_failed` | `segments_failed_total`, `segment_seal_to_ack_seconds` |
  | `backpressure_on` and `backpressure_off` | `reads_paused` (gauge), `reads_paused_seconds_total` |

`cp_call_seconds{op}` also reports CP calls. `cpclient.Client.Observe` supplies that data through a separate wire. The registry includes Go collectors and process collectors. `sys/runinfo` adds `manibroker_build_info`. The labels are version, go_version, git_sha, and run_id. `sys/runinfo` adds `start_time_seconds` and `uptime_seconds`. The single loop goroutine calls `Observe`. Collectors accept concurrent scrapes. `serveMetrics` serves the registry on `-metrics-addr`. The default is `:9100`. `runinfo.Mux` serves the registry. `runinfo.Mux` also answers `/healthz`.

`sys/prod/seglog.go` writes one structured log line for each segment lifecycle event. The events are `segment_sealed`, `segment_put_done`, `segment_committed`, `segment_failed`, and `backpressure_on/off`. The `segment_sealed` fact carries no key. `SegmentLog` pairs that fact with the next `ObjPut`. `SegmentLog` sees `ObjPut` through `NodeConfig.OnIO`. `NodeConfig.OnIO` is a `tapExecutor` wrapper. The wrapper shows every action to the hook on the loop goroutine before the wrapper executes the action. The `-log-level` flag help gives you one instruction. "raise -log-level to warn to keep only failures"

Run identity ties the three outputs together. `sys/runinfo.New` builds an `Info`. The run id comes from `-run-id`. The run id comes from `$MANIFESTER_RUN_ID` if `-run-id` is empty. The code generates `run-<utc>-<hex>` if both values are empty. Version data and git data come from build info or from `-ldflags -X`. `NewLogger` stamps `run_id`, `service`, `version`, and `git_sha` on every JSON log line. `runinfo.Manifest` and `Write` can save every effective flag at startup. `runinfo.Manifest` and `Write` can also save resolved tuning at startup. The flag is `-manifest-out`. A manifest write failure never stops startup.

## 7.7 Graceful shutdown gives the machine 10 s to emit `Exit`

Graceful shutdown follows these steps:

1. `main` wires `sys.ShutdownOnSignal(syscall.SIGTERM, os.Interrupt)` in `sys/signal.go`. The first signal closes the channel. Later signals get the default disposition.
2. `prod.Node` watches the channel in `stopOn`. `prod.Node` calls `Node.Stop()`. `Node.Stop()` closes the node internal `stop` channel one time. `Node.Stop()` uses `sync.Once` for that close. That channel is `Bundle.Shutdown`.
3. `sys.Run` delivers `core.Stop{Deadline: Mono + StopGrace}` to the control queue. In-flight produce can finish and receive answers. `sys.Run` arms a real `time.Timer` for `StopGrace`. The default is 10 s. The flag is `-stop-grace`.
4. The machine emits `Exit` before the deadline in a clean stop. `Run` returns nil. `Node.run` gives `Net.Shutdown` a `FlushGrace`. The default is 5 s. Writers flush the last answers before the code resets connections.
5. The deadline can fire first. `Run` then returns `ErrKilled`. The process skips flush grace. The code resets remaining connections. That error reaches `serve` in `manibroker` through `n.Wait()`. The process exits 1.
6. `Kill()` cancels the context. `Run` returns `ErrStopped` on the next turn. The code resets connections at once. Tests use `Kill()` to simulate crashes.

Exit codes come from `cmd/manibroker/main.go`. A flag parse error gives 2. `-h` gives 0. Any `serve` error gives 1. `ErrTripped` and `ErrKilled` count as `serve` errors. The process writes one `slog.Error("manibroker", …)` line for a `serve` error. A clean stop gives 0 after `manibroker stopped`.

## 7.8 `cmd/manibroker` wires the runtime, then starts `sys.Run`

`serve()` in `cmd/manibroker/main.go` runs in a fixed order:

1. `serve()` parses flags.
2. `serve()` builds `runinfo.Info` and the JSON logger.
3. `serve()` builds `nodeConfig`.
4. `nodeConfig` builds the CP client, applies tuning, and sets the facts chain.
5. `serve()` calls `openObjects`, and `openObjects` returns S3 storage or mem storage.
6. `serve()` starts the metrics listener.
7. `serve()` binds the Kafka listener and resolves the advertised address.
8. `serve()` calls `prod.Start`.
9. `serve()` writes the optional run manifest.
10. `serve()` calls `n.Wait()`.

`openObjects` has one branch. If you set `-objects mem`, `openObjects` uses the in-process store from `veritas/vsys` with `prod.MemClassifier`. The flag help describes that mode as "single broker, lost on exit." If you set `-objects s3`, `openObjects` builds `s3store.Store`. `openObjects` attaches the `awsobs` recorder when metrics are on. `openObjects` can call `EnsureBucket`. `prod.Start` in `sys/prod/node.go` validates the listener, the store, and the CP. `prod.Start` creates `sys.NewInbox(cfg.DataInbox)`. `prod.Start` creates `NewNet`. `prod.Start` creates `NewExecutor`. `prod.Start` wraps the executor with the `OnIO` tap if the config sets a tap. `prod.Start` builds the `sys.Bundle`. `prod.Start` runs `sys.Run(ctx, b, ex, broker.New(bc), self)` on a separate goroutine.

### Tuning knobs (`tuning.go`, `units.go`)

`parseSize` in `cmd/manibroker/units.go` parses sizes. `parseSize` accepts plain bytes. `parseSize` accepts binary `KiB/MiB/GiB/TiB`. `parseSize` accepts decimal `KB/MB/GB/TB`. `parseSize` accepts an optional fraction such as `1.5GiB`. `parseSize` accepts an optional space. `parseSize` ignores case. `parseSize` rejects values at or above 2⁶². Durations use Go `time.Duration` flag strings.

| Flag | Default | Resolved to | Effect |
|---|---|---|---|
| `-segment-bytes` | `16MiB` | `Broker.MaxSegmentBytes` | Seal the open segment at this many batch bytes |
| `-segment-window` | `200ms` (`broker.DefaultConfig().BatchWindow`) | `Broker.BatchWindow` | Seal this long after the segment's first batch |
| `-max-segment-partitions` | `4096` | `Broker.MaxSegmentPartitions` | Seal at this many distinct partitions (DynamoDB transaction store needs ≤ 96) |
| `-max-segment-batches` | `256` | `Broker.MaxSegmentBatches` | Seal at this many batches. This bounds the CP segment record (~300 B/batch) |
| `-max-buffered` | `auto` | `Broker.MaxBufferedBytes` | Pause client reads above this much unacked produce data. `auto` = `clamp(memory/16, 128MiB, 1GiB)` (§7.4) |
| `-resume-buffered` | half of max | `Broker.ResumeBufferedBytes` | Resume reads below this. It must be positive and ≤ max |
| `-commit-deadline` | `120s` | `Broker.CommitDeadline` | Retry a segment's CP commit (capped exp. backoff + jitter) this long, then fail its produces. Keep it under producers' `delivery.timeout.ms` |
| `-obj-timeout` | `30s` | `ExecConfig.ObjTimeout` | Base object-call timeout. Body size adds time at 2 MiB/s |
| `-stop-grace` | `10s` | `Bundle.StopGrace` | Time the machine gets after SIGTERM before `ErrKilled` |

Prod also uses fixed defaults with no flags. The defaults are plain values in code:

- The CP timeout is 10 s.
- The data inbox is 1024.
- Flush grace is 5 s.
- The read chunk is 64 KiB.
- The write timeout is 30 s.
- Close linger is 2 s.

`sys/prod/{executor,node,net}.go` sets these values in `withDefaults`.

The remaining flags set addresses and identity. You set the listen address with `-listen` (`127.0.0.1:9092`). You set the advertised address with `-advertise`. You set the broker number with `-broker-id` (1). You set the node name with `-node-id` (default `broker-<id>`). You set the CP address with `-cp-url`, and the flag takes a comma-separated failover list. The S3 flags are `-s3-endpoint/-bucket/-prefix/-region/-path-style/-create-bucket`. The other flags are `-metrics-addr` (`:9100`, empty disables), `-log-level`, `-debug`, `-run-id`, and `-manifest-out`. The process logs every effective value one time at startup. The log line is `manibroker tuning`. The line includes `memory_bytes` and `memory_source`. The manifest captures every effective value.

## 7.9 Two drivers run one machine, and the split tells you where a bug lives

Prod and sim run the same `broker` machine. Both drivers take the same Actions. Both drivers use the same `core.ControlFirst` policy. The drivers run in different outside worlds. The simulator does not implement `sys.Executor`. Each external system has one `sim.Engine`. The engines live in `sim/engines/{objstore,cpengine,logengine,netsim}`. Each engine routes by `Handles(a)`. Each engine answers through scheduled kernel entries.

You gain replay in this split. The sim driver controls time, faults, and randomness, so you can repeat one run from one seed. The prod driver controls none of those inputs. The prod driver instead classifies real errors, enforces real timeouts, and applies real backpressure. The cost is a second driver to keep honest. A timeout error, a pool limit, and a pause wire live only in `sys/`, and a sim run does not test those parts.

| Dimension | Production (`sys/prod.Executor`) | Simulator (`sim.Engine`) |
|---|---|---|
| Interface | `Execute(a core.IOAction)`, sync return, must not block | `Execute(ctx, from, a)`, schedules entries. "never completes anything synchronously" |
| Concurrency | One goroutine for each object action and each CP action. One writer goroutine per connection. One loop goroutine owns the machine | One goroutine total (kernel). Ordering comes from lanes and scheduled times |
| Time | `prod.Clock`, monotonic + UTC wall, read only at dequeue | Simulated `core.Time`, advanced by scheduler. Per-node clock skew is a fault knob |
| Completion path | Goroutine → `Classifier` (error → `ErrCode`+`Outcome`) → `DeliverControl` → stamped `Event` | `EngineCtx.Deliver(to, at, lane, body)` → kernel entry → stamped `Event` |
| Ordering | FIFO only where physics gives it. Per-connection writer queue. Control-before-data policy | FIFO guaranteed per lane key (`EngineCtx.Lane`). Distinct keys never share a lane |
| Faults | Whatever S3, TCP, and the CP do. The runtime classifies faults after the fact | Drawn from `CountedRand` at issue time: 503s, timeouts, lost responses, outages, corruption (`sim/engine.go` fault families) |
| Randomness and Env | `prod.Env`, crypto-seeded, buggify inert, assertions log-and-continue | Seeded `CountedRand`. Every draw counted in trace hash. Assertions fail the run |
| State of the world | Real bucket, real CP, real sockets. This state survives the process | Engine-owned maps folded into trace hash after every step (`core.Hashable`) |
| Crash | `Node.Kill` cancels ctx. Inbox close drops late completions | `OnCrash`. Applied effects stand. Only entries addressed to the dead node dropped. A PUT already on the wire can land after the broker dies |
| Observability | Facts → Prometheus + segment logs (this chapter) | Facts → timeline checked by invariants |

A bug reproduced in sim with a seed is a bug in the machine or the vocabulary. The bug is not in the code in this chapter. A bug that happens only in prod lives in classification, timeouts, pool sizes, or backpressure wiring. You put the fix in `sys/`.

## Key takeaways

- `sys.Run` is the only caller of `Step` in production. One goroutine runs `sys.Run`. `ControlFirst` picks each event. The runtime stamps `At/Wall` at dequeue. The runtime dispatches actions in slice order. The runtime closes the inbox on return.
- `prod.Executor` turns each object Action and each CP Action into a goroutine under a size-aware timeout. `prod.Executor` turns each `Send` into an entry on the connection ordered writer queue. One `Completion` with the same ReqID comes back. The completion never arrives on the caller goroutine.
- Errors never cross into the core as Go `error` values. A `Classifier` reduces each error to `ErrCode` + `Outcome`. The outcome is `Applied`, `NotApplied`, or `MaybeApplied`. Acks use that outcome.
- Backpressure uses a broker limit, a network pause, and an inbox limit. The broker limit is `MaxBufferedBytes`. The broker limit emits `PauseReads` with resume at `ResumeBufferedBytes`. The network parks readers. The inbox holds 1024 data slots. `-max-buffered auto` is `clamp(memory/16, 128MiB, 1GiB)`. Resume is half of max.
- Metrics are facts. `Emit` goes to `Bundle.Facts`. `prod.Metrics.Observe` writes Prometheus data. `SegmentLog` writes one line per segment. Durations come from event times. Sim and prod use the same duration rule.
- Shutdown follows a protocol. SIGTERM produces a `Stop` event with a deadline. The machine emits `Exit` before the deadline in a clean stop. The machine returns `ErrKilled` at `StopGrace` (10 s) after the deadline. The supervisor sees a non-zero exit after `ErrKilled`.

## Checkpoint

1. An `ObjPut` goroutine finishes 200 ms after `sys.Run` returned `ErrTripped`. What happens to its `ObjPutDone`, and at which two points is it dropped?

<details>
<summary>Answer</summary>

The `ObjPutDone` drops at two points. `prod.Executor.deliver` checks `sink.Closed()` and returns without delivering. `sys.Inbox.DeliverControl` can also drop it. The inbox is closed at that point because `Run` uses `defer in.Close()`. The completion cannot block. It cannot reach the next incarnation. See `sys/prod/executor.go` (`deliver`) and `sys/inbox.go` (`DeliverControl`, `Close`).
</details>

2. A broker runs in a container limited to 4 GiB with no tuning flags. What are `MaxBufferedBytes` and `ResumeBufferedBytes`, and where does the 4 GiB number come from?

<details>
<summary>Answer</summary>

4 GiB divided by 16 gives 256 MiB. 256 MiB sits inside the clamp. `MaxBufferedBytes` is 256 MiB. Resume is 128 MiB. `prod.AutoBuffered` returns `m, m/2`. `MemoryLimit` reads the cgroup limit first. It reads `/sys/fs/cgroup/memory.max` on cgroup v2. It prefers that value over host `/proc/meminfo`. The startup line `manibroker tuning` logs the source as `memory_source`. See `sys/prod/memory.go` and `cmd/manibroker/tuning.go`.
</details>

3. Which component decides that a client reader parks, which one actually parks it, and which queue is only a safety net?

<details>
<summary>Answer</summary>

The broker machine sets the park condition. At `MaxBufferedBytes` of unacked produce it emits `backpressure_on` and `PauseReads` actions. Resume uses `ResumeBufferedBytes`. `prod.Net` parks the connection reader goroutine on its `resume` channel (`sys/prod/net.go`). The data inbox 1024-slot semaphore in `DeliverData` (`sys/inbox.go`) is the safety net. It blocks reader deliveries if readers outrun the loop after the pause.
</details>

4. A segment's S3 PUT times out. What `(ErrCode, Outcome)` pair can the machine see, why that outcome, and who owns the retry?

<details>
<summary>Answer</summary>

The pair is `ErrTimeout` with `MaybeApplied` for a PUT. DELETE uses the same pair. The request may have landed before the caller gave up (`s3store.Classifier`, `sys/s3store/classify.go`). The SDK does not retry. `s3store.New` sets `aws.NopRetryer{}` because "the broker owns retries." The machine re-issues the PUT as a new Action. Segment keys are unique per upload, so a second PUT is safe. GET and LIST failures carry the same codes. GET and LIST failures are `NotApplied` in all cases.
</details>

5. SIGTERM arrives while produces are in flight. Trace the stop, and name what makes it end as `ErrKilled` instead of a clean exit.

<details>
<summary>Answer</summary>

`sys.ShutdownOnSignal` closes the shutdown channel. `Node.Stop` closes `Bundle.Shutdown` one time. `sys.Run` delivers `core.Stop{Deadline: Mono + StopGrace}` on the control queue. The default `StopGrace` is 10 s. `sys.Run` arms a kill timer. The machine finishes in-flight work and emits `Exit`. `Run` returns nil. `Net.Shutdown` gets 5 s `FlushGrace` to flush final acks. The stop ends as `ErrKilled` if the machine has not emitted `Exit` when the timer fires. `Run` checks the timer on every loop turn. A busy inbox cannot block that check. The process exits 1 through `n.Wait()` in `serve`. See `sys/run.go`, `sys/signal.go`, and `sys/prod/node.go`.
</details>

## Try it

1. Run the smallest local stack with no Docker and no AWS. Use one terminal for `go run ./cmd/manicp -store mem` (CP `:8080`, metrics `:9101`). Use another terminal for `go run ./cmd/manibroker -objects mem` (Kafka `:9092`, metrics `:9100`). Run `curl -s 127.0.0.1:9100/metrics | grep -E 'build_info|buffered_bytes'`. Run `curl -s 127.0.0.1:9100/healthz`. Press Ctrl-C in the broker terminal. Watch for `manibroker stopped` and exit code 0. Check the `manibroker tuning` line for `memory_bytes`/`memory_source` on your machine.
2. Exercise the clamp. Restart with `-max-buffered 512MiB -manifest-out /tmp/manifest.json`. Compare `max_buffered`/`resume_buffered` in the tuning log line and manifest against the auto values from step 1.
3. Run the broker against a real S3 backend. `./scripts/it-up.sh` starts SeaweedFS S3 on `127.0.0.1:8333` and the DynamoDB-compatible backend on `:18080` from `deploy/docker-compose.yml`. `./scripts/it-down.sh` stops them. Run the broker with `-objects s3 -s3-endpoint http://127.0.0.1:8333 -s3-bucket manifester -s3-path-style -s3-create-bucket` against the in-memory CP. Watch `manibroker_s3_attempt_seconds*` on `:9100/metrics` under load from `go run ./cmd/manibench benchmark-producer -topic t -cp-url http://127.0.0.1:8080 -create-partitions 3 -duration 10s`.
4. Break classification on purpose. Run `docker compose -f deploy/docker-compose.yml stop seaweedfs` during a run. Read the broker log for `segment_failed`. Compare that line with the `manibroker_s3_*` error counters when it restarts.

## Further reading

You can read the code and the design notes in this order:

- [`../../sys/sys.go`](../../sys/sys.go) and [`../../sys/run.go`](../../sys/run.go). Read the interface and the loop in full.
- [`../flow-control.md`](../flow-control.md). Read the design behind the two queues, `ControlFirst`, and `PauseReads` (§2-§3).
- [`../lessons.md`](../lessons.md). Read lessons 0, 1, 6, and 10 on the same vocabulary, errors as data, connection faults, and facts.
- [`../decisions.md`](../decisions.md). Read decision 35 on buffer sizing, decision 36 on metrics-are-facts, and the CP item limits behind `-max-segment-partitions`.
- [`../observability.md`](../observability.md). Read how operators use the metrics and segment logs to judge a run.
- [`../../sys/prod/metrics.go`](../../sys/prod/metrics.go) and [`../../sys/prod/seglog.go`](../../sys/prod/seglog.go). Read every fact consumer.
- [`../../sim/engine.go`](../../sim/engine.go). Read the `Engine`, fault-family, and latency vocabulary behind the contrast in §7.9.
- [`../aws-benchmark-runbook.md`](../aws-benchmark-runbook.md). Read the same flags and metrics at benchmark scale.
