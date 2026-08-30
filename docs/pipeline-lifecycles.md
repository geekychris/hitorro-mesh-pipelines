# Pipeline lifecycles: terminating vs continuous

A mesh pipeline job has one of two lifecycles, decided by the shape of
its **sources** — nothing else in the spec matters for this choice.
The classification drives what "stop" means, how "resume from where I
left off" is implemented, and which UI badge you see.

| Shape | Sources | Ends when | UI badge |
|---|---|---|---|
| **Terminating** | Every source is finite (`sqlite`, `ndjson-file`, `csv-file`, `json-file`, `kvstore`, `lucene`, `sql`, `inline`) | Source(s) drain naturally | `▪ terminating` |
| **Continuous** | At least one source is unbounded (`kafka`, `nats`) | Only when cancelled — `DELETE /mesh/jobs/{id}` — or the driver dies | `● continuous` |

The runner computes this at job start
([`JobRunner.lifecycleOf`](../src/main/java/com/hitorro/mesh/pipelines/runtime/JobRunner.java))
and stamps it onto the `JobStatus` snapshot, so pollers and the UI
never need to re-parse the spec.

## Why the distinction matters

Terminating jobs are the batch case: kick them off, wait for
`SUCCEEDED`, move on. Their natural home is the [pipeline
scheduler](./scheduling.md) — "run this every hour, remember where you
got to."

Continuous jobs never reach `SUCCEEDED` on their own. They stay in
`RUNNING` until something interrupts them. Their natural home is
**boot-time resumption** via `restartable: true` — the driver
persists the spec at submit and re-submits it after a restart. Their
notion of "where I left off" is stored inside the **source**, not the
mesh pipeline framework:

- **Kafka** — the consumer group (`groupId`) tracks the last committed
  offset per partition. Restart the job → same group → resume from the
  same offset. No mesh code involved.
- **NATS JetStream** — the durable pull consumer's server-side ack
  state does the same job.

That's why a continuous job doesn't need the scheduler's `${CHECKPOINT}`
machinery — the checkpoint lives one layer down, in the broker's own
metadata. The scheduler is designed for terminating polls, not
long-lived stream tails.

## Stopping a running job

Both shapes stop the same way:

```
DELETE /mesh/jobs/{jobId}
```

This sets `status.cancelRequested = true`. The Kafka / NATS sources
check the flag between polls (~500ms tick), unblock, and let the
runner drain sinks cleanly. Cancel is idempotent — hitting DELETE on
a job that already terminated returns 200.

Terminating jobs can also be cancelled the same way — useful for
killing a long batch backfill that's producing wrong output.

## Resuming after a driver restart

| Shape | `restartable: true`? | Resume behaviour |
|---|---|---|
| Terminating | usually no | Re-run from the start of the source on next scheduler tick (or manual submit) |
| Terminating | yes | Driver's `PipelinesBootResumer` re-submits the same spec on boot — but it re-processes the entire source. Meaningful only when the source is idempotent, or paired with a scheduler that carries `${CHECKPOINT}` |
| Continuous | **yes** | Driver re-submits the spec; source resumes from its own committed offset. Zero data loss (Kafka), at-least-once (NATS with ack policy) |
| Continuous | no | You'd have to re-submit manually — usually a mistake |

`restartable` is a flag on `JobSpec` — set it in YAML:

```yaml
job: my-kafka-tail
restartable: true
nodes:
  - id: consume
    pipeline:
      source: { kind: kafka, bootstrap: localhost:9092, topic: events, groupId: my-app }
      sinks:
        - kind: jvs-lucene-sink
          name: events-index
```

That job runs forever. If the driver reboots, the same spec re-armed
under the same `job-N` id — the Kafka broker still holds the offset
for `groupId: my-app`, so processing picks up exactly where it stopped.

## Choosing between "poll on cadence" and "tail continuously"

Both patterns can end up doing roughly the same work; picking the
right shape is about failure modes and latency, not throughput:

**Prefer terminating + scheduled** when:
- Source has a natural cursor (`WHERE id > X`, IMAP `SEARCH SINCE`, HTTP `?since=`)
- You want per-run auditability (Runs count, `lastError`, `lastSuccessAt`)
- Latency requirements are minutes, not seconds
- The system you're polling doesn't want a permanent connection

**Prefer continuous + restartable** when:
- You have a push-native source (Kafka, NATS, MQTT, IMAP IDLE)
- You need sub-second latency
- Ordering / exactly-once semantics matter and the broker gives them
- You'd rather one long-lived JVM socket than the reconnect cost every N minutes

## Advancing the checkpoint from a terminating job

For terminating scheduled jobs, the default is that the scheduler
stamps `${NOW}` (the run's start time, ISO instant) as the new
checkpoint on success — perfect for "WHERE received_ts >
${CHECKPOINT_UNIX_SECONDS}" queries where the cursor is a time.

For non-time cursors (`WHERE ROWID > ${CHECKPOINT}`), the job itself
knows the value it consumed up to — the scheduler doesn't. A
`groovy-map` step can write the new checkpoint via the `ctx`
binding:

```yaml
- id: capture-max-id
  pipeline:
    source: { kind: ref, node: extract }
    steps:
      - kind: groovy-map
        script: |
          // Track the max ROWID we've seen in this batch.
          if (row.rowid > (binding.hasVariable('maxSeen') ? maxSeen : 0L)) {
              maxSeen = row.rowid
              // Push it to the scheduler on every advance — last-writer-wins,
              // so the value at end-of-run is what gets persisted.
              ctx.setCheckpoint(String.valueOf(maxSeen))
          }
          row
    sinks: []
```

The `ctx` binding is bound only when a step is running inside a job
launched by the scheduler (which is always the case for scheduled
runs). Its
[`setCheckpoint(String)`](../src/main/java/com/hitorro/mesh/pipelines/runtime/PipelineContext.java)
method sets `JobStatus.scheduledCheckpointOverride`; on success the
scheduler picks that up instead of stamping `${NOW}`.

The override wins over an operator's mid-run `PUT /checkpoint` — the
job knows more than the human here, by definition.

## Restartable ≠ scheduled

Two independent mechanisms that both help "survive downtime":

- **`restartable: true`** — driver re-submits the spec on boot. Owned
  by `JobSpec` + `RestartableJobStore`. Best paired with a continuous
  source that has its own offset store (Kafka, NATS).
- **Schedules** — driver runs the spec on cron/interval with a
  persistent `${CHECKPOINT}` cursor and boot-time catch-up. Owned by
  `Schedule` + `ScheduleStore` + `CheckpointStore`. Best for
  terminating sources with a natural WHERE-clause cursor.

You almost never want both on the same job. If you're tempted, ask:
"is my source finite or infinite?" and pick the mechanism that
matches.

## See also

- [`scheduling.md`](./scheduling.md) — the scheduler REST surface + template variables
- [`examples/mac-mail-hourly-time.yaml`](./examples/mac-mail-hourly-time.yaml) — terminating + time cursor
- [`examples/mac-mail-hourly-rowid.yaml`](./examples/mac-mail-hourly-rowid.yaml) — terminating + `ctx.setCheckpoint` row-id cursor
- [`examples/kafka-events-to-index.yaml`](./examples/kafka-events-to-index.yaml) — continuous + `restartable: true`
