# Durable pipeline scheduling

The scheduler runs mesh pipeline jobs on a cron/interval, remembers a
per-schedule **checkpoint** between runs, and catches up automatically
after the driver has been offline.  It is the answer to "I want the mail
ingest to run every hour, and if my laptop was asleep from 2pm to 5pm I
want the 3 missed hours to replay on wake."

> **When to use scheduling vs. continuous mode.** Scheduled pipelines
> are **terminating** — every source is finite (SQLite, NDJSON, HTTP
> `?since=`, IMAP `SEARCH SINCE`, …) and each run drains to completion.
> For **continuous** pipelines whose source is unbounded (Kafka, NATS),
> use `restartable: true` on the job spec instead and let the source's
> own offset store (consumer group / durable pull) handle resume. See
> [pipeline-lifecycles.md](./pipeline-lifecycles.md) for the full
> distinction.

Core plumbing lives in `hitorro-mesh-pipelines/schedule/`; Spring wiring
and the REST surface live in `hitorro-mesh-driver-app/schedule/`.

## Concepts

| Term | What it is |
|---|---|
| **Schedule** | A named binding of `{jobYaml, cron|intervalSeconds, checkpoint}` — the durable job definition. Persisted in `<HT_DATA>/schedules/schedules.json`. |
| **Checkpoint** | An opaque UTF-8 string owned by *your* job.  The scheduler substitutes it into your job template as `${CHECKPOINT}` — you decide the semantic (ISO instant, unix seconds, monotonic row id, IMAP UID, …). Stored per schedule in `<HT_DATA>/schedules/checkpoints/<name>.txt` so hourly advances don't rewrite the big schedules file. |
| **Catch-up** | On driver startup, if the last successful run's timestamp is older than one interval ago, one immediate catch-up run fires (bounded by `catchupGraceSeconds` so a schedule that has been off for weeks doesn't try to replay all of it in one go). |
| **maxConcurrent** | Hard cap on overlapping runs of the same schedule — protects sources that don't tolerate a second reader while the first is mid-batch. Default `1`. |

The checkpoint model is deliberately opaque so the same scheduler works
for time-based sources (Mac Mail's `date_received`), monotonic row ids
(SQL `WHERE ROWID > ${CHECKPOINT}`), IMAP UIDs, or Kafka-style byte
offsets — without the scheduler needing per-source knowledge.

## Template variables

Every `${…}` in your job YAML is substituted at run time:

| Token | Meaning |
|---|---|
| `${CHECKPOINT}` | Verbatim last-good value. Blank on first run. |
| `${CHECKPOINT_UNIX_SECONDS}` | Non-blank only when the checkpoint parses as an ISO-8601 instant.  Blank otherwise (use `COALESCE(NULLIF(...),0)` in SQL). |
| `${CHECKPOINT_UNIX_MILLIS}` | Same, in millis. |
| `${NOW}` | ISO instant of the current run's start. |
| `${NOW_UNIX_SECONDS}` / `${NOW_UNIX_MILLIS}` | Same, numeric. |
| `${SCHEDULE_NAME}` | The schedule's `name` field — handy for per-schedule output dirs. |

Unknown `${…}` tokens are left as literal text so a typo shows up in the
job log rather than silently emptying out.

## Persistence

Both files live under `<HT_DATA>/schedules/` (driver's home dir + `/schedules`):

```
schedules/
  schedules.json                 # array of Schedule POJOs, atomic move-with-replace
  checkpoints/
    mac-mail-hourly.txt          # one small file per schedule
    imap-inbox.txt
```

The schedule file is rewritten whole on every mutation (cheap — dozens
of schedules at most).  Checkpoints are per-file so hot per-run advances
don't churn the whole registry.  Both use write-tmp-then-atomic-rename.

## Failure semantics

- **Job throws / node fails** → checkpoint is **not** advanced; `lastFailureAt` + `lastError` recorded.  Next trigger tick retries with the same checkpoint (safe re-read).
- **Job succeeds** → checkpoint advances. Precedence:
  1. **Job-provided override** — a step called `ctx.setCheckpoint("...")` (see [Advancing the checkpoint from the job itself](#advancing-the-checkpoint-from-the-job-itself) below).
  2. **External `PUT /checkpoint`** mid-run — operator's value is preserved.
  3. **Default** — scheduler stamps `${NOW}` (ISO instant of run start).
- **Driver crashes mid-run** → checkpoint is untouched; on restart, the catch-up path kicks the schedule immediately.
- **Two runs asked to fire concurrently** → capped by `maxConcurrent`; overflow triggers are dropped with a debug log.

## Advancing the checkpoint from the job itself

When the natural cursor isn't a time (e.g. `WHERE ROWID > ?`), the
scheduler's default `${NOW}` advance doesn't help — only the job
knows the max id it consumed.  A `groovy-map` step (or any custom
step that receives the `ctx` binding) can write the value back:

```yaml
- kind: groovy-map
  script: |
    def cur = binding.hasVariable('maxSeen') ? maxSeen : 0L
    def rid = (row.rowid ?: 0L) as long
    if (rid > cur) {
        binding.setVariable('maxSeen', rid)
        ctx.setCheckpoint(String.valueOf(rid))
    }
    row
```

The last write in the run wins; on success, that value is persisted
as the next run's `${CHECKPOINT}`.  See
[`examples/mac-mail-hourly-rowid.yaml`](./examples/mac-mail-hourly-rowid.yaml)
for the full worked example.

## REST surface

Base path: `/mesh/schedules`.  Backed by `ScheduleController` in
`hitorro-mesh-driver-app`.

| Method | Path | Body / Effect |
|---|---|---|
| `GET`   | `/mesh/schedules` | List all schedules |
| `POST`  | `/mesh/schedules` | Create or replace (upsert by `name`) |
| `GET`   | `/mesh/schedules/{name}` | Fetch one |
| `DELETE`| `/mesh/schedules/{name}` | Delete and unschedule |
| `POST`  | `/mesh/schedules/{name}/pause` | Set `enabled=false`; unschedules the trigger |
| `POST`  | `/mesh/schedules/{name}/resume` | Set `enabled=true`; re-arms the trigger |
| `POST`  | `/mesh/schedules/{name}/run-now` | Trigger immediately (honours `maxConcurrent`) |
| `GET`   | `/mesh/schedules/{name}/checkpoint` | `{"checkpoint": "..."}` |
| `PUT`   | `/mesh/schedules/{name}/checkpoint` | Raw text body OR `{"checkpoint": "..."}` — external override |

## Shell CLI

`hitorro-mesh-examples/scripts/mesh-schedule.sh` wraps the REST surface:

```bash
# List
./mesh-schedule.sh list

# Create — hourly cron, 1-minute catch-up grace
./mesh-schedule.sh create mac-mail-hourly \
    ../docs/examples/mac-mail-hourly-schedule.yaml \
    --cron '0 0 * * * *' --catchup-grace 60s

# Or every-N-seconds
./mesh-schedule.sh create mac-mail-5min ../mail-yaml.yaml --every 300s

# Trigger immediately
./mesh-schedule.sh run-now mac-mail-hourly

# Pause / resume
./mesh-schedule.sh pause  mac-mail-hourly
./mesh-schedule.sh resume mac-mail-hourly

# Inspect / override the checkpoint
./mesh-schedule.sh checkpoint get mac-mail-hourly
./mesh-schedule.sh checkpoint set mac-mail-hourly '2026-08-01T00:00:00Z'

# Delete
./mesh-schedule.sh delete mac-mail-hourly
```

## Full worked example

See [`examples/mac-mail-hourly-schedule.yaml`](examples/mac-mail-hourly-schedule.yaml)
for an hourly delta ingest of the Mac Mail Envelope Index using
`${CHECKPOINT_UNIX_SECONDS}` in the SQL `WHERE` clause.

## When *not* to use this

- **Event-driven work** — if you have a webhook, a Kafka topic, or a filesystem watcher, wire that source into a long-running pipeline instead of polling on a schedule.
- **Sub-second cadences** — Spring's `TaskScheduler` is fine down to seconds but not milliseconds; drive tight loops from inside the pipeline itself.
- **Cross-driver coordination** — this scheduler is single-process.  For horizontally-scaled deployments where only one driver should own a given schedule, put a leader election layer in front (ZK, etcd, Postgres advisory lock).
