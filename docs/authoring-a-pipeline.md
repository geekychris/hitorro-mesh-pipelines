# Authoring a pipeline

A hands-on walkthrough of writing a mesh pipeline from scratch — the
mental model, the smallest possible pipeline, and how to grow it into
something real. If you already know the model and just need reference,
skip to [`pipeline-framework.adoc`](./pipeline-framework.adoc).

## The mental model in three sentences

A **job** is a directed acyclic graph (DAG) of **nodes**. Each node
runs its own **pipeline**: pull rows from one **source**, run them
through a chain of **steps** (filter/project/enrich/…), then fan every
row out to one or more **sinks**. The runner topologically sorts the
DAG and hands each node its own JVM thread; edges between nodes
materialize as files, KV stores, memory tables, or streaming queues.

## Your smallest possible pipeline

```yaml
job: hello
nodes:
  - id: emit
    pipeline:
      source: { kind: inline, rows: [{msg: hi}, {msg: world}] }
      sinks:
        - { kind: counting, label: emitted }
```

Two rows, one counter. Save as `hello.yaml` and run:

```bash
./mesh-pipeline.sh run hello.yaml --wait
```

You'll get a `JobStatus` back with `emit.rowsOut=2` and
`sinks: { emitted: 2 }`. That's a real pipeline — it just doesn't do
anything useful.

## Adding steps

Steps transform rows one at a time and either return a new row, the
same row, or `null` to drop it (filter behavior). Chain them in
list order:

```yaml
job: hello-steps
nodes:
  - id: process
    pipeline:
      source: { kind: inline, rows: [
          {city: NYC, pop: 8000000},
          {city: SLC, pop: 200000},
          {city: LA,  pop: 4000000} ] }
      steps:
        - { kind: filter,  expr: "pop > 1000000" }
        - { kind: project, cols: [city, pop] }
        - { kind: set-field, name: is_metro, value: true }
      sinks:
        - { kind: ndjson-file, url: "target/metros.ndjson" }
```

Built-in step kinds (all zero-dependency, live in the core module):

| kind | purpose |
|---|---|
| `filter`      | `expr: "col op literal"` — drop rows that don't match |
| `project`     | `cols: [a, b]` — keep only the listed columns |
| `rename`      | `from: old, to: new` |
| `set-field`   | `name: k, value: v` — literal value |
| `to-typed`    | Coerce fields into JVS types (core_long, core_string, …) |
| `lookup`      | Enrich from another node's `memory-table` |
| `groovy-map`  | Arbitrary Groovy for the "when nothing else fits" case |

Groovy is the escape hatch — anything you can express as a script over
`row` (a `LinkedHashMap`), `gen` (uuid/now/sequence helpers), and
`ctx` (see [`scheduling.md`](./scheduling.md#advancing-the-checkpoint-from-the-job-itself)
for the write-back API) is legal.

## Multi-node: DAG with `depends` and `ref`

Fan a single upstream node's output into two independent downstream
nodes:

```yaml
job: fan-out
nodes:

  - id: seed
    pipeline:
      source: { kind: csv-file, url: "data/airports.csv" }
      steps:
        - { kind: filter, expr: "type == 'large_airport'" }
      sinks: []                 # relies on the auto-attached memory-table:seed

  - id: by-country
    depends: [ seed ]
    pipeline:
      source: { kind: ref, node: seed }
      reduce:
        group-by: [ iso_country ]
        aggs:
          - { name: n, kind: COUNT }
      sinks:
        - { kind: ndjson-file, url: "target/airports-by-country.ndjson" }

  - id: index
    depends: [ seed ]
    pipeline:
      source: { kind: ref, node: seed }
      sinks:
        - { kind: lucene, name: airports-idx, storeSource: true }
```

Every node automatically publishes its output stream as a
`memory-table:<nodeId>` — that's what `source: { kind: ref, node: seed }`
reads. You don't have to declare it. Downstream nodes with
`depends: [ … ]` run after the upstream drains. The runner assigns
Kahn ranks — nodes at the same rank run in parallel.

## Picking a source

| kind | when |
|---|---|
| `inline`       | Tests + demos — literal rows in the YAML |
| `ndjson-file`  | Persisted intermediate, transparent gzip/bzip |
| `csv-file`     | Header row + values; downstream steps cast types |
| `json-file`    | Top-level JSON array |
| `ref`          | Read an upstream node's materialized output |
| `sqlite`       | Read-only local SQLite DB (needs `-sqlite` module) |
| `sql`          | SQL query against a registered mesh table |
| `kvstore`      | Scan a RocksDB store (needs `-kvstore` module) |
| `lucene`       | Scan a Lucene index (needs `-lucene` module) |
| **`kafka`**    | Continuous consumer — see [pipeline-lifecycles.md](./pipeline-lifecycles.md) |
| **`nats`**     | Continuous subscription — same |

Sources ending in `kafka`/`nats` are **unbounded** — they never EOF.
A pipeline reading from one of them runs until cancelled. Read
[pipeline-lifecycles.md](./pipeline-lifecycles.md) before combining
one with a schedule (spoiler: don't).

## Picking a sink

| kind | when |
|---|---|
| `counting`      | Just count the rows — useful for smoke tests |
| `memory-table`  | Publish for a downstream `ref` source (auto-attached) |
| `ndjson-file`   | Persisted intermediate (optionally auto-registers as a mesh table) |
| `csv-file`      | Human-readable output |
| `json-file`     | Single-array JSON output |
| `kvstore`       | RocksDB with a `keyExpr` (e.g. `iso3` or `user.id`) |
| `lucene`        | Untyped Lucene index (StringField per column) |
| `jvs-lucene`    | Type-aware Lucene, uses a JVS type json to project fields |
| `kafka`         | Publish to a Kafka topic |
| `nats`          | Publish to a NATS subject |

Multiple sinks per node are fine — the row is delivered to every one.
Sinks' `close()` runs in a `finally` block; a slow or failing sink
doesn't block its neighbors.

## YAML or Groovy?

YAML for anything a config file can express. Groovy when you want
control flow, computed values, shared closures, or you're already
scripting a build:

```groovy
job('hello') {
    node('emit') {
        source inline: [[msg: 'hi'], [msg: 'world']]
        sink counting: 'emitted'
    }
}
```

POST to `/mesh/jobs/run-groovy` (rather than `/run`) so the parser
knows to use the Groovy front-end. Both produce the same `JobSpec` —
pick whichever fits the job.

## Running it

Three equally valid entry points:

```bash
# CLI wrapper (from hitorro-mesh-examples/scripts/)
./mesh-pipeline.sh run my-job.yaml --wait

# raw REST
curl -X POST http://localhost:8085/mesh/jobs/run \
     -H 'content-type: application/x-yaml' \
     --data-binary @my-job.yaml

# UI
# open http://localhost:8085/ → Pipelines tab → Build/Run
```

Follow it live via `--events` (SSE from `/mesh/jobs/{id}/events`) or
`GET /mesh/jobs/{id}` for a snapshot.

## Where pipelines fit next to *streaming SQL*

Pipelines and streaming SQL solve different problems, and it's worth
knowing which one to reach for:

| | Pipelines | Streaming SQL |
|---|---|---|
| **Purpose** | ETL — transform + materialize into a persistent shape (index, KV, file) | Ad-hoc live queries — "show me rows matching X as they arrive" |
| **Lifetime** | Batch runs to completion, or continuous with `restartable:true` | Per-query SSE stream, closed by the client |
| **Interface** | YAML/Groovy job spec, DAG of nodes | `SELECT` over a registered stream-backed table |
| **Best for** | Building an index once, ingesting hourly, joining streams onto disk | Dashboards, live tail, ad-hoc filtering of a firehose |
| **Endpoint** | `POST /mesh/jobs/run` | `GET /mesh/queries/stream?sql=…` (SSE) |

A common combined shape: a pipeline continuously ingests Kafka into a
Lucene index; a UI runs streaming SQL against the same Kafka topic
for a live tail. Same source, two different consumers.

Full streaming SQL walkthrough: [`streaming-sql.md`](./streaming-sql.md).

## Where to look next

- [`pipeline-lifecycles.md`](./pipeline-lifecycles.md) — terminating vs continuous; when to use `restartable:true`
- [`scheduling.md`](./scheduling.md) — running jobs on cron/interval with checkpoints
- [`streaming-sql.md`](./streaming-sql.md) — ad-hoc SSE queries over Kafka/NATS
- [`pipeline-framework.adoc`](./pipeline-framework.adoc) — internals: dispatchers, shuffle, agent capabilities
- [`examples/`](./examples/) — worked YAMLs for the mail + Kafka cases
- `hitorro-mesh-examples/scripts/` — `mesh-pipeline.sh`, `mesh-schedule.sh`, `mesh-stream-query.sh` wrappers
