# hitorro-mesh-pipelines

DAG pipeline framework for the [Hitorro Mesh](https://github.com/hitorro/hitorro-mesh).
A job is a directed graph of nodes; each node is internally a pipeline
(source → steps → optional reduce → one-or-more sinks); edges materialise
as files, key-value stores, indices, or streaming queues.

Phase 1 (driver-local) and Phase 2 (mesh distribution) are both landed —
`PipelineScheduler` fans nodes across agents that advertise the
`pipeline-node` capability via `hitorro-mesh-agent-pipelines`, and the
`-kvstore` / `-lucene` / `-jvstype` adapter modules ship real backends
via ServiceLoader. Phase 3 (long-running jobs) is partial — restartable
jobs persist across driver restarts, and streaming source/sinks
(NATS + Kafka) work for pub/sub. Phase 4 (full exactly-once + sink
lifecycle) is partial — `KvStoreSink` and `LuceneSink` implement the
`addIdempotent` contract via native dedup semantics.

## Quick example

```yaml
job: countries-triple-sink
version: "1"
nodes:
  - id: seed
    pipeline:
      source: {kind: inline, rows: [{iso3: USA, name: United States, population: 331000000}, …]}
      steps:
        - {kind: filter, expr: "population > 50000000"}
        - {kind: project, cols: [iso3, name, region, population]}
      sinks:
        - {kind: memory-table, name: big-countries}
        - {kind: counting, label: seed}

  - id: rollup
    depends: [seed]
    pipeline:
      source: {kind: ref, node: seed}
      reduce:
        group-by: [region]
        aggs:
          - {name: n, kind: COUNT}
          - {name: total_pop, kind: SUM, of: population}
      sinks: [{kind: ndjson-file, url: "target/rollup.ndjson"}]

  - id: index
    depends: [seed]
    pipeline:
      source: {kind: ref, node: seed}
      sinks:
        - {kind: kvstore, name: countries-kv, keyExpr: "iso3"}
        - {kind: lucene,  name: countries-idx, storeSource: true}
```

Sends 15 rows through `seed`, fans out to two independent downstream nodes,
lands physical outputs under `~/.hitorro/pipelines/{kv,lucene}/…`.

### Same job in Groovy

```groovy
job('countries-triple-sink') {
    node('seed') {
        source inline: [[iso3: 'USA', name: 'United States', population: 331_000_000]]
        step filter: 'population > 50000000'
        step project: ['iso3', 'name', 'region', 'population']
        sink memoryTable: 'big-countries'
        sink counting: 'seed'
    }
    node('rollup', depends: ['seed']) {
        source ref: 'seed'
        reduce groupBy: ['region'], aggs: [
            [name: 'n',         kind: 'COUNT'],
            [name: 'total_pop', kind: 'SUM', of: 'population']
        ]
        sink ndjson: 'target/rollup.ndjson'
    }
    node('index', depends: ['seed']) {
        source ref: 'seed'
        sink kvstore: 'countries-kv', keyExpr: 'iso3'
        sink lucene:  'countries-idx'
    }
}
```

The Groovy front-end reaches the same `JobSpec` output as YAML — pick
it when you want control flow, computed values, or shared closures.
Groovy jobs POST to `/mesh/jobs/run-groovy` (rather than `/run`) so
the parser knows to use `JobSpecGroovy` instead of YAML.

### Long-running / restartable jobs

Streaming pipelines can opt into persistent registration:

```yaml
job: nats-ingest
restartable: true    # driver persists spec to disk; resumes on restart
nodes:
  - id: consume
    pipeline:
      source: {kind: nats, subject: "events.>"}
      sink: [{kind: kvstore, name: raw-events, keyExpr: "id"}]
```

Or equivalent Groovy:

```groovy
job('nats-ingest') {
    restartable true
    node('consume') {
        source nats: 'events.>'
        sink kvstore: 'raw-events', keyExpr: 'id'
    }
}
```

On driver restart, `PipelinesBootResumer` reads pending entries from
`~/.hitorro/pipelines/restartable-jobs.json` and re-submits each
through the runner (original jobId preserved). Not a checkpoint — the
resumed job re-processes from the source's beginning, so:

- `KvStoreSink` (RocksDB, dedup by taskId+seq via `addIdempotent`) and
  `LuceneSink` (dedup by hidden `_taskseq` StringField + updateDocument
  by term) stay safe on retry
- Other sinks may see duplicates — document explicitly on user-facing
  jobs

## Model

```
Job
├── Node                 unit of computation
│   ├── PartitionSpec    "run me N times, one per key" (optional)
│   ├── Pipeline         source → steps → optional reduce → sink(s)
│   │   ├── Source       ndjson-file | json-file | csv-file | inline | ref | kvstore | lucene | sql
│   │   ├── Step[]       filter | project | rename | set-field | groovy-map | jvssql
│   │   ├── Reduce       group-by + aggs (count / sum / avg / min / max / distinct / first / last / collect)
│   │   └── Sink[]       ndjson-file | csv-file | json-file | kvstore | lucene | nats | kafka | counting | memory-table
│   └── depends: [ids…]  upstream nodes that must EOS before this starts
```

Every DAG node auto-attaches a `memory-table:<nodeId>` sink so downstream
nodes reading `source: {kind: ref, node: <id>}` work with zero extra config.

## Runtime, phased

- **Phase 1 — driver-local execution.** ✅ Shipped. Topological sort +
  per-rank parallelism via `JobRunner`. Every source/step/sink
  factory-built from spec through the built-in registry.
- **Phase 2 — mesh distribution.** ✅ Shipped. `PipelineScheduler`
  discovers agents that advertise the `pipeline-node` capability
  (via `hitorro-mesh-agent-pipelines`), dispatches each node over
  NATS as a `TaskDescriptor.pipelineNodeSpec` envelope, and
  accumulates results on a per-task subject. Auto-splits any node
  with `reduce.shuffle=true` into M mappers × K reducers connected
  via NATS shuffle-bucket subjects — raw-row shuffle so every
  AggKind (COUNT, SUM, AVG, MIN, MAX, DISTINCT_COUNT, FIRST, LAST,
  COLLECT) stays correct. Real `RocksDBStore` / `JVSLuceneIndexWriter`
  writers land through the `-kvstore` / `-lucene` / `-jvstype`
  adapter modules via `ServiceLoader`. Locality-aware placement
  co-locates ref-source nodes with their upstream.
- **Phase 3 — streaming edges.** Partial. NATS + Kafka sources and
  sinks work for pub/sub semantics. Restartable jobs (`restartable:
  true` on the JobSpec) persist to `~/.hitorro/pipelines/restartable-jobs.json`
  on submit + resume on driver restart via `PipelinesBootResumer`.
  Watermark propagation and mid-stream checkpointing remain.
- **Phase 4 — backpressure + exactly-once.** Partial. Two durable
  sinks (`KvStoreSink` + `LuceneSink`) implement the exactly-once
  contract via `Sink.addIdempotent(taskId, seq, row)` — retried
  mappers don't produce duplicate output. Other sinks (`NdjsonFileSink`,
  `NatsSink`, `KafkaSink`) fall back to at-least-once. Full sink
  lifecycle (create / pause / drain / drop) + backpressure remain.

## REST surface

Driver exposes these when the module is on the classpath (via Spring Boot
autoconfig):

| Method | Path                                  | Purpose                                           |
|--------|---------------------------------------|---------------------------------------------------|
| POST   | `/mesh/jobs/run`                      | Body is YAML or JSON job spec, kicks off async (driver-local execution). |
| POST   | `/mesh/jobs/run-groovy`               | Body is a Groovy DSL script (see `JobSpecGroovy`). |
| POST   | `/mesh/jobs/run-distributed`          | Same body, dispatches nodes across agents with the `pipeline-node` capability. |
| POST   | `/mesh/jobs/run/bundled/{name}`       | Run a bundled example.                            |
| GET    | `/mesh/jobs`                          | List past + running runs.                         |
| GET    | `/mesh/jobs/{jobId}`                  | Snapshot for one run — per-node `deps` + `rank` for UI topology; `restartable` flag for the badge. |
| GET    | `/mesh/jobs/{jobId}/events`           | Progress events for that run.                     |
| GET    | `/mesh/jobs/bundled`                  | id → YAML text for every bundled example.         |
| GET    | `/mesh/jobs/history`                  | Persistent job history (survives driver restarts). |
| GET    | `/mesh/jobs/sink-locations`           | Which agent holds each named persistent sink.     |
| DELETE | `/mesh/jobs/{jobId}`                  | Cooperative cancel — sets the cancelRequested flag. |

The driver-app also ships a Jobs UI tab that renders each running job
as a topological DAG (columns per Kahn rank, cubic-bezier SVG arrows
between deps, edge colour tracking upstream state as the wave of
"done" cascades through the graph). Restartable jobs get a
`↻ restartable` badge in the live header + history cards.

## Testing

```bash
mvn clean install
```

Runs the full pipelines suite (currently 170 tests): parsers (YAML +
Groovy DSL), runtime (topo sort + reduce + hash routing + step
factory + source factory), persistence (JobRegistry ring buffer +
history + restartable store), REST controller, DAG shape, and sink
decorator wiring.

Real KV / Lucene / JVS-DSL round-trip coverage lives in the sibling
adapter modules (`-kvstore`, `-lucene`, `-jvstype`) plus agent-side
envelope coverage in `-agent-pipelines`. Cross-module suite counts:
`hitorro-mesh-pipelines` 170, `-lucene` 18, `-agent-pipelines` 9,
`-jvstype` 7, `-kvstore` 1 (round-trip only). The exactly-once
contract for `KvStoreSink` is in `hitorro-kvstore/KvStoreSinkTest`
(13 tests) and for `LuceneSink` in `-lucene/LuceneSinkIdempotentTest`
(5 tests).

## License

Apache 2.0.
