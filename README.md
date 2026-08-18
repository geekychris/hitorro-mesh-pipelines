# hitorro-mesh-pipelines

DAG pipeline framework for the [Hitorro Mesh](https://github.com/hitorro/hitorro-mesh).
A job is a directed graph of nodes; each node is internally a pipeline
(source → steps → optional reduce → one-or-more sinks); edges materialise
as files, key-value stores, indices, or streaming queues.

Phase 1 (driver-local) and Phase 2 (mesh distribution) are both landed —
`PipelineScheduler` fans nodes across agents that advertise the
`pipeline-node` capability via `hitorro-mesh-agent-pipelines`, and the
`-kvstore` / `-lucene` / `-jvstype` adapter modules ship real backends
via ServiceLoader. Phase 3 (streaming edges) and Phase 4 (backpressure /
exactly-once) are the remaining follow-ups.

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
  sinks work today for pub/sub semantics; watermark propagation +
  long-running named jobs with restart are the remaining pieces.
- **Phase 4 — backpressure + exactly-once.** Not started. Sink
  lifecycle (create / pause / drain / drop), checkpointing via
  RocksDB WAL, idempotent sink writes by sequence.

## REST surface

Driver exposes these when the module is on the classpath (via Spring Boot
autoconfig):

| Method | Path                                  | Purpose                                           |
|--------|---------------------------------------|---------------------------------------------------|
| POST   | `/mesh/jobs/run`                      | Body is YAML or JSON job spec, kicks off async (driver-local execution). |
| POST   | `/mesh/jobs/run-distributed`          | Same body, dispatches nodes across agents with the `pipeline-node` capability. |
| POST   | `/mesh/jobs/run/bundled/{name}`       | Run a bundled example.                            |
| GET    | `/mesh/jobs`                          | List past + running runs.                         |
| GET    | `/mesh/jobs/{jobId}`                  | Snapshot for one run — includes per-node `deps` + `rank` so the UI can render topology. |
| GET    | `/mesh/jobs/{jobId}/events`           | Progress events for that run.                     |
| GET    | `/mesh/jobs/bundled`                  | id → YAML text for every bundled example.         |

The driver-app also ships a Jobs UI tab that renders each running job
as a topological DAG (columns per Kahn rank, cubic-bezier SVG arrows
between deps, edge colour tracking upstream state as the wave of
"done" cascades through the graph).

## Testing

```bash
mvn clean install
```

Runs the parser + runtime + reduce + hash-routing + DAG-shape suite
(currently 42 tests across `JobSpecYamlTest`, `JobRunnerLocalTest`,
`DagShapeTest`, `ReduceEngineTest`, `ShuffleFanoutHashTest`,
`GroovyMapStepTest`, `SinkRegistryDecoratorTest`, `CsvJsonSinkIntegrationTest`).
Real KV / Lucene / JVS-DSL round-trip coverage lives in the sibling
adapter modules (`-kvstore`, `-lucene`, `-jvstype`) plus agent-side
envelope coverage in `-agent-pipelines`.

## License

Apache 2.0.
