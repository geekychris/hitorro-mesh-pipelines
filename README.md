# hitorro-mesh-pipelines

DAG pipeline framework for the [Hitorro Mesh](https://github.com/hitorro/hitorro-mesh).
A job is a directed graph of nodes; each node is internally a pipeline
(source → steps → optional reduce → one-or-more sinks); edges materialise
as files, key-value stores, indices, or streaming queues.

Phase 1 executes the whole DAG on the driver JVM, reusing Jackson + Java
standard library. Phases 2–4 add mesh distribution, streaming edges, and
long-running jobs.

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

- **Phase 1 (this release).** Driver-local execution: topological sort +
  per-rank parallelism. All sinks fully implemented as file/in-memory
  stubs. KV / Lucene write stubs (TSV / NDJSON respectively) so tests
  assert the physical output shape end-to-end.
- **Phase 2.** Concrete adapter modules land real `RocksDBStore` and
  `JVSLuceneIndexWriter` writers behind the same `SinkSpec` names.
  `TaskDescriptor` gets a nullable `pipelineSpec` field and `TaskExecutor`
  gets a fourth branch — pipelines dispatch to agents by capability.
- **Phase 3.** NATS / Kafka sinks + sources become streaming edges;
  watermarks flow through pipeline nodes.
- **Phase 4.** Sink lifecycle (create / pause / drain / drop),
  checkpointing via RocksDB WAL, exactly-once sink writes by sequence.

## REST surface

Driver exposes these when the module is on the classpath (via Spring Boot
autoconfig):

| Method | Path                                  | Purpose                                           |
|--------|---------------------------------------|---------------------------------------------------|
| POST   | `/mesh/jobs/run`                      | Body is YAML or JSON job spec, kicks off async.   |
| POST   | `/mesh/jobs/run/bundled/{name}`       | Run a bundled example.                            |
| GET    | `/mesh/jobs`                          | List past + running runs.                         |
| GET    | `/mesh/jobs/{jobId}`                  | Snapshot for one run.                             |
| GET    | `/mesh/jobs/{jobId}/events`           | Progress events for that run.                     |
| GET    | `/mesh/jobs/bundled`                  | id → YAML text for every bundled example.         |

## Testing

```bash
mvn clean install
```

Runs the parser + runtime tests including the three-node bundled example.

## License

Apache 2.0.
