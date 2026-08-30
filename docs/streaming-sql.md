# Streaming SQL

Ad-hoc live SQL over Kafka/NATS topics — for dashboards, live tails,
and one-off "show me matching rows as they arrive" queries. Separate
mechanism from pipelines: pipelines materialize into indexes / KV /
files; streaming SQL just pushes rows to the client over SSE and forgets
them.

## When to reach for it

| Signal | Reach for |
|---|---|
| "I want to durably index every Kafka message" | **Pipeline** with `kind: kafka` source + `jvs-lucene` sink |
| "I want to build a rolling dashboard off a topic" | **Streaming SQL** |
| "I want to alert on live matches" | **Streaming SQL** consumed by a webhook forwarder |
| "I want to backfill an index" | **Pipeline** with `kind: sqlite`/`ndjson-file` source |
| "I want an operator to sanity-check the topic" | **Streaming SQL** — a curl one-liner |

Both models can coexist over the same source. A pipeline continuously
ingesting `kafka:events` into a JVS Lucene index doesn't block anyone
else registering the same topic as a streaming SQL table.

## Two endpoints

### 1. Register a stream-backed table

```
POST /mesh/queries/register-streaming
Content-Type: application/json

{
  "name":     "events",
  "format":   "kafka",
  "typeJson": "{ ... JVS type definition ... }",
  "sourceConfig": {
    "bootstrap-servers": "localhost:9092",
    "group-id":          "mesh-events-live",
    "topic":             "events"
  }
}
```

Response:

```json
{
  "tableName":      "events",
  "format":         "kafka",
  "sourceConfig":   { ... },
  "agentsNotified": 2
}
```

The driver broadcasts a `RegisterTable` message to every agent that
advertises the `jvssql` capability. Each agent subscribes to the topic
and lazily streams rows on demand when a query hits that table.

- `format`: `"kafka"` or `"nats"`. Kafka needs `bootstrap-servers`, `group-id`, `topic`. NATS needs `servers`, `subject`, and JetStream `stream` if you want durable pull semantics.
- `typeJson`: inline JVS type definition — the row schema. Same shape as `config/types/*.json`.
- `name`: the table name you'll `SELECT FROM` — must be unique across the mesh.

Registration is broadcast (not sharded) — every agent sees every row.
That's fine for cheap consumers; for shard-per-partition, see the
non-streaming `POST /mesh/queries/register-existing` path.

### 2. Stream a query as SSE

```
GET /mesh/queries/stream?sql=<url-encoded>&timeoutMs=300000
Accept: text/event-stream
```

Response is a Server-Sent-Events stream with three event types:

```
event: opened
data: {"queryId":"q-1788...","assignedAgents":["agent-us","agent-eu"]}

event: row
data: {"id":"e-42","level":"WARN","message":"disk 92%"}

event: row
data: {"id":"e-43","level":"INFO","message":"gc pause 12ms"}

event: complete
data: {"queryId":"q-1788...","rowCount":2}
```

Errors come through as `event: error` with `{"error":"..."}`.

`timeoutMs` is the *between-rows* idle timeout. For a truly infinite
tail (Kafka topic with a `WHERE`) set it high or let the client
manage the connection. The default is 5 minutes.

## Curl one-liners

Sanity-check a table exists and returns rows:

```bash
# `-N` disables curl's buffering — SSE needs unbuffered stdout
curl -N 'http://localhost:8085/mesh/queries/stream?sql=SELECT+id,level+FROM+events+WHERE+level+%3D+%27WARN%27'
```

Register a Kafka topic first:

```bash
TYPE_JSON=$(python3 -c "import json; print(json.dumps({
  'name':'events','core':[{'field':'id','type':'core_string'},
                          {'field':'level','type':'core_string'},
                          {'field':'message','type':'core_string'}]}))")
curl -X POST http://localhost:8085/mesh/queries/register-streaming \
     -H 'content-type: application/json' \
     -d "$(python3 -c "import json,sys; print(json.dumps({
       'name':'events','format':'kafka','typeJson':'$TYPE_JSON',
       'sourceConfig':{
         'bootstrap-servers':'localhost:9092',
         'group-id':'mesh-events-live',
         'topic':'events'}}))")"
```

Or use the shell wrapper (see [`mesh-stream-query.sh`](../../../hitorro-mesh-examples/scripts/mesh-stream-query.sh)):

```bash
./mesh-stream-query.sh "SELECT id, level FROM events WHERE level='WARN'"
```

## JavaScript / browser

`EventSource` in every modern browser handles SSE natively:

```javascript
const es = new EventSource(
  '/mesh/queries/stream?sql=' + encodeURIComponent(
    "SELECT id, level, message FROM events WHERE level IN ('WARN','ERROR')"
  ));

es.addEventListener('opened',   e => console.log('opened',   JSON.parse(e.data)));
es.addEventListener('row',      e => appendToTable(JSON.parse(e.data)));
es.addEventListener('complete', e => { console.log('done', JSON.parse(e.data)); es.close(); });
es.addEventListener('error',    e => { console.error('err', e.data); es.close(); });
```

The driver's UI Streaming tab is exactly this pattern — see
`hitorro-mesh-driver-app/src/main/resources/static/ui/app.js` for a
production version with pause/resume + row buffering.

## When to combine with pipelines

The clean pattern:

- **Pipeline** does the durable, exactly-once path: `kind: kafka` source
  → `jvs-lucene` sink with `restartable: true`. Indexes every message
  once, resumes via Kafka consumer group after driver restart.
- **Streaming SQL** is the live-view path: registered against the SAME
  topic with a DIFFERENT `group-id`, so the two consumers don't
  compete for offsets. Serves dashboards + ad-hoc queries.
- **Batch SQL** (`POST /mesh/queries`) queries the durable index once
  it's populated. Same SQL syntax as streaming, different endpoint.

Same JVS type json in all three — write it once, use it everywhere.

## What it isn't

- Not an OLAP engine. Aggregates (`GROUP BY`, `SUM`) work but hold
  state in memory. For "count events per level over the last hour"
  you probably want a pipeline that writes to a `kvstore` with a
  time-bucketed key.
- Not exactly-once. SSE clients that disconnect miss rows arriving
  while they're gone. Kafka's own consumer-group offset is not
  advanced by SSE consumers — every reconnect starts from the
  current live position for that consumer group.
- Not push-to-many. Each SSE client is one consumer; N dashboards
  = N Kafka consumer group members (they share the offset).

## Reference

- Endpoint code: [`MeshRestController.stream()`](../../hitorro-mesh-driver-app/src/main/java/com/hitorro/mesh/driver/app/MeshRestController.java) — line ~317
- Registration: [`QueryWriteController.registerStreaming()`](../../hitorro-mesh-driver-app/src/main/java/com/hitorro/mesh/driver/app/query/QueryWriteController.java) — line ~272
- Shell wrapper: [`mesh-stream-query.sh`](../../../hitorro-mesh-examples/scripts/mesh-stream-query.sh)
- Related: [`authoring-a-pipeline.md`](./authoring-a-pipeline.md), [`pipeline-lifecycles.md`](./pipeline-lifecycles.md)
