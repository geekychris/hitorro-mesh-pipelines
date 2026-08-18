/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.AggSpec;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.model.PipelineSpec;
import com.hitorro.mesh.pipelines.model.ReduceSpec;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fans a job's partition-instances across agents that advertise the
 * {@code pipeline-node} capability. Uses NATS as the transport
 * (mesh.agent.pipeline.&lt;agentId&gt;), decoupled from the SQL task
 * dispatcher so mesh-agent-app stays untouched.
 *
 * <p>Scope of this class: round-robin across matching agents, one node
 * per envelope. Reduce / cross-agent shuffle stays local for now — the
 * fan-out is at the node level, not the row level. Extended fan-out
 * (per-partition dispatch, shuffle across agents) is a future slice.</p>
 *
 * <p>Agent discovery is done via HTTP against the driver's
 * {@code /mesh/agents} endpoint — avoids depending on the mesh Driver
 * class directly (this module stays free of mesh internals).</p>
 */
public final class PipelineScheduler implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final AtomicLong TASK_SEQ = new AtomicLong();

    private static final int MAX_RETRIES = 2;

    private final String natsUrl;
    private final String driverBaseUrl;
    private final HttpClient http;
    private final Object nats;         // io.nats.client.Connection when jnats present
    private final Object dispatcher;   // io.nats.client.Dispatcher when jnats present
    private final Class<?> connIface;      // io.nats.client.Connection (interface — public methods reachable)
    private final Class<?> dispIface;      // io.nats.client.Dispatcher (interface)
    private final java.util.Map<String, ResultAccumulator> outstanding = new java.util.concurrent.ConcurrentHashMap<>();
    private final SinkLocationRegistry sinkLocations;
    /** Rolling counter of tasks-per-agent for round-robin fallback. */
    private final java.util.Map<String, java.util.concurrent.atomic.AtomicLong> agentTaskCounts
            = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * @param natsUrl        NATS server URL (usually {@code nats://localhost:4222})
     * @param driverBaseUrl  where the driver's REST endpoints live
     */
    public PipelineScheduler(String natsUrl, String driverBaseUrl) throws Exception {
        this(natsUrl, driverBaseUrl, new SinkLocationRegistry());
    }

    public PipelineScheduler(String natsUrl, String driverBaseUrl,
                             SinkLocationRegistry sinkLocations) throws Exception {
        this.natsUrl = natsUrl;
        this.driverBaseUrl = driverBaseUrl;
        this.sinkLocations = sinkLocations;
        this.http = HttpClient.newHttpClient();
        try {
            // Interface lookups so reflective method calls resolve against
            // the public jnats API (NatsConnectionImpl is package-private
            // and its concrete class rejects getMethod queries at runtime).
            this.connIface = Class.forName("io.nats.client.Connection");
            this.dispIface = Class.forName("io.nats.client.Dispatcher");
            Class<?> natsClass = Class.forName("io.nats.client.Nats");
            this.nats = natsClass.getMethod("connect", String.class).invoke(null, natsUrl);
            this.dispatcher = null;
        } catch (Throwable e) {
            throw new IllegalStateException(
                    "PipelineScheduler needs io.nats:jnats on the classpath", e);
        }
    }

    /**
     * Dispatch each node in the job to an available agent (round-robin),
     * wait for their {@code eos} envelopes, aggregate row counts into a
     * live JobStatus. Blocks until every dispatched node terminates or
     * {@link JobStatus#cancelRequested} fires.
     */
    public JobStatus dispatch(JobSpec spec, JobStatus status,
                              List<String> targetAgents) throws Exception {
        status.state = JobStatus.State.RUNNING;
        try {
            if (targetAgents == null || targetAgents.isEmpty()) {
                targetAgents = discoverAgentsWith("pipeline-node");
                if (targetAgents.isEmpty()) {
                    throw new IllegalStateException(
                            "no agents advertise the pipeline-node capability — add "
                                    + "hitorro-mesh-agent-pipelines to at least one agent "
                                    + "and set hitorro.mesh.pipelines.enabled=true");
                }
            }
            // Track per-node placement so downstream ref-source nodes can
            // co-locate with their upstream, and record persistent-sink
            // writes into the registry so future jobs benefit too.
            java.util.Map<String, String> nodeToAgent = new java.util.HashMap<>();
            for (NodeSpec node : spec.nodes()) {
                if (status.cancelRequested.get()) break;

                // Auto-shuffle-reduce: if node has reduce.shuffle=true,
                // split into mapper × M + reducer × K and dispatch each.
                if (node.pipeline().reduce() != null
                        && node.pipeline().reduce().shuffle()) {
                    dispatchShuffleReduce(node, targetAgents, status, nodeToAgent);
                    continue;
                }

                String preferred = pickAgent(node, targetAgents, nodeToAgent);
                java.util.Set<String> blacklist = new java.util.HashSet<>();
                boolean ok = false;
                Throwable lastErr = null;
                for (int attempt = 0; attempt <= MAX_RETRIES && !ok; attempt++) {
                    String candidate = attempt == 0 ? preferred
                            : pickAlternate(preferred, targetAgents, blacklist);
                    if (candidate == null) break;
                    try {
                        dispatchOne(node, candidate, status);
                        ok = status.node(node.id()).state == JobStatus.State.SUCCEEDED;
                        if (ok) {
                            nodeToAgent.put(node.id(), candidate);
                            agentTaskCounts.computeIfAbsent(candidate,
                                    k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
                            recordSinkWrites(node, candidate);
                        } else {
                            blacklist.add(candidate);
                            lastErr = new RuntimeException("agent " + candidate + " reported "
                                    + status.node(node.id()).state);
                            status.addEvent(new JobStatus.ProgressEvent(node.id(), "retry",
                                    "attempt " + (attempt + 1) + " on " + candidate + " failed; retrying",
                                    Instant.now()));
                        }
                    } catch (Exception ex) {
                        blacklist.add(candidate);
                        lastErr = ex;
                        status.addEvent(new JobStatus.ProgressEvent(node.id(), "retry",
                                "attempt " + (attempt + 1) + " on " + candidate + " threw " + ex.getMessage(),
                                Instant.now()));
                    }
                }
                if (!ok) {
                    status.node(node.id()).state = JobStatus.State.FAILED;
                    status.node(node.id()).error = lastErr == null
                            ? "no matching agent could run node " + node.id()
                            : "all retries exhausted; last error: " + lastErr.getMessage();
                    throw new RuntimeException("node " + node.id() + " failed after retries");
                }
            }
            status.state = status.cancelRequested.get()
                    ? JobStatus.State.CANCELLED
                    : JobStatus.State.SUCCEEDED;
        } catch (Throwable t) {
            status.state = JobStatus.State.FAILED;
            status.error = t.getClass().getSimpleName() + ": " + t.getMessage();
        } finally {
            status.finishedAt = Instant.now();
        }
        return status;
    }

    /**
     * Auto-split a reduce node into M mapper tasks + K reducer tasks
     * connected via NATS shuffle bucket subjects.
     *
     * <p>Mapper (one per available agent):</p>
     * <pre>
     *   source: (original)          steps: (original)
     *   sinks: [ ShuffleFanout(subjectPrefix=mesh.pipeline.shuffle.&lt;taskId&gt;,
     *                          buckets=K, keyExpr=first group-by,
     *                          mapperId=m0..M-1) ]
     * </pre>
     *
     * <p>Reducer (one per bucket, spread across agents):</p>
     * <pre>
     *   source: ShuffleBucket(subjectPrefix, bucket=0..K-1, expectedMappers=M)
     *   reduce: (rewritten aggs — COUNT becomes SUM of partials)
     *   sinks:  (original)
     * </pre>
     *
     * <p>Row semantics: mappers publish RAW rows to the shuffle
     * subjects (via {@link SinkSpec.ShuffleFanout}) — no mapper-side
     * pre-aggregation. Reducers receive every raw row hashed into
     * their bucket and run the ORIGINAL aggs unchanged. This keeps
     * every AggKind — including AVG, DISTINCT_COUNT, FIRST/LAST,
     * COLLECT — semantically identical to the single-agent path,
     * at the cost of higher network traffic than a two-phase
     * pre-agg + combine would need.</p>
     *
     * <p>The {@link #rewriteForShuffle} step exists as a hook for a
     * future two-phase decomposition (mapper: partial SUM+COUNT →
     * reducer: divide) — today it only disables further shuffling
     * on the reducer's own reduce (shuffle=false, buckets=1) so we
     * don't recursively split.</p>
     */
    private void dispatchShuffleReduce(NodeSpec original, List<String> targetAgents,
                                       JobStatus status,
                                       java.util.Map<String, String> nodeToAgent) throws Exception {
        ReduceSpec r = original.pipeline().reduce();
        int buckets  = Math.max(1, r.buckets());
        int mappers  = Math.max(1, targetAgents.size());
        String taskId = "shuf-" + TASK_SEQ.incrementAndGet();
        String subjectPrefix = "mesh.pipeline.shuffle." + taskId;
        String keyExpr = r.groupBy().isEmpty() ? "id" : r.groupBy().get(0);

        status.addEvent(new JobStatus.ProgressEvent(original.id(), "shuffle-plan",
                "split into " + mappers + " mapper(s) × " + buckets + " reducer(s)",
                Instant.now()));

        // Reducers FIRST — they need to subscribe to their bucket subjects
        // before mappers publish, otherwise core NATS drops messages sent
        // when no subscriber exists yet.
        java.util.List<Thread> reducerThreads = new java.util.ArrayList<>();
        for (int b = 0; b < buckets; b++) {
            String assigned = targetAgents.get(b % targetAgents.size());
            NodeSpec reducer = new NodeSpec(original.id() + "-reduce-" + b, null,
                    new PipelineSpec(
                            new SourceSpec.ShuffleBucket(subjectPrefix, b, mappers, natsUrl),
                            java.util.List.of(),
                            rewriteForShuffle(r),
                            original.pipeline().sinks()
                    ), java.util.List.of());
            Thread t = new Thread(() -> {
                try { dispatchOne(reducer, assigned, status); }
                catch (Exception e) { status.addEvent(new JobStatus.ProgressEvent(
                        reducer.id(), "error", e.getMessage(), Instant.now())); }
            }, "shuffle-reducer-" + b);
            t.start();
            reducerThreads.add(t);
        }
        // Give reducers time to reach their assigned agent AND establish
        // their NATS subscriptions before mappers publish. The round trip
        // is: driver → NATS → agent → PipelineAgent handler → JobRunner
        // startup → SourceFactory.open → ShuffleBucketSource → NATS
        // subscribe. 2 s is generous for a single-host smoke stack; a
        // real deployment should use JetStream (persistent subjects) or
        // an explicit ready-handshake instead of a sleep.
        try { Thread.sleep(2000); } catch (InterruptedException ignored) { }

        // Then mappers: same source + steps as original, sink to shuffle
        // bucket subjects. No reduce at mapper — reducer does all agg.
        java.util.List<Thread> mapperThreads = new java.util.ArrayList<>();
        for (int mi = 0; mi < mappers; mi++) {
            String mapperId = "m" + mi;
            String assigned = targetAgents.get(mi % targetAgents.size());
            NodeSpec mapper = new NodeSpec(original.id() + "-map-" + mi, null,
                    new PipelineSpec(
                            original.pipeline().source(),
                            original.pipeline().steps(),
                            null,
                            java.util.List.of(new SinkSpec.ShuffleFanout(
                                    subjectPrefix, buckets, keyExpr, mapperId, natsUrl))
                    ), java.util.List.of());
            Thread t = new Thread(() -> {
                try { dispatchOne(mapper, assigned, status); }
                catch (Exception e) { status.addEvent(new JobStatus.ProgressEvent(
                        mapper.id(), "error", e.getMessage(), Instant.now())); }
            }, "shuffle-mapper-" + mapperId);
            t.start();
            mapperThreads.add(t);
        }

        // Wait for everyone.
        for (Thread t : mapperThreads)  t.join();
        for (Thread t : reducerThreads) t.join();

        // Aggregate synthetic status onto the original node's status.
        JobStatus.NodeStatus origNs = status.node(original.id());
        long totalRowsOut = 0;
        boolean allGood = true;
        for (int mi = 0; mi < mappers; mi++) {
            var s = status.node(original.id() + "-map-" + mi);
            allGood &= s.state == JobStatus.State.SUCCEEDED;
            totalRowsOut += s.rowsOut;
        }
        long reducerOut = 0;
        for (int bi = 0; bi < buckets; bi++) {
            var s = status.node(original.id() + "-reduce-" + bi);
            allGood &= s.state == JobStatus.State.SUCCEEDED;
            reducerOut += s.rowsOut;
        }
        origNs.state = allGood ? JobStatus.State.SUCCEEDED : JobStatus.State.FAILED;
        origNs.rowsIn = totalRowsOut;
        origNs.rowsOut = reducerOut;
        origNs.assignedAgent = "shuffle(" + mappers + "×M + " + buckets + "×R)";
        origNs.finishedAt = Instant.now();
    }

    /**
     * Prepare the reducer's ReduceSpec. Aggs are unchanged (the reducer
     * sees raw rows), but shuffle is turned OFF and buckets forced to 1
     * so a downstream {@link #dispatchShuffleReduce} doesn't recursively
     * split the reducer's own reduce.
     *
     * <p>Landing spot for a future two-phase decomposition: rewrite each
     * AVG here into (SUM, COUNT) with a paired sum/count agg at the
     * reducer, then divide in a post-step. Until that lands, this is a
     * one-line pass-through that just disables the shuffle flag.</p>
     */
    private ReduceSpec rewriteForShuffle(ReduceSpec r) {
        return new ReduceSpec(r.groupBy(), r.aggs(), false, 1);
    }

    /**
     * Locality-aware placement:
     * <ol>
     *   <li>If source is {@code ref} to an already-placed upstream node,
     *       run on the same agent (no network hop for the row stream).</li>
     *   <li>If source is a named persistent sink ({@code kvstore},
     *       {@code lucene}), run on the agent that has that sink on-disk.</li>
     *   <li>Fallback: pick the agent with the fewest tasks assigned so far
     *       this session (least-loaded round-robin).</li>
     * </ol>
     */
    private String pickAgent(NodeSpec node, List<String> targetAgents,
                             java.util.Map<String, String> nodeToAgent) {
        SourceSpec s = node.pipeline().source();
        String local = null;
        if (s instanceof SourceSpec.Ref r)      local = nodeToAgent.get(r.node());
        else if (s instanceof SourceSpec.KvStore k) local = sinkLocations.locate("kvstore", k.name());
        else if (s instanceof SourceSpec.Lucene l)  local = sinkLocations.locate("lucene",  l.name());
        if (local != null && targetAgents.contains(local)) return local;
        return leastLoaded(targetAgents);
    }

    /** Pick a not-yet-blacklisted alternative agent for a retry. */
    private String pickAlternate(String previous, List<String> candidates, java.util.Set<String> blacklist) {
        for (String a : candidates) {
            if (!blacklist.contains(a) && !a.equals(previous)) return a;
        }
        // If everyone's blacklisted or only one candidate exists, allow the
        // previous one on the assumption it might be transient — one more try.
        for (String a : candidates) if (!blacklist.contains(a)) return a;
        return null;
    }

    private String leastLoaded(List<String> agents) {
        String best = agents.get(0);
        long bestCount = Long.MAX_VALUE;
        for (String a : agents) {
            long c = agentTaskCounts.getOrDefault(a,
                    new java.util.concurrent.atomic.AtomicLong()).get();
            if (c < bestCount) { bestCount = c; best = a; }
        }
        return best;
    }

    /** Remember which agent now holds each persistent sink this node wrote. */
    private void recordSinkWrites(NodeSpec node, String agentId) {
        for (SinkSpec ss : node.pipeline().sinks()) {
            if      (ss instanceof SinkSpec.KvStore k) sinkLocations.record("kvstore", k.name(), agentId);
            else if (ss instanceof SinkSpec.Lucene  l) sinkLocations.record("lucene",  l.name(), agentId);
            else if (ss instanceof SinkSpec.MemoryTable m) sinkLocations.record("memory", m.name(), agentId);
        }
    }

    private void dispatchOne(NodeSpec node, String agentId, JobStatus status) throws Exception {
        JobStatus.NodeStatus ns = status.node(node.id());
        ns.state = JobStatus.State.RUNNING;
        ns.startedAt = Instant.now();
        ns.assignedAgent = agentId;   // for Mesh viz per-agent placement

        String taskId = "pt-" + TASK_SEQ.incrementAndGet();
        String resultSubject = "mesh.pipeline.result." + taskId;

        // Serialise the NodeSpec to YAML for the wire.
        String nodeYaml = JobSpecYaml.toJson(new JobSpec(taskId, "1", List.of(node)));

        ObjectNode env = JSON.createObjectNode();
        env.put("taskId", taskId);
        env.put("resultSubject", resultSubject);
        env.put("nodeSpec", nodeYaml);

        // Reflective NATS via the public Connection / Dispatcher / MessageHandler
        // interfaces so package-private impl classes don't block getMethod.
        Class<?> handlerIface = Class.forName("io.nats.client.MessageHandler");
        var dispatcherObj = connIface.getMethod("createDispatcher", handlerIface).invoke(nats,
                java.lang.reflect.Proxy.newProxyInstance(
                        handlerIface.getClassLoader(),
                        new Class[]{ handlerIface },
                        (proxy, method, args) -> {
                            if ("onMessage".equals(method.getName())) {
                                Object msg = args[0];
                                byte[] data = (byte[]) msg.getClass().getMethod("getData").invoke(msg);
                                onResult(taskId, ns, data);
                            }
                            return null;
                        }));
        dispIface.getMethod("subscribe", String.class)
                .invoke(dispatcherObj, resultSubject);

        ResultAccumulator acc = new ResultAccumulator();
        outstanding.put(taskId, acc);

        String targetSubject = "mesh.agent.pipeline." + agentId;
        connIface.getMethod("publish", String.class, byte[].class)
                .invoke(nats, targetSubject, JSON.writeValueAsBytes(env));
        connIface.getMethod("flush", Duration.class)
                .invoke(nats, Duration.ofSeconds(2));

        long deadline = System.currentTimeMillis() + 60_000;
        while (!acc.eos && System.currentTimeMillis() < deadline) {
            if (status.cancelRequested.get()) break;
            try { Thread.sleep(50); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }

        try { dispIface.getMethod("unsubscribe", String.class)
                .invoke(dispatcherObj, resultSubject); } catch (Exception ignored) { }
        outstanding.remove(taskId);

        ns.rowsOut = acc.rowCount;
        ns.state = acc.eos ? JobStatus.State.SUCCEEDED : JobStatus.State.FAILED;
        if (!acc.eos) ns.error = "no eos within 60s";
        ns.finishedAt = Instant.now();
        status.addEvent(new JobStatus.ProgressEvent(node.id(), "dispatched",
                "agent=" + agentId + " rows=" + acc.rowCount, Instant.now()));
    }

    private void onResult(String taskId, JobStatus.NodeStatus ns, byte[] data) {
        try {
            JsonNode msg = JSON.readTree(data);
            if (msg.hasNonNull("eos") && msg.get("eos").asBoolean()) {
                ResultAccumulator acc = outstanding.get(taskId);
                if (acc != null) {
                    acc.rowCount = msg.hasNonNull("rowCount") ? msg.get("rowCount").asLong() : acc.rows.size();
                    acc.eos = true;
                }
            } else if (msg.hasNonNull("row")) {
                ResultAccumulator acc = outstanding.get(taskId);
                if (acc != null) acc.rows.add(msg.get("row"));
                ns.rowsOut++;
            }
        } catch (Exception ignored) { }
    }

    private List<String> discoverAgentsWith(String cap) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(driverBaseUrl + "/mesh/agents"))
                .GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        JsonNode arr = JSON.readTree(resp.body());
        List<String> out = new java.util.ArrayList<>();
        for (JsonNode a : arr) {
            for (JsonNode c : a.get("capabilities")) {
                if (cap.equals(c.asText())) { out.add(a.get("agentId").asText()); break; }
            }
        }
        return out;
    }

    @Override
    public void close() {
        try { connIface.getMethod("close").invoke(nats); } catch (Exception ignored) { }
    }

    private static final class ResultAccumulator {
        final List<JsonNode> rows = new CopyOnWriteArrayList<>();
        volatile long rowCount;
        volatile boolean eos;
    }
}
