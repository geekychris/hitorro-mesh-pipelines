/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
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
