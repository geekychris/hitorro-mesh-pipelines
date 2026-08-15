/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
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

    private final String natsUrl;
    private final String driverBaseUrl;
    private final HttpClient http;
    private final Object nats;         // io.nats.client.Connection when jnats present
    private final Object dispatcher;   // io.nats.client.Dispatcher when jnats present
    private final java.util.Map<String, ResultAccumulator> outstanding = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * @param natsUrl        NATS server URL (usually {@code nats://localhost:4222})
     * @param driverBaseUrl  where the driver's REST endpoints live
     */
    public PipelineScheduler(String natsUrl, String driverBaseUrl) throws Exception {
        this.natsUrl = natsUrl;
        this.driverBaseUrl = driverBaseUrl;
        this.http = HttpClient.newHttpClient();
        try {
            Class<?> natsClass = Class.forName("io.nats.client.Nats");
            this.nats = natsClass.getMethod("connect", String.class).invoke(null, natsUrl);
            // We don't wire the dispatcher here — the driver-side result
            // subject is subscribed on-demand per dispatch. Keeping this
            // reflective so pipelines core doesn't force jnats on the
            // classpath at compile time.
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
            for (int i = 0; i < spec.nodes().size(); i++) {
                if (status.cancelRequested.get()) break;
                NodeSpec node = spec.nodes().get(i);
                String agentId = targetAgents.get(i % targetAgents.size());
                dispatchOne(node, agentId, status);
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

    private void dispatchOne(NodeSpec node, String agentId, JobStatus status) throws Exception {
        JobStatus.NodeStatus ns = status.node(node.id());
        ns.state = JobStatus.State.RUNNING;
        ns.startedAt = Instant.now();

        String taskId = "pt-" + TASK_SEQ.incrementAndGet();
        String resultSubject = "mesh.pipeline.result." + taskId;

        // Serialise the NodeSpec to YAML for the wire.
        String nodeYaml = JobSpecYaml.toJson(new JobSpec(taskId, "1", List.of(node)));

        ObjectNode env = JSON.createObjectNode();
        env.put("taskId", taskId);
        env.put("resultSubject", resultSubject);
        env.put("nodeSpec", nodeYaml);

        // Publish + wait for eos. Reflective NATS calls to keep pipelines
        // core free of a hard jnats compile-time dep.
        Class<?> connClass = nats.getClass();
        // subscribe first so we don't miss the eos.
        var dispatcherObj = connClass.getMethod("createDispatcher",
                Class.forName("io.nats.client.MessageHandler")).invoke(nats,
                java.lang.reflect.Proxy.newProxyInstance(
                        connClass.getClassLoader(),
                        new Class[]{ Class.forName("io.nats.client.MessageHandler") },
                        (proxy, method, args) -> {
                            if ("onMessage".equals(method.getName())) {
                                Object msg = args[0];
                                byte[] data = (byte[]) msg.getClass().getMethod("getData").invoke(msg);
                                onResult(taskId, ns, data);
                            }
                            return null;
                        }));
        dispatcherObj.getClass().getMethod("subscribe", String.class)
                .invoke(dispatcherObj, resultSubject);

        ResultAccumulator acc = new ResultAccumulator();
        outstanding.put(taskId, acc);

        String targetSubject = "mesh.agent.pipeline." + agentId;
        connClass.getMethod("publish", String.class, byte[].class)
                .invoke(nats, targetSubject, JSON.writeValueAsBytes(env));
        connClass.getMethod("flush", Duration.class)
                .invoke(nats, Duration.ofSeconds(2));

        // Wait for eos with a 60s cap.
        long deadline = System.currentTimeMillis() + 60_000;
        while (!acc.eos && System.currentTimeMillis() < deadline) {
            if (status.cancelRequested.get()) break;
            try { Thread.sleep(50); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }

        // Detach.
        try { dispatcherObj.getClass().getMethod("unsubscribe", String.class)
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
        try { nats.getClass().getMethod("close").invoke(nats); } catch (Exception ignored) { }
    }

    private static final class ResultAccumulator {
        final List<JsonNode> rows = new CopyOnWriteArrayList<>();
        volatile long rowCount;
        volatile boolean eos;
    }
}
