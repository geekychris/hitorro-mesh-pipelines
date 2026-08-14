/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Top-level job driver: topologically sorts the DAG on
 * {@link NodeSpec#depends()}, submits each level's nodes to a pool in
 * parallel, waits for each level to complete before advancing.
 *
 * <p>Phase-1 model: every node runs in the same JVM. Phase 2 replaces
 * this class with a {@code JobScheduler} that dispatches node executions
 * to remote agents via {@code TaskDescriptor}, keeping the same
 * {@link JobStatus} shape so the UI doesn't change.</p>
 */
public final class JobRunner implements AutoCloseable {

    private static final AtomicLong RUN_ID = new AtomicLong(System.currentTimeMillis());

    private final SinkRegistry sinkRegistry;
    private final ExecutorService pool;

    public JobRunner(SinkRegistry sinkRegistry) {
        this.sinkRegistry = sinkRegistry;
        this.pool = Executors.newFixedThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                r -> { Thread t = new Thread(r, "pipelines-node"); t.setDaemon(true); return t; });
    }

    public JobStatus run(JobSpec spec) {
        String jobId = "job-" + RUN_ID.incrementAndGet();
        JobStatus status = new JobStatus(jobId, spec.id());
        status.state = JobStatus.State.RUNNING;
        try {
            List<List<NodeSpec>> ranks = topoRank(spec.nodes());
            NodeRunner runner = new NodeRunner(sinkRegistry);
            for (List<NodeSpec> level : ranks) {
                if (level.size() == 1) {
                    runner.run(level.get(0), status);
                } else {
                    List<Future<?>> futs = new ArrayList<>(level.size());
                    for (NodeSpec n : level) futs.add(pool.submit(() -> runner.run(n, status)));
                    for (Future<?> f : futs) f.get();
                }
                // If any node in the level failed, abort further ranks.
                for (NodeSpec n : level) {
                    if (status.node(n.id()).state == JobStatus.State.FAILED) {
                        throw new RuntimeException("node " + n.id() + " failed: "
                                + status.node(n.id()).error);
                    }
                }
            }
            status.state = JobStatus.State.SUCCEEDED;
        } catch (Exception e) {
            status.state = JobStatus.State.FAILED;
            status.error = e.getMessage();
        } finally {
            status.finishedAt = Instant.now();
        }
        return status;
    }

    /**
     * Kahn's algorithm — one topological sort producing "ranks" (nodes at
     * the same rank have all their deps satisfied by earlier ranks and can
     * run concurrently).
     */
    private static List<List<NodeSpec>> topoRank(List<NodeSpec> nodes) {
        Map<String, NodeSpec> byId = new LinkedHashMap<>();
        Map<String, Integer> inDeg = new LinkedHashMap<>();
        for (NodeSpec n : nodes) {
            byId.put(n.id(), n);
            inDeg.put(n.id(), n.depends().size());
        }
        for (NodeSpec n : nodes) {
            for (String dep : n.depends()) {
                if (!byId.containsKey(dep)) {
                    throw new IllegalArgumentException("node " + n.id()
                            + " depends on unknown node '" + dep + "'");
                }
            }
        }
        List<List<NodeSpec>> ranks = new ArrayList<>();
        while (!inDeg.isEmpty()) {
            List<NodeSpec> level = new ArrayList<>();
            for (var e : inDeg.entrySet()) if (e.getValue() == 0) level.add(byId.get(e.getKey()));
            if (level.isEmpty()) {
                throw new IllegalArgumentException("cycle in job DAG — remaining: " + inDeg.keySet());
            }
            for (NodeSpec n : level) inDeg.remove(n.id());
            // Decrement in-degree of nodes whose deps just cleared.
            for (var e : inDeg.entrySet()) {
                NodeSpec n = byId.get(e.getKey());
                long dropped = n.depends().stream().filter(d -> !inDeg.containsKey(d)).count();
                inDeg.put(n.id(), (int) (n.depends().size() - dropped));
            }
            ranks.add(level);
        }
        return ranks;
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }
}
