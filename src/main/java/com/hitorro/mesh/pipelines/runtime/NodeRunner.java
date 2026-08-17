/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.model.PipelineSpec;
import com.hitorro.mesh.pipelines.model.StepSpec;
import com.hitorro.mesh.pipelines.sinks.LabeledCountingSink;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import com.hitorro.mesh.pipelines.sources.SourceFactory;
import com.hitorro.util.core.iterator.sinks.MemoryTableSink;
import com.hitorro.util.core.iterator.sinks.Sink;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Runs one node's pipeline end-to-end:
 * <ol>
 *   <li>Open the source into an iterator.</li>
 *   <li>Chain the {@code steps} (filter / project / etc.).</li>
 *   <li>If {@code reduce} is set, buffer + fold via {@link ReduceEngine}.</li>
 *   <li>Fan out every row to every sink.</li>
 *   <li>Auto-attach a {@code memory-table:<nodeId>} sink so downstream
 *       nodes with {@code source: {kind: ref, node: <nodeId>}} can read
 *       this node's output without the user having to declare it.</li>
 * </ol>
 * Sink close() runs in a finally block; per-sink errors are logged into
 * the node's progress events but don't abort other sinks in the same node.
 */
public final class NodeRunner {

    private final SinkRegistry sinkRegistry;
    private final SourceFactory sourceFactory;

    public NodeRunner(SinkRegistry sinkRegistry) {
        this.sinkRegistry  = sinkRegistry;
        this.sourceFactory = new SourceFactory(sinkRegistry);
    }

    public void run(NodeSpec node, JobStatus status) {
        JobStatus.NodeStatus ns = status.node(node.id());
        ns.state = JobStatus.State.RUNNING;
        ns.startedAt = Instant.now();
        status.addEvent(new JobStatus.ProgressEvent(node.id(), "start", "starting node", Instant.now()));

        PipelineSpec p = node.pipeline();

        // Always attach an auto memory-table sink so downstream refs work.
        // If the user also declared their own memory-table with a different
        // name that's fine — this one is a system-scoped default.
        List<Sink<JsonNode>> sinks = new ArrayList<>();
        MemoryTableSink refSink = new MemoryTableSink(node.id(),
                sinkRegistry.memoryTable(node.id()));
        sinks.add(refSink);
        for (var spec : p.sinks()) sinks.add(sinkRegistry.create(spec));

        try {
            // Open every sink up front — fail fast on bad configs.
            for (Sink<JsonNode> s : sinks) s.start();

            // Count source-side rows-in even when filters drop them.
            long[] rowsInHolder = { 0 };
            Iterator<JsonNode> rawSource = sourceFactory.open(p.source(), status.cancelRequested);
            Iterator<JsonNode> raw = wrap(rawSource, rowsInHolder);

            List<Function<JsonNode, JsonNode>> compiledSteps = p.steps().stream()
                    .map(s -> StepFactory.compile(s, sinkRegistry)).toList();
            Iterator<JsonNode> stream = StepFactory.chain(raw, compiledSteps);

            if (p.reduce() != null) {
                // Reduce buffers, folds, re-emits; rows-in is source count.
                stream = ReduceEngine.reduce(stream, p.reduce());
            }

            long rowsOut = 0;
            while (stream.hasNext()) {
                if (status.cancelRequested.get()) {
                    status.addEvent(new JobStatus.ProgressEvent(node.id(), "cancelled",
                            "stopping mid-flight at " + rowsOut + " rows", Instant.now()));
                    break;
                }
                JsonNode row = stream.next();
                for (Sink<JsonNode> s : sinks) {
                    try { s.add(row); }
                    catch (Exception e) {
                        status.addEvent(new JobStatus.ProgressEvent(node.id(), "sink-error",
                                sinkName(s) + ": " + e.getMessage(), Instant.now()));
                    }
                }
                rowsOut++;
                // Cheap volatile writes every row so pollers see live
                // progress for streaming pipelines. Batch the heavier
                // sink-count refresh + progress event every 64 rows.
                ns.rowsOut = rowsOut;
                ns.rowsIn  = rowsInHolder[0];
                // Sink counts refresh every 8 rows — friendly to
                // low-volume streaming pipelines whose UI otherwise
                // shows sinks={} forever. High-throughput pipelines
                // still amortise the map writes.
                if ((rowsOut & 7) == 0) {
                    for (Sink<JsonNode> s : sinks) ns.sinkCounts.put(sinkName(s), s.count());
                }
                if ((rowsOut & 1023) == 0) {
                    status.addEvent(new JobStatus.ProgressEvent(node.id(), "progress",
                            "rows: " + rowsOut, Instant.now()));
                }
            }
            ns.rowsOut = rowsOut;
            ns.rowsIn  = rowsInHolder[0];

            for (Sink<JsonNode> s : sinks) ns.sinkCounts.put(sinkName(s), s.count());

            if (status.cancelRequested.get()) {
                ns.state = JobStatus.State.CANCELLED;
            } else {
                ns.state = JobStatus.State.SUCCEEDED;
                status.addEvent(new JobStatus.ProgressEvent(node.id(), "done",
                        "done: " + rowsOut + " rows", Instant.now()));
            }
        } catch (Throwable e) {
            ns.state = JobStatus.State.FAILED;
            ns.error = e.getClass().getSimpleName() + ": " + e.getMessage();
            status.addEvent(new JobStatus.ProgressEvent(node.id(), "error", ns.error, Instant.now()));
            System.err.println("[NodeRunner] " + node.id() + " failed: " + ns.error);
            e.printStackTrace(System.err);
        } finally {
            for (Sink<JsonNode> s : sinks) {
                try { s.close(); }
                catch (Exception ignored) { /* logged in per-sink error above */ }
            }
            // Post-close: for any kvstore sink with registerAsTable set,
            // materialise the memory snapshot into $HITORRO_DATASETS_HOME
            // and POST /mesh/broadcast-tables. Best-effort — errors surface
            // as progress events, don't fail the node.
            for (var spec : p.sinks()) {
                if (spec instanceof com.hitorro.mesh.pipelines.model.SinkSpec.KvStore ks
                        && Boolean.TRUE.equals(ks.registerAsTable())) {
                    try {
                        String home = System.getProperty("hitorro.datasets.home",
                                System.getProperty("user.home") + "/.hitorro/datasets");
                        String driverUrl = System.getProperty("hitorro.pipelines.driverUrl",
                                "http://localhost:8085");
                        String summary = com.hitorro.mesh.pipelines.sinks.MeshTableBridge
                                .materializeAndRegister(ks.name(),
                                        sinkRegistry.memoryTable(node.id()),
                                        java.nio.file.Path.of(home),
                                        driverUrl);
                        status.addEvent(new JobStatus.ProgressEvent(node.id(),
                                "mesh-bridge", summary, Instant.now()));
                    } catch (Exception e) {
                        status.addEvent(new JobStatus.ProgressEvent(node.id(),
                                "mesh-bridge-error", e.getMessage(), Instant.now()));
                    }
                }
            }
            ns.finishedAt = Instant.now();
        }
    }

    private static Iterator<JsonNode> wrap(Iterator<JsonNode> it, long[] counter) {
        return new Iterator<>() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public JsonNode next() { counter[0]++; return it.next(); }
        };
    }

    private static String sinkName(Sink<JsonNode> s) {
        if (s instanceof MemoryTableSink m)       return "memory:" + m.name();
        if (s instanceof LabeledCountingSink c)   return "count:"  + c.label();
        return s.getClass().getSimpleName();
    }
}
