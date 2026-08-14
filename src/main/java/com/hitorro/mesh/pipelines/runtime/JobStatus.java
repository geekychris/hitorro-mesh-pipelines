/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Live status of one job run — updated by {@link NodeRunner} as it works.
 * Snapshot-friendly (read-only projection via {@link #snapshot()}).
 */
public final class JobStatus {

    public enum State { PENDING, RUNNING, SUCCEEDED, FAILED, CANCELLED }

    public final String jobId;              // stable run id ("job-1729…")
    public final String jobSpecName;        // spec's `job:` field
    public final Instant startedAt;
    public volatile Instant finishedAt;
    public volatile State state = State.PENDING;
    public volatile String error;

    // Order matches topological rank so UI can render columns.
    private final Map<String, NodeStatus> nodes = new LinkedHashMap<>();

    // Progress events, tailed by the SSE endpoint.
    private final List<ProgressEvent> events = new CopyOnWriteArrayList<>();

    public JobStatus(String jobId, String jobSpecName) {
        this.jobId = jobId;
        this.jobSpecName = jobSpecName;
        this.startedAt = Instant.now();
    }

    public synchronized NodeStatus node(String id) {
        return nodes.computeIfAbsent(id, k -> new NodeStatus(k));
    }

    public synchronized Map<String, NodeStatus> nodes() { return new LinkedHashMap<>(nodes); }

    public synchronized void addEvent(ProgressEvent e) { events.add(e); }
    public List<ProgressEvent> events() { return List.copyOf(events); }

    /** Public snapshot for JSON serialisation. */
    public synchronized Snapshot snapshot() {
        return new Snapshot(jobId, jobSpecName, state.name(),
                startedAt.toString(),
                finishedAt == null ? null : finishedAt.toString(),
                error,
                nodes.values().stream().map(NodeStatus::snapshot).toList());
    }

    // ---------------------------------------------------------- Node status
    public static final class NodeStatus {
        public final String id;
        public volatile State state = State.PENDING;
        public volatile long rowsIn;
        public volatile long rowsOut;
        public volatile Instant startedAt;
        public volatile Instant finishedAt;
        public volatile String error;
        public final Map<String, Long> sinkCounts = new LinkedHashMap<>();

        NodeStatus(String id) { this.id = id; }

        public synchronized NodeStatusSnapshot snapshot() {
            return new NodeStatusSnapshot(id, state.name(), rowsIn, rowsOut,
                    startedAt == null ? null : startedAt.toString(),
                    finishedAt == null ? null : finishedAt.toString(),
                    error, new LinkedHashMap<>(sinkCounts));
        }
    }

    // ---------------------------------------------------------- Progress event
    public record ProgressEvent(String nodeId, String kind, String message, Instant at) { }

    // ---------------------------------------------------------- Snapshots
    public record Snapshot(String jobId, String jobSpecName, String state,
                           String startedAt, String finishedAt, String error,
                           List<NodeStatusSnapshot> nodes) { }

    public record NodeStatusSnapshot(String id, String state,
                                     long rowsIn, long rowsOut,
                                     String startedAt, String finishedAt,
                                     String error, Map<String, Long> sinkCounts) { }
}
