/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

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
    /**
     * True when the job spec was submitted with {@code restartable: true}.
     * Set by JobRunner (or the resumer) when it starts running the spec.
     * Purely informational — the persistence + resume flow works off the
     * {@code RestartableJobStore}; this flag just exposes it to pollers
     * so the UI can badge the row.
     */
    public volatile boolean restartable;

    /**
     * Cooperative cancel flag. NodeRunner polls between each row and stops
     * the inner loop cleanly (still flushing / closing sinks) when set.
     * JobRunner skips remaining ranks. DELETE /mesh/jobs/{id} sets this.
     */
    public final AtomicBoolean cancelRequested = new AtomicBoolean();

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
                nodes.values().stream().map(NodeStatus::snapshot).toList(),
                restartable);
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
        /** Agent that ran this node (null when not dispatched, e.g. driver-local). */
        public volatile String assignedAgent;
        public final Map<String, Long> sinkCounts = new LinkedHashMap<>();
        /**
         * Upstream node ids this node reads via {@code depends} or via
         * {@code source: {kind: ref, node: …}}. Populated by JobRunner at
         * job start (before any node runs) so the UI can render arrows
         * before rows flow. Empty for root nodes.
         */
        public volatile java.util.List<String> deps = java.util.List.of();
        /**
         * Topological rank assigned by JobRunner's Kahn sort. Root nodes
         * are rank 0; each downstream node is {@code 1 + max(rank of deps)}.
         * The UI uses this to place nodes in columns.
         */
        public volatile int rank;

        NodeStatus(String id) { this.id = id; }

        public synchronized NodeStatusSnapshot snapshot() {
            return new NodeStatusSnapshot(id, state.name(), rowsIn, rowsOut,
                    startedAt == null ? null : startedAt.toString(),
                    finishedAt == null ? null : finishedAt.toString(),
                    error, assignedAgent, new LinkedHashMap<>(sinkCounts),
                    java.util.List.copyOf(deps), rank);
        }
    }

    // ---------------------------------------------------------- Progress event
    public record ProgressEvent(String nodeId, String kind, String message, Instant at) { }

    // ---------------------------------------------------------- Snapshots
    public record Snapshot(String jobId, String jobSpecName, String state,
                           String startedAt, String finishedAt, String error,
                           List<NodeStatusSnapshot> nodes, boolean restartable) {
        /** Back-compat 7-arg constructor — restartable defaults to false. */
        public Snapshot(String jobId, String jobSpecName, String state,
                        String startedAt, String finishedAt, String error,
                        List<NodeStatusSnapshot> nodes) {
            this(jobId, jobSpecName, state, startedAt, finishedAt, error, nodes, false);
        }
    }

    public record NodeStatusSnapshot(String id, String state,
                                     long rowsIn, long rowsOut,
                                     String startedAt, String finishedAt,
                                     String error, String assignedAgent,
                                     Map<String, Long> sinkCounts,
                                     List<String> deps, int rank) { }
}
