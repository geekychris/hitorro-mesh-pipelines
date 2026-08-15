/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records which agent's local filesystem holds each named persistent
 * sink (kvstore + lucene). Written by the {@link PipelineScheduler}
 * after a node succeeds; read by the scheduler when placing downstream
 * nodes so their {@code source: {kind: kvstore, name: X}} runs on the
 * agent that has X locally — data locality without a fetch step.
 *
 * <p>Also records which agent hosted each named memory-table (best-
 * effort — memory tables are process-local, so a downstream ref-source
 * placement must go to the same agent or fetch from it).</p>
 *
 * <p>Registry is process-local to the driver JVM — restart clears it.
 * Consequently pipeline chains that assume prior sink placement need
 * to happen in the same driver lifetime; long-lived deployments where
 * you want persistent placement should back this by an external
 * lookup (zookeeper, etcd, ...) — deferred.</p>
 */
public final class SinkLocationRegistry {

    /** sinkKind:name → agentId of last writer. */
    private final Map<String, String> byKey = new ConcurrentHashMap<>();

    public void record(String kind, String name, String agentId) {
        if (name == null || agentId == null) return;
        byKey.put(kind + ":" + name, agentId);
    }

    /** Returns the agentId that most recently wrote (kind, name), or null. */
    public String locate(String kind, String name) {
        if (name == null) return null;
        return byKey.get(kind + ":" + name);
    }

    /** Full snapshot for UI / diagnostics. */
    public Map<String, String> snapshot() {
        return Map.copyOf(byKey);
    }
}
