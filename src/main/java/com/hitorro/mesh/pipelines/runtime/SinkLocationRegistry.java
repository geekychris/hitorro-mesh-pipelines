/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records which agent's local filesystem holds each named persistent
 * sink (kvstore + lucene). Written by the {@link PipelineScheduler}
 * after a node succeeds; read by the scheduler when placing downstream
 * nodes so their {@code source: {kind: kvstore, name: X}} runs on the
 * agent that has X locally — data locality without a fetch step.
 *
 * <p>Optionally backed by a JSON snapshot file on disk — reloaded on
 * construction, saved on every {@link #record}. Pipeline chains
 * survive driver restart: a run today writes to {@code kvstore:users}
 * on agent-us; tomorrow's driver comes back up and downstream jobs
 * still know to schedule users-consuming nodes on agent-us.</p>
 *
 * <p>External lookup (etcd / zk / consul) is deferred — appropriate
 * for multi-driver deployments where more than one JVM needs to see
 * the same view. Single-driver deployments are covered by the
 * on-disk snapshot.</p>
 */
public final class SinkLocationRegistry {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** sinkKind:name → agentId of last writer. */
    private final Map<String, String> byKey = new ConcurrentHashMap<>();

    /** Where to persist snapshots. Null = in-memory only. */
    private final Path snapshotPath;

    public SinkLocationRegistry() {
        this(null);
    }

    public SinkLocationRegistry(Path snapshotPath) {
        this.snapshotPath = snapshotPath;
        if (snapshotPath != null && Files.exists(snapshotPath)) {
            try {
                Map<String, String> loaded = JSON.readValue(snapshotPath.toFile(),
                        new TypeReference<Map<String, String>>() { });
                byKey.putAll(loaded);
            } catch (IOException ignored) {
                // Corrupt file — start fresh, rewrite on next record.
            }
        }
    }

    /** Default location under the pipelines home. */
    public static SinkLocationRegistry defaultOnDisk() {
        String home = System.getProperty("hitorro.pipelines.home",
                System.getProperty("user.home") + "/.hitorro/pipelines");
        Path p = Path.of(home).resolve("sink-locations.json");
        return new SinkLocationRegistry(p);
    }

    public void record(String kind, String name, String agentId) {
        if (name == null || agentId == null) return;
        byKey.put(kind + ":" + name, agentId);
        persist();
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

    private void persist() {
        if (snapshotPath == null) return;
        try {
            Files.createDirectories(snapshotPath.getParent());
            JSON.writerWithDefaultPrettyPrinter()
                    .writeValue(snapshotPath.toFile(), byKey);
        } catch (IOException ignored) {
            // Best-effort — don't fail the pipeline for a snapshot IO error.
        }
    }
}
