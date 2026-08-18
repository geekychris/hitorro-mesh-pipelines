/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.util.core.iterator.sinks.JsonNodeSinkBase;

import java.io.IOException;

/**
 * Mapper-side shuffle sink for auto-split reduce. Hashes each incoming
 * row by a dotted-path {@code keyExpr} into one of {@code buckets} NATS
 * subjects ({@code subjectPrefix + "." + bucket}). On {@link #close}
 * publishes a terminal EOS envelope to every bucket so the reducer
 * knows when this mapper is done.
 *
 * <p>NATS is accessed reflectively so the pipelines-core module doesn't
 * force a compile-time jnats dependency — jnats has to be on the
 * runtime classpath (any agent using shuffle already pulls it via
 * hitorro-mesh-nats or hitorro-mesh-agent-pipelines).</p>
 */
public final class ShuffleFanoutSink extends JsonNodeSinkBase {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String subjectPrefix;
    private final int buckets;
    private final String keyExpr;
    private final String mapperId;
    private final String natsUrl;

    private Object conn;              // io.nats.client.Connection
    private Class<?> connIface;

    public ShuffleFanoutSink(String subjectPrefix, int buckets, String keyExpr,
                             String mapperId, String natsUrl) {
        this.subjectPrefix = subjectPrefix;
        this.buckets = Math.max(1, buckets);
        this.keyExpr = keyExpr == null ? "id" : keyExpr;
        this.mapperId = mapperId == null ? "m0" : mapperId;
        this.natsUrl = natsUrl == null ? "nats://localhost:4222" : natsUrl;
    }

    @Override
    public boolean start() throws IOException {
        try {
            connIface = Class.forName("io.nats.client.Connection");
            Class<?> natsClass = Class.forName("io.nats.client.Nats");
            conn = natsClass.getMethod("connect", String.class).invoke(null, natsUrl);
        } catch (Exception e) {
            throw new IOException("ShuffleFanoutSink connect failed: " + e.getMessage(), e);
        }
        return true;
    }

    @Override
    protected void writeRow(JsonNode row) throws IOException {
        if (conn == null) start();
        int bucket = pickBucket(row);
        String subj = subjectPrefix + "." + bucket;
        byte[] payload = JSON.writeValueAsBytes(
                JSON.createObjectNode().set("row", row));
        try {
            connIface.getMethod("publish", String.class, byte[].class)
                    .invoke(conn, subj, payload);
        } catch (Exception e) {
            throw new IOException("ShuffleFanoutSink publish " + subj + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (conn == null) return;
        // Publish EOS to every bucket so waiting reducers know this
        // mapper has finished.
        for (int b = 0; b < buckets; b++) {
            String subj = subjectPrefix + "." + b;
            try {
                byte[] eos = JSON.writeValueAsBytes(
                        JSON.createObjectNode().put("_eos", true).put("mapperId", mapperId));
                connIface.getMethod("publish", String.class, byte[].class)
                        .invoke(conn, subj, eos);
            } catch (Exception ignored) { }
        }
        try {
            connIface.getMethod("flush", java.time.Duration.class)
                    .invoke(conn, java.time.Duration.ofSeconds(5));
        } catch (Exception ignored) { }
        try { connIface.getMethod("close").invoke(conn); } catch (Exception ignored) { }
        conn = null;
    }

    /**
     * Package-private for {@code ShuffleFanoutHashTest}.
     * Deterministic hash-partitioning on a dotted-path key:
     *   bucket = |hashCode(key.asText or "<null>")| mod buckets
     * Same key always maps to same bucket — critical invariant for
     * the reducer to see all rows of a group together.
     */
    int pickBucket(JsonNode row) {
        return pickBucket(row, keyExpr, buckets);
    }

    static int pickBucket(JsonNode row, String keyExpr, int buckets) {
        JsonNode v = pluck(row, keyExpr);
        String key = (v == null || v.isNull()) ? "<null>" : v.asText();
        int h = key.hashCode();
        return (h & 0x7fffffff) % buckets;
    }

    private static JsonNode pluck(JsonNode row, String path) {
        JsonNode cur = row;
        for (String seg : path.split("\\.")) {
            if (cur == null || cur.isNull()) return null;
            cur = cur.get(seg);
        }
        return cur;
    }
}
