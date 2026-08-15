/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.atomic.AtomicLong;

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
public final class ShuffleFanoutSink implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String subjectPrefix;
    private final int buckets;
    private final String keyExpr;
    private final String mapperId;
    private final String natsUrl;
    private final AtomicLong n = new AtomicLong();

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
    public void open() throws Exception {
        connIface = Class.forName("io.nats.client.Connection");
        Class<?> natsClass = Class.forName("io.nats.client.Nats");
        conn = natsClass.getMethod("connect", String.class).invoke(null, natsUrl);
    }

    @Override
    public void add(JsonNode row) throws Exception {
        if (conn == null) open();
        int bucket = pickBucket(row);
        String subj = subjectPrefix + "." + bucket;
        byte[] payload = JSON.writeValueAsBytes(
                JSON.createObjectNode().set("row", row));
        connIface.getMethod("publish", String.class, byte[].class)
                .invoke(conn, subj, payload);
        n.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
        if (conn == null) return;
        // Publish EOS to every bucket so waiting reducers know this
        // mapper has finished.
        for (int b = 0; b < buckets; b++) {
            String subj = subjectPrefix + "." + b;
            byte[] eos = JSON.writeValueAsBytes(
                    JSON.createObjectNode().put("_eos", true).put("mapperId", mapperId));
            try {
                connIface.getMethod("publish", String.class, byte[].class)
                        .invoke(conn, subj, eos);
            } catch (Exception ignored) { }
        }
        try {
            connIface.getMethod("flush", java.time.Duration.class)
                    .invoke(conn, java.time.Duration.ofSeconds(5));
        } catch (Exception ignored) { }
        try { connIface.getMethod("close").invoke(conn); } catch (Exception ignored) { }
    }

    @Override public long count() { return n.get(); }

    private int pickBucket(JsonNode row) {
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
