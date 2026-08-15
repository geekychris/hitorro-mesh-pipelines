/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fire-and-forget NATS core-publish sink. Every row is serialised to
 * JSON bytes and published to {@code subject}. Rows land on the NATS
 * message bus; other pipelines can consume them via {@code NatsSource}
 * to form a multi-process compute graph without a shared file store.
 */
public final class NatsSink implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String servers;
    private final String subject;
    private final AtomicLong n = new AtomicLong();
    private Connection conn;

    public NatsSink(String servers, String subject) {
        this.servers = servers == null ? "nats://localhost:4222" : servers;
        this.subject = subject;
    }

    @Override
    public void open() throws Exception {
        conn = Nats.connect(servers);
    }

    @Override
    public void add(JsonNode row) throws Exception {
        if (conn == null) open();
        conn.publish(subject, JSON.writeValueAsBytes(row));
        // Flush every message so tests using small volumes see them
        // arrive immediately at subscribers. High-throughput jobs can
        // rely on jnats's own batching plus close()-time flush.
        conn.flush(java.time.Duration.ofSeconds(2));
        n.incrementAndGet();
    }

    @Override public long count() { return n.get(); }

    @Override
    public void close() {
        try { if (conn != null) conn.flush(java.time.Duration.ofSeconds(2)); } catch (Exception ignored) { }
        try { if (conn != null) conn.close(); } catch (Exception ignored) { }
    }

    public String servers() { return servers; }
    public String subject() { return subject; }
}
