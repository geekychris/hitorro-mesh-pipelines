/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Streaming NATS core-subscribe source. Blocks on {@link Subscription#nextMessage}
 * with a short timeout so it can check the {@code cancelled} flag between
 * polls and shut down cleanly. Each message payload is parsed as JSON;
 * malformed payloads are wrapped as {@code {"_raw": "..."}}.
 *
 * <p>The connection is closed when the iterator's {@link #close} runs
 * (called by NodeRunner's try-with-resources on the source).</p>
 */
public final class NatsSource implements Iterator<JsonNode>, AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration POLL = Duration.ofMillis(500);

    private final Connection conn;
    private final Subscription sub;
    private final AtomicBoolean cancelled;
    private JsonNode next;

    public NatsSource(String servers, String subject, AtomicBoolean cancelled) throws Exception {
        this.cancelled = cancelled;
        String s = servers == null ? "nats://localhost:4222" : servers;
        this.conn = Nats.connect(s);
        this.sub  = conn.subscribe(subject);
    }

    @Override
    public boolean hasNext() {
        while (next == null && !cancelled.get()) {
            try {
                Message m = sub.nextMessage(POLL);
                if (m == null) continue;   // poll timed out; check cancel + loop
                byte[] data = m.getData();
                if (data == null || data.length == 0) continue;
                try {
                    next = JSON.readTree(data);
                } catch (Exception badJson) {
                    next = JSON.createObjectNode().put("_raw", new String(data));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return next != null;
    }

    @Override
    public JsonNode next() {
        if (!hasNext()) throw new java.util.NoSuchElementException();
        JsonNode out = next; next = null; return out;
    }

    @Override
    public void close() {
        try { sub.unsubscribe(); } catch (Exception ignored) { }
        try { conn.close(); }     catch (Exception ignored) { }
    }
}
