/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reducer-side shuffle source for auto-split reduce. Subscribes to a
 * single bucket subject (one of the fanout targets) and yields rows in
 * arrival order. Blocks in {@link #hasNext} until at least one of:
 *
 * <ul>
 *   <li>a row is available in the local queue (return true, drain it)</li>
 *   <li>{@code expectedMappers} EOS envelopes have been received AND the
 *       queue is empty (return false)</li>
 *   <li>{@code cancelled} flips (return false)</li>
 * </ul>
 *
 * <p>Envelopes on the wire are:</p>
 * <pre>
 *   {"row": &lt;json object&gt;}
 *   {"_eos": true, "mapperId": "..."}
 * </pre>
 */
public final class ShuffleBucketSource implements Iterator<JsonNode>, AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final int expectedMappers;
    private final AtomicBoolean cancelled;
    private final AtomicInteger eosCount = new AtomicInteger();
    private final Set<String> eosMapperIds = java.util.Collections.synchronizedSet(new HashSet<>());
    private final Deque<JsonNode> queue = new java.util.concurrent.LinkedBlockingDeque<>();

    private final Object conn;              // io.nats.client.Connection
    private final Object dispatcher;        // io.nats.client.Dispatcher
    private final Class<?> connIface;
    private final Class<?> dispIface;
    private final String subject;

    private JsonNode next;

    public ShuffleBucketSource(String subjectPrefix, int bucket, int expectedMappers,
                               String natsUrl, AtomicBoolean cancelled) throws Exception {
        this.expectedMappers = Math.max(1, expectedMappers);
        this.cancelled = cancelled;
        this.subject = subjectPrefix + "." + bucket;

        connIface = Class.forName("io.nats.client.Connection");
        dispIface = Class.forName("io.nats.client.Dispatcher");
        Class<?> handlerIface = Class.forName("io.nats.client.MessageHandler");
        Class<?> natsClass = Class.forName("io.nats.client.Nats");
        this.conn = natsClass.getMethod("connect", String.class)
                .invoke(null, natsUrl == null ? "nats://localhost:4222" : natsUrl);
        this.dispatcher = connIface.getMethod("createDispatcher", handlerIface).invoke(conn,
                java.lang.reflect.Proxy.newProxyInstance(
                        handlerIface.getClassLoader(),
                        new Class[]{handlerIface},
                        (proxy, method, args) -> {
                            if ("onMessage".equals(method.getName())) {
                                Object msg = args[0];
                                byte[] data = (byte[]) msg.getClass().getMethod("getData").invoke(msg);
                                onMessage(data);
                            }
                            return null;
                        }));
        dispIface.getMethod("subscribe", String.class).invoke(dispatcher, subject);
    }

    private void onMessage(byte[] data) {
        try {
            JsonNode env = JSON.readTree(data);
            if (env.hasNonNull("_eos") && env.get("_eos").asBoolean()) {
                String mid = env.hasNonNull("mapperId") ? env.get("mapperId").asText() : ("m" + eosCount.get());
                if (eosMapperIds.add(mid)) eosCount.incrementAndGet();
            } else if (env.hasNonNull("row")) {
                queue.add(env.get("row"));
            }
        } catch (Exception ignored) { }
    }

    private final long idleDeadline = System.currentTimeMillis() + 30_000;

    @Override
    public boolean hasNext() {
        long lastActivity = System.currentTimeMillis();
        while (next == null) {
            if (cancelled.get()) return false;
            JsonNode q = queue.pollFirst();
            if (q != null) { next = q; return true; }
            if (eosCount.get() >= expectedMappers && queue.isEmpty()) return false;
            // Idle-timeout fallback — if we've been waiting 30 s with no
            // activity and no full EOS set, give up (partial results) so
            // a stuck mapper doesn't hang the whole job forever.
            if (System.currentTimeMillis() > idleDeadline) return false;
            try { Thread.sleep(20); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
        }
        return true;
    }

    @Override
    public JsonNode next() {
        if (!hasNext()) throw new java.util.NoSuchElementException();
        JsonNode out = next; next = null; return out;
    }

    @Override
    public void close() {
        try { dispIface.getMethod("unsubscribe", String.class).invoke(dispatcher, subject); } catch (Exception ignored) { }
        try { connIface.getMethod("close").invoke(conn); } catch (Exception ignored) { }
    }
}
