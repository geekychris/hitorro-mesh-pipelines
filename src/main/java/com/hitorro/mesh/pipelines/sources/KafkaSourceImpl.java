/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Streaming Kafka consumer source. Runs forever unless cancelled;
 * batches from {@link KafkaConsumer#poll} are drained sequentially into
 * downstream steps. Poll timeout is short (500ms) so cancellation is
 * responsive.
 *
 * <p>String key + value serdes; value is parsed as JSON. Malformed JSON
 * values become {@code {"_raw": "..."}}.</p>
 */
public final class KafkaSourceImpl implements Iterator<JsonNode>, AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration POLL = Duration.ofMillis(500);

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean cancelled;
    private final Deque<ConsumerRecord<String, String>> buffer = new ArrayDeque<>();
    private JsonNode next;

    public KafkaSourceImpl(String bootstrap, String topic, String groupId,
                           AtomicBoolean cancelled) {
        this.cancelled = cancelled;
        Properties cfg = new Properties();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG,          groupId);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        this.consumer = new KafkaConsumer<>(cfg);
        this.consumer.subscribe(java.util.List.of(topic));
    }

    @Override
    public boolean hasNext() {
        while (next == null && !cancelled.get()) {
            if (!buffer.isEmpty()) {
                ConsumerRecord<String, String> r = buffer.poll();
                String v = r.value();
                if (v == null || v.isEmpty()) continue;
                try { next = JSON.readTree(v); }
                catch (Exception badJson) { next = JSON.createObjectNode().put("_raw", v); }
                continue;
            }
            ConsumerRecords<String, String> batch = consumer.poll(POLL);
            for (ConsumerRecord<String, String> r : batch) buffer.add(r);
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
        try { consumer.close(); } catch (Exception ignored) { }
    }
}
