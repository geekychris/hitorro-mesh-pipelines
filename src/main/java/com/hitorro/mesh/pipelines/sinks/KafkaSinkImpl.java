/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka producer sink. Sends each row as a String key / String JSON value
 * record to {@code topic}. {@code keyExpr} is an optional dotted path
 * into the row — if unset, records land round-robin across partitions.
 */
public final class KafkaSinkImpl implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String bootstrap;
    private final String topic;
    private final String keyExpr;
    private final AtomicLong n = new AtomicLong();
    private KafkaProducer<String, String> producer;

    public KafkaSinkImpl(String bootstrap, String topic, String keyExpr) {
        this.bootstrap = bootstrap;
        this.topic     = topic;
        this.keyExpr   = keyExpr;
    }

    @Override
    public void open() {
        Properties cfg = new Properties();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,     bootstrap);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.LINGER_MS_CONFIG,              5);   // small batching
        producer = new KafkaProducer<>(cfg);
    }

    @Override
    public void add(JsonNode row) throws Exception {
        if (producer == null) open();
        String key = keyExpr == null ? null : extract(row, keyExpr);
        String val = JSON.writeValueAsString(row);
        producer.send(new ProducerRecord<>(topic, key, val));
        n.incrementAndGet();
    }

    @Override public long count() { return n.get(); }

    @Override
    public void close() {
        try { if (producer != null) producer.flush(); } catch (Exception ignored) { }
        try { if (producer != null) producer.close(); } catch (Exception ignored) { }
    }

    private static String extract(JsonNode row, String path) {
        JsonNode cur = row;
        for (String seg : path.split("\\.")) {
            if (cur == null || cur.isNull()) return null;
            cur = cur.get(seg);
        }
        return (cur == null || cur.isNull()) ? null : cur.asText();
    }
}
