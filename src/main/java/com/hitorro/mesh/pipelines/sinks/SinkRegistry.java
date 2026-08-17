/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.util.core.iterator.sinks.CountingSink;
import com.hitorro.util.core.iterator.sinks.MemoryTableSink;
import com.hitorro.util.core.iterator.sinks.NdjsonFileSink;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.core.iterator.sinks.kafka.KafkaSink;
import com.hitorro.util.core.iterator.sinks.nats.NatsSink;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wires {@link SinkSpec} → {@link Sink}{@code <JsonNode>}. Consults
 * ServiceLoader-registered adapters first (so
 * {@code hitorro-mesh-pipelines-kvstore}, {@code -lucene},
 * {@code -jvstype} can bind their real impls); falls through to a
 * built-in switch for the sinks whose home is core (memory-table,
 * counting, ndjson-file, nats, kafka).
 *
 * <p>Every concrete sink lives in its natural home module — nothing
 * here is a mesh-only sink apart from {@code ShuffleFanoutSink} (the
 * hash-bucket NATS fanout used by mesh's shuffle-join). Non-mesh
 * callers who just want to use the sinks directly should import them
 * from {@code hitorro-streams}, {@code hitorro-streams-kafka},
 * {@code hitorro-streams-nats}, {@code hitorro-kvstore}, or
 * {@code hitorro-index} instead of going through this registry.</p>
 */
public final class SinkRegistry {

    private final Path home;
    private final Map<String, List<JsonNode>> memoryTables = new ConcurrentHashMap<>();

    /**
     * Adapters loaded via ServiceLoader — sub-modules like
     * {@code hitorro-mesh-pipelines-kvstore} contribute real
     * RocksDB / Lucene / etc. sinks. Consulted first so real impls
     * override built-in errors.
     */
    private final List<SinkAdapter> adapters = new ArrayList<>();

    public SinkRegistry(Path home) {
        this.home = home;
        for (SinkAdapter a : ServiceLoader.load(SinkAdapter.class)) adapters.add(a);
    }

    /** Register a sink adapter programmatically (mainly for tests). */
    public void register(SinkAdapter adapter) {
        adapters.add(adapter);
    }

    public static SinkRegistry withDefaultHome() {
        String p = System.getProperty("hitorro.pipelines.home",
                Paths.get(System.getProperty("user.home"), ".hitorro", "pipelines").toString());
        return new SinkRegistry(Path.of(p));
    }

    public Path home() { return home; }

    /**
     * Retrieve (or create) the in-memory row buffer behind a
     * {@code memory-table} sink of the given name. Used by cross-node
     * {@code source: {kind: ref, node: X}} in the pipeline runtime.
     */
    public List<JsonNode> memoryTable(String name) {
        return memoryTables.computeIfAbsent(name,
                k -> java.util.Collections.synchronizedList(new java.util.ArrayList<>()));
    }

    /** Build a fresh sink instance for one run. Caller {@link Sink#close}s. */
    public Sink<JsonNode> create(SinkSpec spec) {
        // Adapter-first — sub-modules override built-in fallbacks.
        for (SinkAdapter a : adapters) {
            if (a.handles(spec)) return a.create(spec, home);
        }
        return switch (spec) {
            case SinkSpec.NdjsonFile s -> new NdjsonFileSink(s.url());
            case SinkSpec.KvStore    s -> throw new UnsupportedOperationException(
                    "kvstore sink needs hitorro-mesh-pipelines-kvstore on the classpath "
                    + "— the RocksDB adapter registers itself via ServiceLoader when present");
            case SinkSpec.Lucene     s -> throw new UnsupportedOperationException(
                    "lucene sink needs hitorro-mesh-pipelines-lucene on the classpath "
                    + "— the real Lucene adapter registers itself via ServiceLoader when present");
            case SinkSpec.JvsLucene  s -> throw new UnsupportedOperationException(
                    "jvs-lucene sink needs hitorro-mesh-pipelines-jvstype on the classpath");
            case SinkSpec.Counting   s -> new LabeledCountingSink(s.label());
            case SinkSpec.MemoryTable s -> new MemoryTableSink(s.name(), memoryTable(s.name()));
            case SinkSpec.Nats       s -> createNats(s);
            case SinkSpec.Kafka      s -> createKafka(s);
            case SinkSpec.ShuffleFanout s -> new com.hitorro.mesh.pipelines.sinks.ShuffleFanoutSink(
                    s.subjectPrefix(), s.buckets(), s.keyExpr(), s.mapperId(), s.natsUrl());
            case SinkSpec.CsvFile    s -> throw new UnsupportedOperationException(
                    "csv-file sink is Phase 2 (add jackson-csv or the basefile adapter)");
            case SinkSpec.JsonFile   s -> throw new UnsupportedOperationException(
                    "json-file sink is Phase 2");
        };
    }

    /** NATS sink — optional dep. Fails clearly if the jnats jar is absent. */
    private Sink<JsonNode> createNats(SinkSpec.Nats spec) {
        try {
            return new NatsSink(spec.servers(), spec.subject());
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    "nats sink needs io.nats:jnats on the classpath — add the optional dep", e);
        }
    }

    /** Kafka sink — optional dep. */
    private Sink<JsonNode> createKafka(SinkSpec.Kafka spec) {
        try {
            return new KafkaSink(spec.bootstrap(), spec.topic(), spec.keyExpr());
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    "kafka sink needs org.apache.kafka:kafka-clients on the classpath", e);
        }
    }
}
