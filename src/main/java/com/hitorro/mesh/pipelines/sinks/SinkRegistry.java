/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns symbolic-name → physical location mapping for kv / lucene sinks,
 * and hosts the in-process buffers backing memory-table sinks so downstream
 * nodes can read them via {@code source: {kind: ref, ...}}.
 *
 * <p>Concrete adapter modules (kvstore, lucene, basefile) will subclass or
 * decorate this registry to construct sinks backed by real RocksDB /
 * Lucene / BaseFile — the {@link #create} switch here selects the built-in
 * stub. Phase 1 semantics: durable enough to test end-to-end, sound enough
 * that adapters replace the stub cleanly.</p>
 */
public final class SinkRegistry {

    private final Path home;
    private final Map<String, List<JsonNode>> memoryTables = new ConcurrentHashMap<>();

    public SinkRegistry(Path home) {
        this.home = home;
    }

    public static SinkRegistry withDefaultHome() {
        String p = System.getProperty("hitorro.pipelines.home",
                Paths.get(System.getProperty("user.home"), ".hitorro", "pipelines").toString());
        return new SinkRegistry(Path.of(p));
    }

    public Path home() { return home; }

    /**
     * Retrieve (or create) the in-memory row buffer behind a
     * {@code memory-table} sink of the given name.
     */
    public List<JsonNode> memoryTable(String name) {
        return memoryTables.computeIfAbsent(name,
                k -> java.util.Collections.synchronizedList(new java.util.ArrayList<>()));
    }

    /** Build a fresh sink instance for one run. Caller {@link Sink#close}s. */
    public Sink create(SinkSpec spec) {
        return switch (spec) {
            case SinkSpec.NdjsonFile s -> new NdjsonFileSink(s.url());
            case SinkSpec.KvStore    s -> new KvStoreSink(s.name(), s.keyExpr(), home);
            case SinkSpec.Lucene     s -> new LuceneSink(s.name(), s.storeSource(), home);
            case SinkSpec.Counting   s -> new CountingSink(s.label());
            case SinkSpec.MemoryTable s -> new MemoryTableSink(s.name(), memoryTable(s.name()));
            case SinkSpec.CsvFile    s -> throw new UnsupportedOperationException(
                    "csv-file sink is Phase 2 (add jackson-csv or the basefile adapter)");
            case SinkSpec.JsonFile   s -> throw new UnsupportedOperationException(
                    "json-file sink is Phase 2");
            case SinkSpec.Nats       s -> throw new UnsupportedOperationException(
                    "nats sink is Phase 3 (streaming edges)");
            case SinkSpec.Kafka      s -> throw new UnsupportedOperationException(
                    "kafka sink is Phase 3 (streaming edges)");
        };
    }
}
