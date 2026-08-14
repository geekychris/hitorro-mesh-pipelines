/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Where a node's output goes. A node can have multiple sinks (fan-out);
 * each receives every row that reaches the end of the pipeline. Concrete
 * sinks like KV / Lucene resolve their physical location through
 * {@code SinkRegistry}, keyed by the symbolic {@code name}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes({
    @JsonSubTypes.Type(value = SinkSpec.NdjsonFile.class, name = "ndjson-file"),
    @JsonSubTypes.Type(value = SinkSpec.CsvFile.class,    name = "csv-file"),
    @JsonSubTypes.Type(value = SinkSpec.JsonFile.class,   name = "json-file"),
    @JsonSubTypes.Type(value = SinkSpec.KvStore.class,    name = "kvstore"),
    @JsonSubTypes.Type(value = SinkSpec.Lucene.class,     name = "lucene"),
    @JsonSubTypes.Type(value = SinkSpec.Nats.class,       name = "nats"),
    @JsonSubTypes.Type(value = SinkSpec.Kafka.class,      name = "kafka"),
    @JsonSubTypes.Type(value = SinkSpec.Counting.class,   name = "counting"),
    @JsonSubTypes.Type(value = SinkSpec.MemoryTable.class,name = "memory-table"),
})
public sealed interface SinkSpec {

    /** NDJSON file — one JSON per line, compression via extension. */
    record NdjsonFile(String url) implements SinkSpec { }

    /** CSV file with header. */
    record CsvFile(String url, java.util.List<String> cols) implements SinkSpec { }

    /** JSON file holding an array of objects. */
    record JsonFile(String url, boolean pretty) implements SinkSpec { }

    /**
     * RocksDB-backed key-value store. {@code keyExpr} is a dotted path
     * into the row (e.g. {@code "id"} or {@code "user.id"}). Phase 1
     * ships a stub that logs — the concrete adapter arrives with the
     * {@code hitorro-mesh-pipelines-kvstore} module.
     */
    record KvStore(String name, String keyExpr) implements SinkSpec { }

    /**
     * Lucene index. {@code storeSource} toggles _source retention.
     * Stub in Phase 1; concrete adapter in
     * {@code hitorro-mesh-pipelines-lucene}.
     */
    record Lucene(String name, boolean storeSource) implements SinkSpec { }

    /** NATS subject — Phase 3 (streaming edge). */
    record Nats(String subject) implements SinkSpec { }

    /** Kafka topic — Phase 3 (streaming edge). */
    record Kafka(String topic) implements SinkSpec { }

    /** Counting sink — increments a labelled counter surfaced in job status. */
    record Counting(String label) implements SinkSpec { }

    /**
     * In-memory list of rows kept for the lifetime of the runner instance.
     * Useful for tests and for downstream {@code ref} sources within the
     * same job process.
     */
    record MemoryTable(String name) implements SinkSpec { }
}
