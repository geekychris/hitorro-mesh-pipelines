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
    @JsonSubTypes.Type(value = SinkSpec.JvsLucene.class,  name = "jvs-lucene"),
    @JsonSubTypes.Type(value = SinkSpec.Nats.class,       name = "nats"),
    @JsonSubTypes.Type(value = SinkSpec.Kafka.class,      name = "kafka"),
    @JsonSubTypes.Type(value = SinkSpec.Counting.class,   name = "counting"),
    @JsonSubTypes.Type(value = SinkSpec.MemoryTable.class,name = "memory-table"),
    @JsonSubTypes.Type(value = SinkSpec.ShuffleFanout.class,name = "_shuffle-fanout"),
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
    record KvStore(String name, String keyExpr, Boolean registerAsTable) implements SinkSpec {
        /** Back-compat single-arg — registerAsTable defaults to false. */
        public KvStore(String name, String keyExpr) { this(name, keyExpr, Boolean.FALSE); }
    }

    /**
     * Lucene index. {@code storeSource} toggles _source retention.
     * Stub in Phase 1; concrete adapter in
     * {@code hitorro-mesh-pipelines-lucene}.
     */
    record Lucene(String name, boolean storeSource) implements SinkSpec { }

    /**
     * Type-aware Lucene index driven by the JVS type system. Each row is
     * wrapped as a JVS with the loaded {@code Type}, then handed to
     * {@code JVSLuceneIndexWriter} — every field is projected per the
     * type's {@code groups[].method} (identifier → StringField,
     * text → TextField w/ lang analyzer, mls → per-language TextField,
     * long → LongPoint + NumericDocValuesField, etc.).
     *
     * <p>{@code typeJsonResource} is a Spring resource URL — {@code file:...},
     * {@code classpath:...}, or {@code http(s)://...} — pointing at the
     * type JSON. The adapter lives in
     * {@code hitorro-mesh-pipelines-jvstype}; drop that jar on the
     * classpath to enable this sink kind.</p>
     */
    record JvsLucene(String name, String typeJsonResource, boolean storeSource) implements SinkSpec { }

    /**
     * NATS core-publish sink. Serialises each row as JSON, publishes to
     * {@code subject}. Servers defaults to {@code nats://localhost:4222}.
     * Fire-and-forget — the JetStream ack path lands with the streaming
     * adapter module.
     */
    record Nats(String subject, String servers) implements SinkSpec { }

    /**
     * Kafka producer sink. Serialises each row as JSON. Optional
     * {@code keyExpr} (dotted path into the row) sets the partition key;
     * without it, partition assignment is round-robin.
     */
    record Kafka(String bootstrap, String topic, String keyExpr) implements SinkSpec { }

    /** Counting sink — increments a labelled counter surfaced in job status. */
    record Counting(String label) implements SinkSpec { }

    /**
     * In-memory list of rows kept for the lifetime of the runner instance.
     * Useful for tests and for downstream {@code ref} sources within the
     * same job process.
     */
    record MemoryTable(String name) implements SinkSpec { }

    /**
     * <b>Internal.</b> Hash-partitions each row by {@code keyExpr} into
     * one of {@code buckets} NATS subjects
     * ({@code subjectPrefix + "." + bucket}). Used by the scheduler when
     * it auto-splits a reduce node into mapper × M + reducer × K tasks.
     * Not intended for user job specs.
     *
     * <p>On {@link #close} the sink publishes an EOS envelope
     * {@code {"_eos": true, "mapperId": mapperId}} to every bucket so
     * downstream reducers know when to finalise.</p>
     */
    record ShuffleFanout(String subjectPrefix, int buckets, String keyExpr,
                        String mapperId, String natsUrl) implements SinkSpec { }
}
