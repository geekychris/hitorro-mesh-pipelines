/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * Where a node's pipeline reads from. Discriminated by the {@code kind}
 * field in YAML/JSON — Jackson maps each kind literal to a concrete
 * subtype below.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes({
    @JsonSubTypes.Type(value = SourceSpec.NdjsonFile.class, name = "ndjson-file"),
    @JsonSubTypes.Type(value = SourceSpec.JsonFile.class,   name = "json-file"),
    @JsonSubTypes.Type(value = SourceSpec.CsvFile.class,    name = "csv-file"),
    @JsonSubTypes.Type(value = SourceSpec.Inline.class,     name = "inline"),
    @JsonSubTypes.Type(value = SourceSpec.Ref.class,        name = "ref"),
    @JsonSubTypes.Type(value = SourceSpec.KvStore.class,    name = "kvstore"),
    @JsonSubTypes.Type(value = SourceSpec.Lucene.class,     name = "lucene"),
    @JsonSubTypes.Type(value = SourceSpec.Sql.class,        name = "sql"),
    @JsonSubTypes.Type(value = SourceSpec.Nats.class,       name = "nats"),
    @JsonSubTypes.Type(value = SourceSpec.Kafka.class,      name = "kafka"),
    @JsonSubTypes.Type(value = SourceSpec.ShuffleBucket.class,name = "_shuffle-bucket"),
})
public sealed interface SourceSpec {

    /** NDJSON file — one JSON per line, transparent bzip/gzip by extension. */
    record NdjsonFile(String url) implements SourceSpec { }

    /** JSON file holding a top-level array of objects. */
    record JsonFile(String url) implements SourceSpec { }

    /**
     * CSV file. First line assumed to be a header; each row emitted as a
     * flat JSON object of string values (typed steps downstream can cast).
     */
    record CsvFile(String url) implements SourceSpec { }

    /** Small literal row list embedded in the job spec — useful for tests. */
    record Inline(List<java.util.Map<String, Object>> rows) implements SourceSpec { }

    /**
     * Reads the materialised output of an upstream node. Runtime resolves
     * this to the concrete edge type set for that node (file, KV, queue).
     */
    record Ref(String node) implements SourceSpec { }

    /** Scan a KV store — resolved by symbolic name via {@code SinkRegistry}. */
    record KvStore(String name) implements SourceSpec { }

    /** Scan a Lucene index — resolved by symbolic name. Optional query. */
    record Lucene(String name, String query) implements SourceSpec { }

    /**
     * Result of a SQL query run against the mesh. Phase 1 runs this locally
     * through the shipped SQL client; Phase 2 dispatches to mesh agents.
     */
    record Sql(String sql) implements SourceSpec { }

    /**
     * NATS subject subscription. Emits one row per received message; the
     * message payload must be valid JSON. Runs forever unless cancelled
     * — the containing job stays in state {@code RUNNING} until
     * {@code DELETE /mesh/jobs/{id}}.
     *
     * <p>{@code servers} defaults to {@code nats://localhost:4222} when
     * unset. Consumer is push-mode (core NATS, not JetStream).</p>
     */
    record Nats(String subject, String servers) implements SourceSpec { }

    /**
     * Kafka topic consumer. Emits one row per record. Streaming — same
     * lifecycle as the NATS source. {@code groupId} is required; ordering
     * within a partition preserved; parallelism across partitions is a
     * follow-up when nodes learn to partition.
     */
    record Kafka(String bootstrap, String topic, String groupId) implements SourceSpec { }

    /**
     * <b>Internal.</b> Subscribes to a single shuffle-bucket NATS subject
     * ({@code subjectPrefix + "." + bucket}), buffers rows, and yields
     * them once {@code expectedMappers} EOS envelopes have arrived. Used
     * by the scheduler as the reducer's source in auto-split reduce.
     * Not intended for user job specs.
     */
    record ShuffleBucket(String subjectPrefix, int bucket, int expectedMappers,
                        String natsUrl) implements SourceSpec { }
}
