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
}
