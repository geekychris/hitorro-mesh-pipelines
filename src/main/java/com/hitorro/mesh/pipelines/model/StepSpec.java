/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * A transform in the pipeline. Steps are chained; each takes one row in,
 * emits zero-or-one rows out. Multi-emission (flatMap) is deferred.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes({
    @JsonSubTypes.Type(value = StepSpec.Filter.class,     name = "filter"),
    @JsonSubTypes.Type(value = StepSpec.Project.class,    name = "project"),
    @JsonSubTypes.Type(value = StepSpec.Rename.class,     name = "rename"),
    @JsonSubTypes.Type(value = StepSpec.SetField.class,   name = "set-field"),
    @JsonSubTypes.Type(value = StepSpec.ToTyped.class,    name = "to-typed"),
    @JsonSubTypes.Type(value = StepSpec.Lookup.class,     name = "lookup"),
    @JsonSubTypes.Type(value = StepSpec.GroovyMap.class,  name = "groovy-map"),
    @JsonSubTypes.Type(value = StepSpec.Jvssql.class,     name = "jvssql"),
})
public sealed interface StepSpec {

    /**
     * Keep rows matching a simple expression. Phase 1 supports equals /
     * not-equals / greater-than / less-than / LIKE '%substr%' on a single
     * column: {@code "age > 18"}, {@code "country == 'US'"},
     * {@code "topics LIKE '%ai%'"}. Phase 2 adds Groovy expression eval.
     */
    record Filter(String expr) implements StepSpec { }

    /** Keep only the named fields. */
    record Project(List<String> cols) implements StepSpec { }

    /** Rename a field. */
    record Rename(String from, String to) implements StepSpec { }

    /** Set a field to a constant value. */
    record SetField(String name, Object value) implements StepSpec { }

    /**
     * Validate + coerce each field to a JVS-compatible primitive type.
     * Fields not in the schema are dropped unless {@code passthrough} is
     * true. Rows failing coercion are dropped (Phase 2: route to an
     * optional {@code dead-letter} sink).
     *
     * <p>Currently supports the JVS primitive families:
     * {@code core_string} · {@code core_long} · {@code core_double} ·
     * {@code core_boolean} · {@code core_timestamp} (ISO-8601 in, epoch
     * millis out). This is a Phase-1 subset of the full JVS type system;
     * the full {@code hitorro-jsontypesystem} integration lands in a
     * follow-up.</p>
     */
    record ToTyped(java.util.List<Field> fields, boolean passthrough)
            implements StepSpec {
        public record Field(String name, String type) { }
    }

    /**
     * Enrich each row by looking up matching entries in a previously-
     * populated {@code memory-table} (or, in a follow-up, a kvstore).
     * The lookup key is a dotted path into the source row; the target
     * table is scanned once and indexed by its {@code onField}. Every
     * name in {@code adds} is copied from the lookup row into the
     * source row (nulls preserve).
     *
     * <p>Not a JOIN in the SQL sense — a per-row broadcast enrichment.
     * When the lookup source is small (a country dimension, a lookup
     * of user-agent → device class, a currency-symbol table), this is
     * both cheaper than a JOIN and easier to describe.</p>
     */
    record Lookup(String from, String onField, String withKey,
                  java.util.List<String> adds) implements StepSpec { }

    /**
     * Run each row through a Groovy transform script — the same DSL the
     * shipped {@code GroovyTransformMapper} understands. Script text is
     * inline; {@code script-file} loading is Phase 2.
     */
    record GroovyMap(String script) implements StepSpec { }

    /** Run a SQL query treating the incoming rows as an in-memory table. */
    record Jvssql(String sql) implements StepSpec { }
}
