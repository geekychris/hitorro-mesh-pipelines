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
     * Run each row through a Groovy transform script — the same DSL the
     * shipped {@code GroovyTransformMapper} understands. Script text is
     * inline; {@code script-file} loading is Phase 2.
     */
    record GroovyMap(String script) implements StepSpec { }

    /** Run a SQL query treating the incoming rows as an in-memory table. */
    record Jvssql(String sql) implements StepSpec { }
}
