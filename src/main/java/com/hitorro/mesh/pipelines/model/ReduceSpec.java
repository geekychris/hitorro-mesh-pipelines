/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * "Group by these columns, emit one row per group with these aggregates."
 * Runs AFTER all {@code steps}, BEFORE sinks. Phase 1 does in-process
 * hash aggregation; Phase 2 pushes group-by down to jvssql for larger
 * datasets and shuffles across agents for distributed rollups.
 */
public record ReduceSpec(List<String> groupBy, List<AggSpec> aggs,
                         boolean shuffle, int buckets) {

    /** Canonical constructor — copies list refs, no other logic. */
    public ReduceSpec {
        groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
        aggs    = aggs    == null ? List.of() : List.copyOf(aggs);
    }

    /** Jackson-facing factory that fills in defaults for optional fields. */
    @JsonCreator
    public static ReduceSpec fromJson(@JsonProperty("group-by") List<String> groupBy,
                                      @JsonProperty("aggs")     List<AggSpec> aggs,
                                      @JsonProperty("shuffle")  Boolean shuffle,
                                      @JsonProperty("buckets")  Integer buckets) {
        return new ReduceSpec(groupBy, aggs,
                shuffle != null && shuffle,
                buckets == null ? 2 : buckets);
    }

    /** Back-compat 2-arg constructor — shuffle off, single-reducer. */
    public ReduceSpec(List<String> groupBy, List<AggSpec> aggs) {
        this(groupBy, aggs, false, 2);
    }
}
