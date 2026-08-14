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
public record ReduceSpec(List<String> groupBy, List<AggSpec> aggs) {

    @JsonCreator
    public ReduceSpec(@JsonProperty("group-by") List<String> groupBy,
                      @JsonProperty("aggs")     List<AggSpec> aggs) {
        this.groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
        this.aggs    = aggs    == null ? List.of() : List.copyOf(aggs);
    }
}
