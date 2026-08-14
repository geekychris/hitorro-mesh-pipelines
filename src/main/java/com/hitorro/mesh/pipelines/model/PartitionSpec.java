/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * "Run this node N times, once per key." The current partition key is
 * available for {@code ${partition}} substitution in source / sink URLs.
 * Phase 1 runs partitions inline; Phase 2 dispatches each to a separate
 * agent by capability match.
 */
public record PartitionSpec(String by, List<String> keys) {

    @JsonCreator
    public PartitionSpec(@JsonProperty("by")   String by,
                         @JsonProperty("keys") List<String> keys) {
        this.by   = by;
        this.keys = keys == null ? List.of() : List.copyOf(keys);
    }
}
