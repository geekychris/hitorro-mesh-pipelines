/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One aggregation column produced by a {@link ReduceSpec}.
 * {@code name} is the output field name, {@code kind} is the operator,
 * {@code of} is the input column (unused for COUNT).
 */
public record AggSpec(String name, AggKind kind, String of) {

    @JsonCreator
    public AggSpec(@JsonProperty("name") String name,
                   @JsonProperty("kind") AggKind kind,
                   @JsonProperty("of")   String of) {
        this.name = name;
        this.kind = kind;
        this.of   = of;
    }

    public enum AggKind {
        COUNT, SUM, AVG, MIN, MAX, DISTINCT_COUNT,
        FIRST, LAST, COLLECT
    }
}
