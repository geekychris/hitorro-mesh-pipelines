/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * The internal pipeline of one {@link NodeSpec}: source → steps → optional
 * reduce → one-or-more sinks. Steps are applied in list order; reduce (if
 * present) runs after all steps and materialises fresh rows to the sinks.
 */
public record PipelineSpec(SourceSpec source,
                           List<StepSpec> steps,
                           ReduceSpec reduce,
                           List<SinkSpec> sinks) {

    @JsonCreator
    public PipelineSpec(@JsonProperty("source") SourceSpec source,
                        @JsonProperty("steps")  List<StepSpec> steps,
                        @JsonProperty("reduce") ReduceSpec reduce,
                        @JsonProperty("sinks")  List<SinkSpec> sinks) {
        this.source = source;
        this.steps  = steps == null ? List.of() : List.copyOf(steps);
        this.reduce = reduce;
        this.sinks  = sinks == null ? List.of() : List.copyOf(sinks);
    }
}
