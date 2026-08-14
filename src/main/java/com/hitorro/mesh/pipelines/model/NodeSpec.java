/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * One vertex of the DAG. Runs its {@link PipelineSpec} once (or once per
 * partition key if {@link PartitionSpec} is set). Downstream nodes list
 * this id in {@code depends} and read its materialised output via a
 * {@code source: {kind: ref, node: <this-id>}}.
 */
public record NodeSpec(String id,
                       PartitionSpec partition,
                       PipelineSpec pipeline,
                       List<String> depends) {

    @JsonCreator
    public NodeSpec(@JsonProperty("id")        String id,
                    @JsonProperty("partition") PartitionSpec partition,
                    @JsonProperty("pipeline")  PipelineSpec pipeline,
                    @JsonProperty("depends")   List<String> depends) {
        this.id        = id;
        this.partition = partition;
        this.pipeline  = pipeline;
        this.depends   = depends == null ? List.of() : List.copyOf(depends);
    }
}
