/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Top-level DAG spec — a job is an id + a list of nodes with dependencies
 * between them. Runtime topologically sorts on {@link NodeSpec#depends()}.
 */
public record JobSpec(String id, String version, List<NodeSpec> nodes) {

    @JsonCreator
    public JobSpec(@JsonProperty("job") @JsonAlias("id") String id,
                   @JsonProperty("version") String version,
                   @JsonProperty("nodes")   List<NodeSpec> nodes) {
        this.id      = id;
        this.version = version == null ? "1" : version;
        this.nodes   = nodes == null ? List.of() : List.copyOf(nodes);
    }
}
