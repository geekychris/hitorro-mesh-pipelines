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
 *
 * <p>{@code restartable=true} opts into persistent registration: the
 * driver writes the spec to disk on job start and re-submits it on
 * driver boot if it hasn't reached a terminal state. Meant for
 * long-running streaming jobs whose downstream sinks are idempotent
 * ({@code KvStoreSink} with its {@code addIdempotent} contract; other
 * sinks may see duplicates on restart because rows re-process from the
 * source). Batch jobs typically leave this off.</p>
 */
public record JobSpec(String id, String version, List<NodeSpec> nodes,
                      boolean restartable) {

    /** Canonical constructor — copies list ref, provides defaults. */
    public JobSpec {
        version = version == null ? "1" : version;
        nodes   = nodes == null ? List.of() : List.copyOf(nodes);
    }

    /** Back-compat 3-arg constructor — restartable defaults to false. */
    public JobSpec(String id, String version, List<NodeSpec> nodes) {
        this(id, version, nodes, false);
    }

    /** Jackson factory — accepts {@code job:} or {@code id:} as the name;
     *  {@code restartable} defaults to false when absent. */
    @JsonCreator
    public static JobSpec fromJson(
            @JsonProperty("job") @JsonAlias("id") String id,
            @JsonProperty("version") String version,
            @JsonProperty("nodes") List<NodeSpec> nodes,
            @JsonProperty("restartable") Boolean restartable) {
        return new JobSpec(id, version, nodes,
                restartable != null && restartable);
    }
}
