/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.StepSpec;

import java.util.function.Function;

/**
 * Plugin point for {@link StepSpec} kinds that live in optional adapter
 * modules (real JVS projection via hitorro-jsontypesystem, MLE inference
 * steps, etc.). Discovered via {@link java.util.ServiceLoader}; consulted
 * before the built-in step switch.
 *
 * <p>Sub-modules ship
 * {@code META-INF/services/com.hitorro.mesh.pipelines.runtime.StepAdapter}
 * pointing at their impl.</p>
 */
public interface StepAdapter {

    /** Return true if this adapter wants to handle the given step spec. */
    boolean handles(StepSpec spec);

    /**
     * Compile the step into a per-row transform. Row-in, row-out; {@code null}
     * drops the row (filter behaviour).
     */
    Function<JsonNode, JsonNode> compile(StepSpec spec);
}
