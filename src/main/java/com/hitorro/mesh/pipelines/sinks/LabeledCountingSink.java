/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.util.core.iterator.sinks.CountingSink;

/**
 * Core {@link CountingSink} plus a caller-friendly label so the pipeline
 * UI's {@code sinkCounts} map can show something more descriptive than
 * the class name (e.g. {@code count:seed-count} instead of
 * {@code CountingSink}).
 *
 * <p>Kept in mesh (not pushed to core) because "sinks carry a label for
 * UI display" is a pipeline concern, not a Sink&lt;T&gt; concern —
 * other Sink users identify their sinks any way they like (bean name,
 * type name, ...).</p>
 */
public final class LabeledCountingSink extends CountingSink<JsonNode> {

    private final String label;

    public LabeledCountingSink(String label) {
        this.label = label == null ? "count" : label;
    }

    public String label() {
        return label;
    }
}
