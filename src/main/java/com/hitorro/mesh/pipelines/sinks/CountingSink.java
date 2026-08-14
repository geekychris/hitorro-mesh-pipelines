/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Discards rows, counts them. Used by tests and by the "Try in Playground"
 * button as a fast dry-run sink.
 */
public final class CountingSink implements Sink {

    private final String label;
    private final AtomicLong n = new AtomicLong();

    public CountingSink(String label) {
        this.label = label == null ? "count" : label;
    }

    @Override
    public void add(JsonNode row) {
        n.incrementAndGet();
    }

    @Override public long count()   { return n.get(); }
    @Override public void close()   { }
    public String label()           { return label; }
}
