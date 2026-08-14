/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In-process row buffer. Every write is appended to a shared list keyed by
 * name in the {@code SinkRegistry}; downstream nodes reading a
 * {@code ref} source pull from the same list. Fast, no I/O, but not
 * durable — lives for the runner's JVM lifetime.
 */
public final class MemoryTableSink implements Sink {

    private final String name;
    private final List<JsonNode> rows;

    public MemoryTableSink(String name, List<JsonNode> backingList) {
        this.name = name;
        this.rows = backingList;
    }

    @Override
    public synchronized void add(JsonNode row) {
        rows.add(row);
    }

    @Override public long count()   { return rows.size(); }
    @Override public void close()   { }
    public String name()            { return name; }

    /** Snapshot for readers. */
    public synchronized List<JsonNode> snapshot() {
        return Collections.unmodifiableList(new ArrayList<>(rows));
    }
}
