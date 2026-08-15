/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Push-based row terminator. Each row that flows through a node's pipeline
 * is offered to every attached sink in turn. Sinks own their resource
 * lifetime: {@link #open} before the first row, {@link #close} after EOS.
 *
 * <p>Deliberately a thin local interface — not tied to the hitorro-streams
 * {@code Sink}. Wiring layer is free to adapt this shape onto that one.</p>
 */
public interface Sink extends AutoCloseable {

    /** Prepare to receive rows. Called once, before the first {@link #add}. */
    default void open() throws Exception { }

    /** Consume one row. */
    void add(JsonNode row) throws Exception;

    /**
     * Idempotent write — sinks that support it should dedupe by
     * ({@code taskId}, {@code seq}) so retried tasks don't double-write.
     * Default implementation ignores the identity and calls {@link #add}
     * (at-least-once semantics; safe when the task never retries).
     *
     * <p>Adapters that back a persistent store (RocksDB, Lucene) should
     * override — track the highest seq seen per taskId and skip earlier
     * ones. The scheduler bumps a per-task counter and passes it here
     * so retry re-plays deterministically.</p>
     */
    default void addIdempotent(String taskId, long seq, JsonNode row) throws Exception {
        add(row);
    }

    /** Total rows written since open — surfaced in job status. */
    long count();

    @Override
    void close() throws Exception;
}
