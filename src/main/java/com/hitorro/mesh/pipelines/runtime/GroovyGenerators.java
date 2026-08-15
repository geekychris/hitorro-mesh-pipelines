/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Small helper bound as {@code gen} in every groovy-map script. Keeps
 * transform scripts terse — {@code gen.uuid()} instead of pulling in
 * {@code java.util.UUID.randomUUID().toString()} explicitly.
 *
 * <p>Sequences are per-instance, so each script invocation gets its own
 * monotonically-increasing counters. Use {@code gen.next('name')} to
 * grab from a named counter shared across all rows in this pipeline
 * run.</p>
 */
public final class GroovyGenerators {

    private final java.util.Map<String, AtomicLong> counters = new java.util.concurrent.ConcurrentHashMap<>();

    /** Random UUIDv4 as a string. */
    public String uuid() { return UUID.randomUUID().toString(); }

    /** ISO-8601 UTC timestamp of the current instant. */
    public String now() { return Instant.now().toString(); }

    /** Epoch millis of the current instant. */
    public long nowMillis() { return System.currentTimeMillis(); }

    /** Random integer in {@code [lo, hi)}. */
    public int randomInt(int lo, int hi) { return ThreadLocalRandom.current().nextInt(lo, hi); }

    /** Random double in {@code [0, 1)}. */
    public double random() { return ThreadLocalRandom.current().nextDouble(); }

    /** Pick a random element from a list. */
    public Object pick(List<?> choices) {
        return choices.get(ThreadLocalRandom.current().nextInt(choices.size()));
    }

    /**
     * Monotonic sequence, unique per {@code name} for the lifetime of this
     * {@code gen} instance. Groovy: {@code gen.next('order_id')}.
     */
    public long next(String name) {
        return counters.computeIfAbsent(name, k -> new AtomicLong()).incrementAndGet();
    }
}
