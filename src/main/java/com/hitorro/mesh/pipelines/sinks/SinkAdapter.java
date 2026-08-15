/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.hitorro.mesh.pipelines.model.SinkSpec;

import java.nio.file.Path;

/**
 * Plugin point for sink implementations that live in optional adapter
 * modules (real RocksDB KV, real Lucene index, S3-backed NDJSON, ...).
 * Registered with a {@link SinkRegistry} at construction time and
 * consulted before the built-in stubs.
 *
 * <p>Adapters are loaded via {@link java.util.ServiceLoader}. Sub-modules
 * ship a {@code META-INF/services/com.hitorro.mesh.pipelines.sinks.SinkAdapter}
 * pointing at their impl.</p>
 */
public interface SinkAdapter {

    /**
     * Return true if this adapter wants to handle the given spec kind
     * (e.g. {@code KvStore}, {@code Lucene}, {@code CsvFile}).
     */
    boolean handles(SinkSpec spec);

    /**
     * Build the sink. Called once per node run; caller closes.
     *
     * @param home  base directory for adapter-owned data (adapter picks
     *              a subdirectory keyed on the sink name)
     */
    Sink create(SinkSpec spec, Path home);
}
