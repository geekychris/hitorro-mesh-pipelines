/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.SourceSpec;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plugin point for source implementations that live in optional adapter
 * modules (RocksDB KV scan, Lucene search, S3 NDJSON, ...). Discovered
 * via {@link java.util.ServiceLoader}; consulted before the built-in
 * switch in {@link SourceFactory}.
 */
public interface SourceAdapter {

    /** Return true if this adapter handles the given spec kind. */
    boolean handles(SourceSpec spec);

    /**
     * Open the source into an iterator. Streaming sources should honour
     * the {@code cancelled} flag in their poll loops so
     * {@code DELETE /mesh/jobs/{id}} closes them promptly.
     *
     * @param home  base directory for adapter-owned data (mirrors the
     *              adapter's SinkAdapter home so sink→source round-trips
     *              in the same JVM read the right files)
     */
    Iterator<JsonNode> open(SourceSpec spec, Path home, AtomicBoolean cancelled) throws Exception;
}
