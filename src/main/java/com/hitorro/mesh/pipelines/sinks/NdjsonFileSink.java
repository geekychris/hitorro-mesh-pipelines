/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * Writes NDJSON — one JSON object per line — to a local file. Compression
 * follows the URL extension ({@code .gz} → gzip; anything else → plain).
 * Parent directories are created on first write.
 *
 * <p>Local-file only for Phase 1. The BaseFile adapter (HDFS/S3, bzip2,
 * zstd) lands with the {@code hitorro-mesh-pipelines-basefile} module in
 * the next iteration.</p>
 */
public final class NdjsonFileSink implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String url;
    private final AtomicLong n = new AtomicLong();
    private BufferedWriter writer;

    public NdjsonFileSink(String url) {
        this.url = url;
    }

    @Override
    public void open() throws IOException {
        Path path = resolvePath(url);
        Files.createDirectories(path.getParent());
        if (url.endsWith(".gz")) {
            writer = new BufferedWriter(new java.io.OutputStreamWriter(
                    new GZIPOutputStream(Files.newOutputStream(path)),
                    StandardCharsets.UTF_8));
        } else {
            writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        }
    }

    @Override
    public void add(JsonNode row) throws IOException {
        if (writer == null) open();
        writer.write(JSON.writeValueAsString(row));
        writer.write('\n');
        n.incrementAndGet();
    }

    @Override public long count() { return n.get(); }

    @Override
    public void close() throws IOException {
        if (writer != null) { writer.flush(); writer.close(); }
    }

    private static Path resolvePath(String url) {
        // Accept "file:/...", "file:///..." and bare paths — everything else
        // is Phase 2 (BaseFile adapter).
        if (url.startsWith("file:")) return Path.of(URI.create(url));
        return Path.of(url);
    }
}
