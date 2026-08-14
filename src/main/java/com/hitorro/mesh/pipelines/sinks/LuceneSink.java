/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase-1 stub for Lucene indexing: writes each row as NDJSON under
 * {@code ${hitorro.pipelines.home}/lucene/<name>/docs.ndjson}, plus a
 * marker file describing whether _source retention is on. Real
 * {@code JVSLuceneIndexWriter}-backed sink lands with the
 * {@code hitorro-mesh-pipelines-lucene} adapter module.
 *
 * <p>The stub is shaped so tests can assert "documents landed at
 * name={name}, storeSource={bool}" without requiring the ~40 MB Lucene
 * dependency at this layer.</p>
 */
public final class LuceneSink implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String name;
    private final boolean storeSource;
    private final Path docsFile;
    private final Path markerFile;
    private final AtomicLong n = new AtomicLong();
    private BufferedWriter writer;

    public LuceneSink(String name, boolean storeSource, Path homeDir) {
        this.name        = name;
        this.storeSource = storeSource;
        Path dir = homeDir.resolve("lucene").resolve(name);
        this.docsFile    = dir.resolve("docs.ndjson");
        this.markerFile  = dir.resolve("INDEX-CONFIG");
    }

    @Override
    public void open() throws IOException {
        Files.createDirectories(docsFile.getParent());
        writer = Files.newBufferedWriter(docsFile, StandardCharsets.UTF_8);
        Files.writeString(markerFile,
                "name=" + name + "\nstoreSource=" + storeSource + "\n",
                StandardCharsets.UTF_8);
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

    public String name()         { return name; }
    public boolean storeSource() { return storeSource; }
    public Path indexDir()       { return docsFile.getParent(); }
}
