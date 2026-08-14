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
 * Phase-1 stub for KV writes: appends {@code <key>\t<row-json>} to a file
 * under {@code ${hitorro.pipelines.home}/kv/<name>/entries.tsv} so tests
 * can assert the physical output. The full RocksDB-backed sink lands with
 * the {@code hitorro-mesh-pipelines-kvstore} adapter module which pulls
 * in {@code hitorro-kvstore}.
 *
 * <p>The keyExpr is a dotted path into the row (e.g. {@code "iso3"},
 * {@code "user.id"}). Missing values fall back to {@code "<null>"}.</p>
 */
public final class KvStoreSink implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String name;
    private final String keyExpr;
    private final Path file;
    private final AtomicLong n = new AtomicLong();
    private BufferedWriter writer;

    public KvStoreSink(String name, String keyExpr, Path homeDir) {
        this.name    = name;
        this.keyExpr = keyExpr == null ? "id" : keyExpr;
        this.file    = homeDir.resolve("kv").resolve(name).resolve("entries.tsv");
    }

    @Override
    public void open() throws IOException {
        Files.createDirectories(file.getParent());
        writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
    }

    @Override
    public void add(JsonNode row) throws IOException {
        if (writer == null) open();
        String key = extractKey(row, keyExpr);
        writer.write(key);
        writer.write('\t');
        writer.write(JSON.writeValueAsString(row));
        writer.write('\n');
        n.incrementAndGet();
    }

    @Override public long count() { return n.get(); }

    @Override
    public void close() throws IOException {
        if (writer != null) { writer.flush(); writer.close(); }
    }

    public String name() { return name; }
    public Path file()   { return file; }

    private static String extractKey(JsonNode row, String path) {
        JsonNode cur = row;
        for (String seg : path.split("\\.")) {
            if (cur == null || cur.isNull()) return "<null>";
            cur = cur.get(seg);
        }
        return (cur == null || cur.isNull()) ? "<null>" : cur.asText();
    }
}
