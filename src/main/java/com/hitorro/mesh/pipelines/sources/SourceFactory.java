/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

/**
 * Opens a {@link SourceSpec} into an {@code Iterator<JsonNode>}. Callers
 * are expected to drain and close the iterator (via try-with-resources
 * when it implements {@link AutoCloseable}).
 *
 * <p>The {@code cancelled} flag is passed to streaming sources (NATS,
 * Kafka) so they can break out of blocking polls cleanly when
 * {@code DELETE /mesh/jobs/{id}} fires.</p>
 */
public final class SourceFactory {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final SinkRegistry sinkRegistry;
    private final List<SourceAdapter> adapters = new ArrayList<>();

    public SourceFactory(SinkRegistry sinkRegistry) {
        this.sinkRegistry = sinkRegistry;
        for (SourceAdapter a : java.util.ServiceLoader.load(SourceAdapter.class)) adapters.add(a);
    }

    /** Register a source adapter programmatically (mainly for tests). */
    public void register(SourceAdapter adapter) {
        adapters.add(adapter);
    }

    public Iterator<JsonNode> open(SourceSpec spec) throws IOException {
        return open(spec, new AtomicBoolean());
    }

    public Iterator<JsonNode> open(SourceSpec spec, AtomicBoolean cancelled) throws IOException {
        for (SourceAdapter a : adapters) {
            if (a.handles(spec)) {
                try {
                    return a.open(spec, sinkRegistry.home(), cancelled);
                } catch (Exception e) {
                    throw new IOException("source adapter " + a.getClass().getSimpleName()
                            + " failed: " + e.getMessage(), e);
                }
            }
        }
        return switch (spec) {
            case SourceSpec.NdjsonFile s -> openNdjson(s.url());
            case SourceSpec.JsonFile   s -> openJsonArray(s.url());
            case SourceSpec.CsvFile    s -> openCsv(s.url());
            case SourceSpec.Inline     s -> openInline(s.rows());
            case SourceSpec.Ref        s -> openRef(s.node());
            case SourceSpec.Nats       s -> openNats(s.servers(), s.subject(), cancelled);
            case SourceSpec.Kafka      s -> openKafka(s.bootstrap(), s.topic(), s.groupId(), cancelled);
            case SourceSpec.KvStore    s -> throw new UnsupportedOperationException(
                    "kvstore source is Phase 2 (needs the kvstore adapter)");
            case SourceSpec.Lucene     s -> throw new UnsupportedOperationException(
                    "lucene source is Phase 2 (needs the lucene adapter)");
            case SourceSpec.Sql        s -> throw new UnsupportedOperationException(
                    "sql source is Phase 2 (needs the jvssql adapter)");
        };
    }

    // ------------------------------------------------------------ NATS (streaming)
    private Iterator<JsonNode> openNats(String servers, String subject, AtomicBoolean cancelled) throws IOException {
        try {
            return new NatsSource(servers, subject, cancelled);
        } catch (NoClassDefFoundError e) {
            throw new IOException("nats source needs io.nats:jnats on the classpath — add the optional dep", e);
        } catch (Exception e) {
            throw new IOException("failed to connect to NATS: " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------ Kafka (streaming)
    private Iterator<JsonNode> openKafka(String bootstrap, String topic, String groupId,
                                          AtomicBoolean cancelled) throws IOException {
        if (groupId == null || groupId.isBlank()) {
            throw new IOException("kafka source requires groupId");
        }
        try {
            return new KafkaSourceImpl(bootstrap, topic, groupId, cancelled);
        } catch (NoClassDefFoundError e) {
            throw new IOException("kafka source needs org.apache.kafka:kafka-clients on the classpath", e);
        }
    }

    // ------------------------------------------------------------ NDJSON
    private Iterator<JsonNode> openNdjson(String url) throws IOException {
        Path path = resolvePath(url);
        BufferedReader br = url.endsWith(".gz")
                ? new BufferedReader(new java.io.InputStreamReader(
                        new GZIPInputStream(Files.newInputStream(path)),
                        StandardCharsets.UTF_8))
                : Files.newBufferedReader(path, StandardCharsets.UTF_8);
        return new CloseableIterator() {
            String next;

            @Override public boolean hasNext() {
                if (next != null) return true;
                try {
                    while ((next = br.readLine()) != null) {
                        if (!next.isBlank()) return true;
                    }
                } catch (IOException e) { throw new RuntimeException(e); }
                return false;
            }

            @Override public JsonNode next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                try { return JSON.readTree(next); }
                catch (IOException e) { throw new RuntimeException(e); }
                finally { next = null; }
            }

            @Override public void close() throws Exception { br.close(); }
        };
    }

    // ------------------------------------------------------------ JSON array
    private Iterator<JsonNode> openJsonArray(String url) throws IOException {
        JsonNode tree = JSON.readTree(resolvePath(url).toFile());
        if (!tree.isArray()) throw new IOException("expected top-level array in " + url);
        List<JsonNode> rows = new ArrayList<>(tree.size());
        tree.forEach(rows::add);
        return rows.iterator();
    }

    // ------------------------------------------------------------ CSV
    private Iterator<JsonNode> openCsv(String url) throws IOException {
        List<String> lines;
        if (url.startsWith("classpath:")) {
            String cp = url.substring("classpath:".length());
            try (var in = SourceFactory.class.getResourceAsStream(cp)) {
                if (in == null) throw new IOException("no classpath resource: " + cp);
                lines = new java.io.BufferedReader(
                        new java.io.InputStreamReader(in, StandardCharsets.UTF_8))
                        .lines().toList();
            }
        } else {
            Path path = resolvePath(url);
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        }
        if (lines.isEmpty()) return java.util.Collections.emptyIterator();
        String[] header = splitCsvRow(lines.get(0));
        List<JsonNode> rows = new ArrayList<>(lines.size() - 1);
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) continue;
            String[] cells = splitCsvRow(line);
            ObjectNode row = JSON.createObjectNode();
            for (int c = 0; c < header.length && c < cells.length; c++) {
                row.put(header[c], cells[c]);
            }
            rows.add(row);
        }
        return rows.iterator();
    }

    // ------------------------------------------------------------ Inline literal
    private Iterator<JsonNode> openInline(List<java.util.Map<String, Object>> rows) {
        List<JsonNode> out = new ArrayList<>(rows.size());
        for (var r : rows) out.add(JSON.valueToTree(r));
        return out.iterator();
    }

    // ------------------------------------------------------------ Ref (upstream)
    private Iterator<JsonNode> openRef(String node) {
        // Phase 1: every node writes to a synchronised memory table named
        // after its id (via MemoryTableSink). Downstream reads it here.
        // Phase 2 adds file-backed edges when sinks are more diverse.
        return sinkRegistry.memoryTable(node).iterator();
    }

    // ------------------------------------------------------------ Helpers
    private static Path resolvePath(String url) {
        if (url.startsWith("file:")) return Path.of(URI.create(url));
        return Path.of(url);
    }

    /** Minimal CSV split — handles simple quoted fields, no embedded newlines. */
    private static String[] splitCsvRow(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuote && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"'); i++;
                } else inQuote = !inQuote;
            } else if (c == ',' && !inQuote) {
                out.add(cur.toString()); cur.setLength(0);
            } else cur.append(c);
        }
        out.add(cur.toString());
        return out.toArray(new String[0]);
    }

    /** Small marker for iterators that own a closeable resource. */
    interface CloseableIterator extends Iterator<JsonNode>, AutoCloseable { }
}
