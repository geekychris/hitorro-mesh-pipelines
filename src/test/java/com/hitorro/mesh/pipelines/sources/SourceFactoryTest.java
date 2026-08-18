/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct coverage for {@link SourceFactory}'s built-in source-opening
 * paths. Excludes the network-dependent sources (Nats, Kafka, Sql) —
 * those need running brokers / a driver HTTP server and are exercised
 * through end-to-end integration.
 *
 * <p>Every test drives one open() call, drains the returned iterator,
 * and asserts the resulting rows match the file/inline shape. The
 * iterator's close() semantics (for the CloseableIterator inner class
 * that owns a BufferedReader) get exercised implicitly — a leaked
 * reader would fail parallel tests via file-lock races.</p>
 */
class SourceFactoryTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path home;
    SinkRegistry registry;
    SourceFactory sf;

    @BeforeEach
    void setUp() {
        registry = new SinkRegistry(home);
        sf       = new SourceFactory(registry);
    }

    // -------------------------------------------------- NDJSON

    @Test
    void ndjson_plain_readsAllRows() throws Exception {
        Path f = home.resolve("in.ndjson");
        Files.writeString(f, """
                {"n": 1, "s": "one"}
                {"n": 2, "s": "two"}
                {"n": 3, "s": "three"}
                """);
        List<JsonNode> rows = drain(sf.open(new SourceSpec.NdjsonFile(f.toString())));
        assertThat(rows).hasSize(3);
        assertThat(rows).extracting(r -> r.get("s").asText())
                .containsExactly("one", "two", "three");
    }

    @Test
    void ndjson_skipsBlankLines() throws Exception {
        // NDJSON allows blank separators — the reader must skip them,
        // not emit them as parse failures.
        Path f = home.resolve("in.ndjson");
        Files.writeString(f, """
                {"n": 1}

                {"n": 2}

                {"n": 3}
                """);
        List<JsonNode> rows = drain(sf.open(new SourceSpec.NdjsonFile(f.toString())));
        assertThat(rows).hasSize(3);
    }

    @Test
    void ndjson_gzip_transparentDecompression() throws Exception {
        // .gz suffix → GZIPInputStream wrap on read. The NDJSON writer
        // sink has the mirror behaviour; this test proves the source
        // side matches.
        Path f = home.resolve("in.ndjson.gz");
        try (OutputStream fos = Files.newOutputStream(f);
             GZIPOutputStream gz = new GZIPOutputStream(fos)) {
            gz.write("{\"k\":\"first\"}\n{\"k\":\"second\"}\n"
                    .getBytes(StandardCharsets.UTF_8));
        }
        List<JsonNode> rows = drain(sf.open(new SourceSpec.NdjsonFile(f.toString())));
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.get("k").asText())
                .containsExactly("first", "second");
    }

    @Test
    void ndjson_fileUrl_resolvesToPath() throws Exception {
        // BaseFile-style file: URL scheme — the resolver must strip it.
        Path f = home.resolve("in.ndjson");
        Files.writeString(f, "{\"a\": 1}\n");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.NdjsonFile(f.toUri().toString())));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("a").asInt()).isEqualTo(1);
    }

    @Test
    void ndjson_missingFile_throwsIoException() {
        Path missing = home.resolve("does-not-exist.ndjson");
        assertThatThrownBy(() -> sf.open(new SourceSpec.NdjsonFile(missing.toString())))
                .isInstanceOf(IOException.class);
    }

    // -------------------------------------------------- JSON array

    @Test
    void jsonArray_readsTopLevelArray() throws Exception {
        Path f = home.resolve("in.json");
        Files.writeString(f, "[{\"a\":1},{\"a\":2},{\"a\":3}]");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.JsonFile(f.toString())));
        assertThat(rows).hasSize(3);
        assertThat(rows).extracting(r -> r.get("a").asInt())
                .containsExactly(1, 2, 3);
    }

    @Test
    void jsonArray_topLevelObject_throwsClearError() throws Exception {
        // A JSON object at the top isn't iterable as rows — must give
        // a clear message, not a nondescript ClassCastException later.
        Path f = home.resolve("bad.json");
        Files.writeString(f, "{\"not\":\"an array\"}");
        assertThatThrownBy(() -> sf.open(new SourceSpec.JsonFile(f.toString())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expected top-level array");
    }

    // -------------------------------------------------- CSV

    @Test
    void csv_headerAndRows_flatObjects() throws Exception {
        Path f = home.resolve("in.csv");
        Files.writeString(f, "iso3,name,population\nUSA,United States,331\nCHN,China,1400\n");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.CsvFile(f.toString())));
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get("iso3").asText()).isEqualTo("USA");
        assertThat(rows.get(0).get("population").asText()).isEqualTo("331");
        assertThat(rows.get(1).get("name").asText()).isEqualTo("China");
    }

    @Test
    void csv_quotedField_handlesEmbeddedComma() throws Exception {
        // The splitter's minimal RFC 4180 support — commas inside "..."
        // must not split the field.
        Path f = home.resolve("in.csv");
        Files.writeString(f, "name,city\n\"Doe, John\",\"New York, NY\"\n");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.CsvFile(f.toString())));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("name").asText()).isEqualTo("Doe, John");
        assertThat(rows.get(0).get("city").asText()).isEqualTo("New York, NY");
    }

    @Test
    void csv_escapedDoubleQuote_preservedInField() throws Exception {
        // Two consecutive quotes inside a quoted field → one literal
        // quote (RFC 4180 escape). Matches the NDJSON/CsvFileSink
        // writer's inverse behaviour.
        Path f = home.resolve("in.csv");
        Files.writeString(f, "s\n\"he said \"\"hi\"\"\"\n");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.CsvFile(f.toString())));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("s").asText()).isEqualTo("he said \"hi\"");
    }

    @Test
    void csv_blankLines_skipped() throws Exception {
        Path f = home.resolve("in.csv");
        Files.writeString(f, "n\n1\n\n2\n\n\n3\n");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.CsvFile(f.toString())));
        assertThat(rows).hasSize(3);
    }

    @Test
    void csv_emptyFile_returnsEmptyIterator() throws Exception {
        Path f = home.resolve("empty.csv");
        Files.writeString(f, "");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.CsvFile(f.toString())));
        assertThat(rows).isEmpty();
    }

    @Test
    void csv_headerOnly_returnsEmptyRows() throws Exception {
        Path f = home.resolve("hdr.csv");
        Files.writeString(f, "a,b,c\n");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.CsvFile(f.toString())));
        assertThat(rows).isEmpty();
    }

    @Test
    void csv_fewerCellsThanHeader_omitsMissingKeys() throws Exception {
        // Row shorter than header → only the cells present get written.
        // Prevents a "null cell for missing column" from crashing
        // downstream steps expecting typed values.
        Path f = home.resolve("in.csv");
        Files.writeString(f, "a,b,c\n1,2\n");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.CsvFile(f.toString())));
        assertThat(rows.get(0).has("a")).isTrue();
        assertThat(rows.get(0).has("b")).isTrue();
        assertThat(rows.get(0).has("c")).isFalse();      // missing → omitted
    }

    @Test
    void csv_moreCellsThanHeader_extrasIgnored() throws Exception {
        // Row longer than header → extras dropped (fixed-column semantics).
        Path f = home.resolve("in.csv");
        Files.writeString(f, "a,b\n1,2,3,4\n");
        List<JsonNode> rows = drain(sf.open(new SourceSpec.CsvFile(f.toString())));
        assertThat(rows.get(0).get("a").asText()).isEqualTo("1");
        assertThat(rows.get(0).get("b").asText()).isEqualTo("2");
        assertThat(rows.get(0).size()).isEqualTo(2);
    }

    @Test
    void csv_classpathUrl_readsFromResources() throws Exception {
        // classpath:/ prefix reads from the jar/classes tree, not the
        // filesystem. Requires a test resource — using the shipped
        // example-job.yaml as a "does-it-load" probe (technically it's
        // YAML not CSV but the CSV reader just splits lines by comma,
        // which for the YAML content still returns *something* — the
        // point of this test is proving the classpath URL resolves,
        // not that the content is meaningful CSV).
        Iterator<JsonNode> it = sf.open(
                new SourceSpec.CsvFile("classpath:/example-job.yaml"));
        assertThat(it.hasNext()).isTrue();
    }

    @Test
    void csv_missingClasspathResource_throwsIoException() {
        assertThatThrownBy(() -> sf.open(new SourceSpec.CsvFile("classpath:/no-such-resource.csv")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("no classpath resource");
    }

    // -------------------------------------------------- Inline

    @Test
    void inline_literalRows_convertToJsonNodes() throws Exception {
        List<Map<String, Object>> rows = List.of(
                Map.of("n", 1, "s", "one"),
                Map.of("n", 2, "s", "two"));
        List<JsonNode> out = drain(sf.open(new SourceSpec.Inline(rows)));
        assertThat(out).hasSize(2);
        assertThat(out.get(0).get("s").asText()).isEqualTo("one");
        assertThat(out.get(1).get("n").asInt()).isEqualTo(2);
    }

    @Test
    void inline_empty_returnsEmpty() throws Exception {
        List<JsonNode> out = drain(sf.open(new SourceSpec.Inline(List.of())));
        assertThat(out).isEmpty();
    }

    // -------------------------------------------------- Ref (upstream memory-table)

    @Test
    void ref_readsFromNamedMemoryTable() throws Exception {
        // Simulate an upstream node's MemoryTableSink populating the
        // registry: downstream ref source must see the exact same rows.
        var mem = registry.memoryTable("upstream");
        mem.add(JSON.readTree("{\"x\": 1}"));
        mem.add(JSON.readTree("{\"x\": 2}"));

        List<JsonNode> out = drain(sf.open(new SourceSpec.Ref("upstream")));
        assertThat(out).hasSize(2);
        assertThat(out).extracting(r -> r.get("x").asInt())
                .containsExactly(1, 2);
    }

    @Test
    void ref_missingUpstream_returnsEmpty() throws Exception {
        // Auto-created empty memory-table on lookup — never throws.
        // Prevents a spec typo from crashing the run mid-flight.
        List<JsonNode> out = drain(sf.open(new SourceSpec.Ref("does-not-exist")));
        assertThat(out).isEmpty();
    }

    // -------------------------------------------------- Adapter fallback

    @Test
    void register_customAdapter_isConsultedFirst() throws Exception {
        // Prove the ServiceLoader-shaped SourceAdapter can be added
        // programmatically for tests + intercepts before the built-in
        // switch. Guards against ordering regressions in `open()`.
        boolean[] called = {false};
        sf.register(new SourceAdapter() {
            @Override public boolean handles(SourceSpec spec) {
                return spec instanceof SourceSpec.Inline;
            }
            @Override public Iterator<JsonNode> open(SourceSpec spec, Path home,
                                                     java.util.concurrent.atomic.AtomicBoolean cancelled) {
                called[0] = true;
                return List.<JsonNode>of().iterator();
            }
        });
        sf.open(new SourceSpec.Inline(List.of(Map.of("x", 1))));
        assertThat(called[0]).isTrue();
    }

    // ------------------------------------------------------------ helpers

    private static List<JsonNode> drain(Iterator<JsonNode> it) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        try { while (it.hasNext()) out.add(it.next()); }
        finally { if (it instanceof AutoCloseable c) c.close(); }
        return out;
    }
}
