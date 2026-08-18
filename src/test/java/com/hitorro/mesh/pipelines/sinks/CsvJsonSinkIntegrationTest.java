/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.util.core.iterator.sinks.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wires {@link SinkSpec.CsvFile} / {@link SinkSpec.JsonFile} through the
 * {@link SinkRegistry} — proves the "Phase 2 stub" throw has been
 * replaced with a real implementation and that the spec fields
 * ({@code cols}, {@code pretty}) flow through correctly.
 */
class CsvJsonSinkIntegrationTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void csvFile_writesHeaderAndRows_viaRegistry(@TempDir Path tmp) throws Exception {
        Path out = tmp.resolve("countries.csv");
        SinkRegistry reg = new SinkRegistry(tmp);
        Sink<JsonNode> sink = reg.create(new SinkSpec.CsvFile(out.toString(),
                List.of("iso3", "name", "population")));
        sink.start();
        sink.add(row("iso3", "USA", "name", "United States", "population", "331000000"));
        sink.add(row("iso3", "CHN", "name", "China",         "population", "1400000000"));
        sink.close();

        assertThat(Files.readAllLines(out)).containsExactly(
                "iso3,name,population",
                "USA,United States,331000000",
                "CHN,China,1400000000");
    }

    @Test
    void jsonFile_prettyFlag_flowsThrough(@TempDir Path tmp) throws Exception {
        Path compact = tmp.resolve("compact.json");
        Path pretty  = tmp.resolve("pretty.json");
        SinkRegistry reg = new SinkRegistry(tmp);

        try (Sink<JsonNode> s = reg.create(new SinkSpec.JsonFile(compact.toString(), false))) {
            s.start();
            s.add(row("k", "v"));
        }
        try (Sink<JsonNode> s = reg.create(new SinkSpec.JsonFile(pretty.toString(), true))) {
            s.start();
            s.add(row("k", "v"));
        }
        // Pretty file must be larger; content must round-trip to the same tree.
        assertThat(Files.size(pretty)).isGreaterThan(Files.size(compact));
        assertThat(JSON.readTree(pretty.toFile())).isEqualTo(JSON.readTree(compact.toFile()));
    }

    @Test
    void csvFile_partOfDecoratorChain(@TempDir Path tmp) throws Exception {
        // Prove the decorator hook fires for csv-file sinks too, not just NDJSON.
        Path out = tmp.resolve("via-decorator.csv");
        SinkRegistry reg = new SinkRegistry(tmp);
        boolean[] wrapped = {false};
        reg.registerDecorator((spec, base) -> {
            if (spec instanceof SinkSpec.CsvFile) wrapped[0] = true;
            return base;
        });
        Sink<JsonNode> sink = reg.create(new SinkSpec.CsvFile(out.toString(), List.of("a")));
        sink.start();
        sink.add(row("a", "1"));
        sink.close();

        assertThat(wrapped[0]).isTrue();
        assertThat(Files.readAllLines(out)).containsExactly("a", "1");
    }

    private ObjectNode row(String... kv) {
        ObjectNode n = JSON.createObjectNode();
        for (int i = 0; i < kv.length; i += 2) n.put(kv[i], kv[i + 1]);
        return n;
    }
}
