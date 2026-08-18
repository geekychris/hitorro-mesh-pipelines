/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.AggSpec;
import com.hitorro.mesh.pipelines.model.ReduceSpec;
import com.hitorro.mesh.pipelines.runtime.ReduceEngine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct coverage for the in-process group-by + aggregate engine.
 * Every {@link AggSpec.AggKind} gets a test; correctness of the
 * shuffle-reduce path in {@code PipelineScheduler.dispatchShuffleReduce}
 * reduces to correctness of this engine (mappers shuffle raw rows,
 * reducers run ReduceEngine on the shuffled bucket).
 *
 * <p>The comment on {@code PipelineScheduler.dispatchShuffleReduce}
 * used to claim AVG was broken with shuffle — it wasn't, but nothing
 * proved it. These tests + the accompanying doc fix nail the invariant
 * so future changes to the reduce planner can't silently regress it.</p>
 */
class ReduceEngineTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void count_perGroup() {
        List<JsonNode> rows = rows(
                row("region", "NA", "n", 1),
                row("region", "NA", "n", 2),
                row("region", "EU", "n", 3));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("region"),
                        new AggSpec("cnt", AggSpec.AggKind.COUNT, null))));
        assertThat(out).hasSize(2);
        assertThat(fieldOf(out, "region", "NA").get("cnt").asInt()).isEqualTo(2);
        assertThat(fieldOf(out, "region", "EU").get("cnt").asInt()).isEqualTo(1);
    }

    @Test
    void sum_perGroup() {
        List<JsonNode> rows = rows(
                row("region", "NA", "sales", 10),
                row("region", "NA", "sales", 25),
                row("region", "EU", "sales", 7));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("region"),
                        new AggSpec("total", AggSpec.AggKind.SUM, "sales"))));
        assertThat(fieldOf(out, "region", "NA").get("total").asDouble()).isEqualTo(35.0);
        assertThat(fieldOf(out, "region", "EU").get("total").asDouble()).isEqualTo(7.0);
    }

    @Test
    void avg_isSumDividedByCount_notNaive() {
        // The correctness the whole shuffle-reduce path depends on:
        // AVG must be sum(row.v) / count(row) — NOT the average of any
        // partial averages. Reducers see all raw rows for their bucket,
        // so this is a straight sum/count.
        List<JsonNode> rows = rows(
                row("region", "NA", "v", 10),
                row("region", "NA", "v", 20),
                row("region", "NA", "v", 30),   // avg = 20
                row("region", "EU", "v", 5),
                row("region", "EU", "v", 15));  // avg = 10
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("region"),
                        new AggSpec("avg_v", AggSpec.AggKind.AVG, "v"))));
        assertThat(fieldOf(out, "region", "NA").get("avg_v").asDouble()).isEqualTo(20.0);
        assertThat(fieldOf(out, "region", "EU").get("avg_v").asDouble()).isEqualTo(10.0);
    }

    @Test
    void avg_ignoresNonNumericCells() {
        // Non-number values contribute nothing to the sum but the group
        // still counts every row — so avg = sum(numeric-only) / count(all).
        // This is the existing behaviour (SUM increments only on numbers,
        // count() bumps every row); documenting via test so a future
        // reduce refactor doesn't silently change semantics.
        List<JsonNode> rows = rows(
                row("g", "x", "v", 10),
                row("g", "x", "v", "bogus"),   // dropped from SUM, kept in COUNT
                row("g", "x", "v", 20));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("g"),
                        new AggSpec("avg", AggSpec.AggKind.AVG, "v"))));
        // sum=30, count=3 → 10.0
        assertThat(out.get(0).get("avg").asDouble()).isEqualTo(10.0);
    }

    @Test
    void min_max_perGroup() {
        List<JsonNode> rows = rows(
                row("g", "a", "v", 5),
                row("g", "a", "v", 2),
                row("g", "a", "v", 9),
                row("g", "b", "v", 100));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("g"),
                        new AggSpec("lo", AggSpec.AggKind.MIN, "v"),
                        new AggSpec("hi", AggSpec.AggKind.MAX, "v"))));
        assertThat(fieldOf(out, "g", "a").get("lo").asDouble()).isEqualTo(2.0);
        assertThat(fieldOf(out, "g", "a").get("hi").asDouble()).isEqualTo(9.0);
        assertThat(fieldOf(out, "g", "b").get("lo").asDouble()).isEqualTo(100.0);
        assertThat(fieldOf(out, "g", "b").get("hi").asDouble()).isEqualTo(100.0);
    }

    @Test
    void distinctCount_perGroup() {
        List<JsonNode> rows = rows(
                row("g", "x", "t", "red"),
                row("g", "x", "t", "red"),
                row("g", "x", "t", "blue"),
                row("g", "y", "t", "green"));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("g"),
                        new AggSpec("dt", AggSpec.AggKind.DISTINCT_COUNT, "t"))));
        assertThat(fieldOf(out, "g", "x").get("dt").asInt()).isEqualTo(2);
        assertThat(fieldOf(out, "g", "y").get("dt").asInt()).isEqualTo(1);
    }

    @Test
    void first_last_capturePerGroup() {
        List<JsonNode> rows = rows(
                row("g", "x", "v", 1),
                row("g", "x", "v", 2),
                row("g", "x", "v", 3),   // last(x) = 3
                row("g", "y", "v", 100));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("g"),
                        new AggSpec("first_v", AggSpec.AggKind.FIRST, "v"),
                        new AggSpec("last_v",  AggSpec.AggKind.LAST,  "v"))));
        assertThat(fieldOf(out, "g", "x").get("first_v").asInt()).isEqualTo(1);
        assertThat(fieldOf(out, "g", "x").get("last_v").asInt()).isEqualTo(3);
        assertThat(fieldOf(out, "g", "y").get("first_v").asInt()).isEqualTo(100);
        assertThat(fieldOf(out, "g", "y").get("last_v").asInt()).isEqualTo(100);
    }

    @Test
    void collect_gathersEveryValuePerGroup() {
        List<JsonNode> rows = rows(
                row("g", "a", "t", "one"),
                row("g", "a", "t", "two"),
                row("g", "a", "t", "three"));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("g"),
                        new AggSpec("all", AggSpec.AggKind.COLLECT, "t"))));
        JsonNode collected = out.get(0).get("all");
        assertThat(collected.isArray()).isTrue();
        assertThat(collected.size()).isEqualTo(3);
        List<String> values = new ArrayList<>();
        collected.forEach(v -> values.add(v.asText()));
        assertThat(values).containsExactly("one", "two", "three");
    }

    @Test
    void multipleGroupByCols_composeAsCompositeKey() {
        List<JsonNode> rows = rows(
                row("country", "US", "year", 2024, "n", 1),
                row("country", "US", "year", 2024, "n", 2),
                row("country", "US", "year", 2025, "n", 3),
                row("country", "CA", "year", 2024, "n", 4));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("country", "year"),
                        new AggSpec("total", AggSpec.AggKind.SUM, "n"))));
        assertThat(out).hasSize(3);
        // (US, 2024) → 3, (US, 2025) → 3, (CA, 2024) → 4
        assertThat(pickGroup(out, Map.of("country", "US", "year", "2024"))
                .get("total").asDouble()).isEqualTo(3.0);
        assertThat(pickGroup(out, Map.of("country", "US", "year", "2025"))
                .get("total").asDouble()).isEqualTo(3.0);
        assertThat(pickGroup(out, Map.of("country", "CA", "year", "2024"))
                .get("total").asDouble()).isEqualTo(4.0);
    }

    @Test
    void emptySource_emitsNothing() {
        var out = drain(ReduceEngine.reduce(List.<JsonNode>of().iterator(),
                spec(List.of("g"), new AggSpec("c", AggSpec.AggKind.COUNT, null))));
        assertThat(out).isEmpty();
    }

    @Test
    void nullGroupByValue_collapsesToNullBucket() {
        // A row with a null value in a group-by column groups with other
        // rows that have null for the same column — proves the "<null>"
        // sentinel is consistent, not per-row-random.
        List<JsonNode> rows = new ArrayList<>();
        rows.add(row("g", null, "n", 1));
        rows.add(row("g", null, "n", 2));
        rows.add(row("g", "A",  "n", 5));
        var out = drain(ReduceEngine.reduce(rows.iterator(),
                spec(List.of("g"), new AggSpec("total", AggSpec.AggKind.SUM, "n"))));
        assertThat(out).hasSize(2);
        assertThat(fieldOf(out, "g", "<null>").get("total").asDouble()).isEqualTo(3.0);
        assertThat(fieldOf(out, "g", "A").get("total").asDouble()).isEqualTo(5.0);
    }

    // ------------------------------------------------------------ helpers

    private static List<JsonNode> drain(Iterator<JsonNode> it) {
        List<JsonNode> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }

    private static ReduceSpec spec(List<String> groupBy, AggSpec... aggs) {
        return new ReduceSpec(groupBy, List.of(aggs));
    }

    private static List<JsonNode> rows(JsonNode... rs) {
        List<JsonNode> out = new ArrayList<>(rs.length);
        for (JsonNode r : rs) out.add(r);
        return out;
    }

    /** Build a row from alternating key/value pairs. Number values are
     *  inserted as JSON numbers; null values as JSON null; everything
     *  else as strings. */
    private static JsonNode row(Object... kv) {
        ObjectNode n = JSON.createObjectNode();
        for (int i = 0; i < kv.length; i += 2) {
            String k = (String) kv[i];
            Object v = kv[i + 1];
            if (v == null) n.putNull(k);
            else if (v instanceof Integer x) n.put(k, x);
            else if (v instanceof Double x)  n.put(k, x);
            else n.put(k, String.valueOf(v));
        }
        return n;
    }

    private static JsonNode fieldOf(List<JsonNode> out, String key, String val) {
        for (JsonNode r : out) if (val.equals(r.get(key).asText())) return r;
        throw new AssertionError("no row with " + key + "=" + val);
    }

    private static JsonNode pickGroup(List<JsonNode> out, Map<String, String> criteria) {
        outer:
        for (JsonNode r : out) {
            for (var e : criteria.entrySet()) {
                if (!e.getValue().equals(r.get(e.getKey()).asText())) continue outer;
            }
            return r;
        }
        throw new AssertionError("no row matching " + criteria);
    }
}
