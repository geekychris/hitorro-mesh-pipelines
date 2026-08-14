/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.AggSpec;
import com.hitorro.mesh.pipelines.model.ReduceSpec;

import java.util.*;
import java.util.stream.Collectors;

/**
 * In-process group-by + aggregate. Buffers the upstream rows, folds them
 * into groups keyed by concatenated {@code groupBy} values, then emits
 * one row per group with the requested aggregates.
 *
 * <p>Phase 2 pushes group-by down to {@code JvsSqlEngine} for streaming
 * larger row sets and adds distributed reduction via mesh shuffle. The
 * shape of {@link #reduce} does not change — callers see the same
 * iterator either way.</p>
 */
public final class ReduceEngine {

    private static final ObjectMapper JSON = new ObjectMapper();

    private ReduceEngine() { }

    public static Iterator<JsonNode> reduce(Iterator<JsonNode> src, ReduceSpec spec) {
        // Use a LinkedHashMap so groups emit in first-seen order — makes
        // tests deterministic.
        Map<List<String>, Accumulator> byGroup = new LinkedHashMap<>();

        while (src.hasNext()) {
            JsonNode row = src.next();
            List<String> key = new ArrayList<>(spec.groupBy().size());
            for (String c : spec.groupBy()) {
                JsonNode v = row.get(c);
                key.add(v == null || v.isNull() ? "<null>" : v.asText());
            }
            byGroup.computeIfAbsent(key, k -> new Accumulator(spec.aggs())).add(row);
        }

        List<JsonNode> out = new ArrayList<>(byGroup.size());
        for (var e : byGroup.entrySet()) {
            ObjectNode outRow = JSON.createObjectNode();
            List<String> keyVals = e.getKey();
            for (int i = 0; i < spec.groupBy().size(); i++) {
                outRow.put(spec.groupBy().get(i), keyVals.get(i));
            }
            e.getValue().emitInto(outRow);
            out.add(outRow);
        }
        return out.iterator();
    }

    // ---------------------------------------------------------- accumulator
    private static final class Accumulator {
        final List<AggSpec> specs;
        long count;
        final double[] sum;
        final double[] min;
        final double[] max;
        final Set<String>[] distinct;
        JsonNode[] first;
        JsonNode[] last;
        final List<Object>[] collected;

        @SuppressWarnings("unchecked")
        Accumulator(List<AggSpec> aggs) {
            this.specs    = aggs;
            this.sum      = new double[aggs.size()];
            this.min      = new double[aggs.size()];
            this.max      = new double[aggs.size()];
            this.distinct = new Set[aggs.size()];
            this.first    = new JsonNode[aggs.size()];
            this.last     = new JsonNode[aggs.size()];
            this.collected = new List[aggs.size()];
            Arrays.fill(min, Double.POSITIVE_INFINITY);
            Arrays.fill(max, Double.NEGATIVE_INFINITY);
            for (int i = 0; i < aggs.size(); i++) {
                if (aggs.get(i).kind() == AggSpec.AggKind.DISTINCT_COUNT) distinct[i] = new HashSet<>();
                if (aggs.get(i).kind() == AggSpec.AggKind.COLLECT)        collected[i] = new ArrayList<>();
            }
        }

        void add(JsonNode row) {
            count++;
            for (int i = 0; i < specs.size(); i++) {
                AggSpec a = specs.get(i);
                if (a.kind() == AggSpec.AggKind.COUNT) continue;
                JsonNode v = a.of() == null ? null : row.get(a.of());
                switch (a.kind()) {
                    case SUM, AVG -> { if (v != null && v.isNumber()) sum[i] += v.asDouble(); }
                    case MIN -> { if (v != null && v.isNumber()) min[i] = Math.min(min[i], v.asDouble()); }
                    case MAX -> { if (v != null && v.isNumber()) max[i] = Math.max(max[i], v.asDouble()); }
                    case DISTINCT_COUNT -> { if (v != null && !v.isNull()) distinct[i].add(v.asText()); }
                    case FIRST -> { if (first[i] == null) first[i] = v; }
                    case LAST  -> { last[i] = v; }
                    case COLLECT -> { if (v != null && !v.isNull()) collected[i].add(v.isNumber() ? v.asDouble() : v.asText()); }
                    case COUNT -> { /* handled by outer counter */ }
                }
            }
        }

        void emitInto(ObjectNode out) {
            for (int i = 0; i < specs.size(); i++) {
                AggSpec a = specs.get(i);
                switch (a.kind()) {
                    case COUNT -> out.put(a.name(), count);
                    case SUM   -> out.put(a.name(), sum[i]);
                    case AVG   -> out.put(a.name(), count == 0 ? 0.0 : sum[i] / count);
                    case MIN   -> out.put(a.name(), min[i] == Double.POSITIVE_INFINITY ? 0 : min[i]);
                    case MAX   -> out.put(a.name(), max[i] == Double.NEGATIVE_INFINITY ? 0 : max[i]);
                    case DISTINCT_COUNT -> out.put(a.name(), distinct[i].size());
                    case FIRST -> out.set(a.name(), first[i]);
                    case LAST  -> out.set(a.name(), last[i]);
                    case COLLECT -> out.set(a.name(),
                            JSON.valueToTree(collected[i].stream().collect(Collectors.toList())));
                }
            }
        }
    }
}
