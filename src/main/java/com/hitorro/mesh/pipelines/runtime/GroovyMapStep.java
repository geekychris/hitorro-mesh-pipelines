/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Compiles a Groovy transform script into a per-row {@code Function}.
 * The script runs in a fresh {@link Binding} each row with:
 *
 * <ul>
 *   <li>{@code row} — the incoming row as a {@code LinkedHashMap<String,Object>}</li>
 *   <li>{@code gen} — an instance of {@link GroovyGenerators} (uuid, now, sequences, …)</li>
 * </ul>
 *
 * The script's return value is the emitted row:
 * <ul>
 *   <li>a {@code Map} → serialised to JSON and passed downstream</li>
 *   <li>{@code null} → row dropped (filter behaviour)</li>
 *   <li>the modified {@code row} binding (default when the script ends with a mutation) → passed as-is</li>
 * </ul>
 *
 * <p>Compile cost is amortised — the script class is parsed once at
 * pipeline start and cached. Only the Binding is fresh per row.</p>
 */
public final class GroovyMapStep {

    private static final ObjectMapper JSON = new ObjectMapper();

    private GroovyMapStep() { }

    /** Compile a groovy-map script into a row-transform function. */
    public static Function<JsonNode, JsonNode> compile(String script) {
        // Cache the parsed Script class — reused for every row.
        final Script parsed;
        final GroovyGenerators gen;
        try {
            parsed = new GroovyShell().parse(script);
            gen    = new GroovyGenerators();
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    "groovy-map needs org.apache.groovy:groovy on the classpath — add the optional dep", e);
        }
        return row -> {
            if (!row.isObject()) return null;
            // Jackson → LinkedHashMap so the script sees a normal Groovy map.
            Map<String, Object> rowMap = JSON.convertValue(row,
                    new com.fasterxml.jackson.core.type.TypeReference<LinkedHashMap<String, Object>>() { });
            Binding b = new Binding();
            b.setVariable("row", rowMap);
            b.setVariable("gen", gen);
            parsed.setBinding(b);
            Object result;
            try {
                result = parsed.run();
            } catch (Exception e) {
                throw new RuntimeException("groovy-map script error: " + e.getMessage(), e);
            }
            if (result == null) return null;
            if (result instanceof Closure<?> c) return null;    // stray closure body
            // A Map return replaces the row; anything else (bool/etc) means
            // "keep the (possibly mutated) row" — return the binding's row.
            Object emit = (result instanceof Map) ? result : rowMap;
            return JSON.valueToTree(emit);
        };
    }
}
