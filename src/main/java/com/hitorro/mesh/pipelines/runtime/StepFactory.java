/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.StepSpec;

import java.util.Iterator;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compiles a {@link StepSpec} into a function that transforms one row.
 * A {@code null} return drops the row (filter mismatch). Steps are chained
 * by {@code NodeRunner}.
 *
 * <p>Filter/project/rename/set-field are self-contained. Groovy-map and
 * jvssql throw with a clear "Phase 2" message so users get a good error
 * rather than a NullPointerException.</p>
 */
public final class StepFactory {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Filter grammar: {@code <col> <op> <literal>} where op ∈ {==,!=,>,>=,<,<=,LIKE}. */
    private static final Pattern FILTER_EXPR =
            Pattern.compile("\\s*([\\w.]+)\\s*(==|!=|>=|<=|>|<|LIKE)\\s*(.+?)\\s*$",
                            Pattern.CASE_INSENSITIVE);

    private StepFactory() { }

    public static Function<JsonNode, JsonNode> compile(StepSpec spec) {
        throw new IllegalArgumentException("use compile(spec, ctx) — Lookup needs registry access");
    }

    public static Function<JsonNode, JsonNode> compile(StepSpec spec,
            com.hitorro.mesh.pipelines.sinks.SinkRegistry registry) {
        return switch (spec) {
            case StepSpec.Filter    s -> compileFilter(s.expr());
            case StepSpec.Project   s -> compileProject(s.cols());
            case StepSpec.Rename    s -> compileRename(s.from(), s.to());
            case StepSpec.SetField  s -> compileSetField(s.name(), s.value());
            case StepSpec.ToTyped   s -> compileToTyped(s.fields(), s.passthrough());
            case StepSpec.Lookup    s -> compileLookup(s.from(), s.onField(),
                                                       s.withKey(), s.adds(), registry);
            case StepSpec.GroovyMap s -> GroovyMapStep.compile(s.script());
            case StepSpec.Jvssql    s -> throw new UnsupportedOperationException(
                    "jvssql is Phase 2 (add the jvssql adapter)");
        };
    }

    /**
     * Apply an iterator through a chain of step functions. Rows that return
     * {@code null} from any step are dropped; the chain shortcircuits on
     * the first null for a given row.
     */
    public static Iterator<JsonNode> chain(Iterator<JsonNode> src,
                                           java.util.List<Function<JsonNode, JsonNode>> steps) {
        return new Iterator<>() {
            JsonNode next;

            @Override public boolean hasNext() {
                while (next == null && src.hasNext()) {
                    JsonNode row = src.next();
                    for (var step : steps) {
                        row = step.apply(row);
                        if (row == null) break;
                    }
                    next = row;
                }
                return next != null;
            }

            @Override public JsonNode next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                JsonNode out = next; next = null; return out;
            }
        };
    }

    // ------------------------------------------------------------ Filter
    private static Function<JsonNode, JsonNode> compileFilter(String expr) {
        Matcher m = FILTER_EXPR.matcher(expr);
        if (!m.matches()) throw new IllegalArgumentException(
                "filter expression must be `<col> <op> <literal>` — got: " + expr);
        String col   = m.group(1);
        String op    = m.group(2).toUpperCase();
        String rhs   = m.group(3);
        // Strip surrounding single or double quotes for string literals.
        if ((rhs.startsWith("'") && rhs.endsWith("'"))
                || (rhs.startsWith("\"") && rhs.endsWith("\""))) {
            rhs = rhs.substring(1, rhs.length() - 1);
        }
        String literal = rhs;
        Double literalAsNumber;
        try { literalAsNumber = Double.parseDouble(literal); }
        catch (NumberFormatException e) { literalAsNumber = null; }
        Double num = literalAsNumber;

        return row -> {
            JsonNode v = pluck(row, col);
            boolean pass = switch (op) {
                case "==" -> equal(v, literal, num);
                case "!=" -> !equal(v, literal, num);
                case ">"  -> compare(v, num) > 0;
                case ">=" -> compare(v, num) >= 0;
                case "<"  -> compare(v, num) < 0;
                case "<=" -> compare(v, num) <= 0;
                case "LIKE" -> {
                    if (v == null || v.isNull()) yield false;
                    // MySQL-ish: % → .*, _ → .
                    String pat = literal.replace(".", "\\.").replace("%", ".*").replace("_", ".");
                    yield v.asText().matches("(?i)" + pat);
                }
                default -> false;
            };
            return pass ? row : null;
        };
    }

    private static boolean equal(JsonNode v, String literal, Double num) {
        if (v == null || v.isNull()) return "null".equals(literal);
        if (num != null && v.isNumber()) return Double.compare(v.asDouble(), num) == 0;
        return v.asText().equals(literal);
    }

    private static int compare(JsonNode v, Double num) {
        if (v == null || v.isNull() || num == null) return -1;
        return Double.compare(v.asDouble(), num);
    }

    private static JsonNode pluck(JsonNode row, String path) {
        JsonNode cur = row;
        for (String seg : path.split("\\.")) {
            if (cur == null || cur.isNull()) return null;
            cur = cur.get(seg);
        }
        return cur;
    }

    // ------------------------------------------------------------ Project
    private static Function<JsonNode, JsonNode> compileProject(java.util.List<String> cols) {
        return row -> {
            if (!row.isObject()) return row;
            ObjectNode out = JSON.createObjectNode();
            for (String c : cols) {
                JsonNode v = row.get(c);
                if (v != null) out.set(c, v);
            }
            return out;
        };
    }

    // ------------------------------------------------------------ Rename
    private static Function<JsonNode, JsonNode> compileRename(String from, String to) {
        return row -> {
            if (!row.isObject()) return row;
            ObjectNode out = ((ObjectNode) row).deepCopy();
            JsonNode v = out.remove(from);
            if (v != null) out.set(to, v);
            return out;
        };
    }

    // ------------------------------------------------------------ SetField
    private static Function<JsonNode, JsonNode> compileSetField(String name, Object value) {
        JsonNode literal = JSON.valueToTree(value);
        return row -> {
            if (!row.isObject()) return row;
            ObjectNode out = ((ObjectNode) row).deepCopy();
            out.set(name, literal);
            return out;
        };
    }

    // ------------------------------------------------------------ ToTyped
    private static Function<JsonNode, JsonNode> compileToTyped(
            java.util.List<StepSpec.ToTyped.Field> fields, boolean passthrough) {
        return row -> {
            if (!row.isObject()) return null;
            ObjectNode src = (ObjectNode) row;
            ObjectNode out = passthrough ? src.deepCopy() : JSON.createObjectNode();
            for (var f : fields) {
                JsonNode v = src.get(f.name());
                JsonNode coerced = coerce(v, f.type());
                if (coerced == null && v != null && !v.isNull()) {
                    // Coercion failed with a non-null input — drop row.
                    return null;
                }
                out.set(f.name(), coerced == null ? JSON.nullNode() : coerced);
            }
            return out;
        };
    }

    private static JsonNode coerce(JsonNode v, String type) {
        if (v == null || v.isNull()) return null;
        return switch (type) {
            case "core_string" -> JSON.getNodeFactory().textNode(v.asText());
            case "core_long"   -> tryParseLong(v);
            case "core_double" -> tryParseDouble(v);
            case "core_boolean" -> tryParseBool(v);
            case "core_timestamp" -> tryParseTimestamp(v);
            default -> v;   // unknown type → passthrough, don't reject
        };
    }

    private static JsonNode tryParseLong(JsonNode v) {
        try {
            long n = v.isNumber() ? v.asLong() : Long.parseLong(v.asText().trim());
            return JSON.getNodeFactory().numberNode(n);
        } catch (Exception e) { return null; }
    }
    private static JsonNode tryParseDouble(JsonNode v) {
        try {
            double n = v.isNumber() ? v.asDouble() : Double.parseDouble(v.asText().trim());
            return JSON.getNodeFactory().numberNode(n);
        } catch (Exception e) { return null; }
    }
    private static JsonNode tryParseBool(JsonNode v) {
        if (v.isBoolean()) return v;
        String s = v.asText().trim().toLowerCase();
        return switch (s) {
            case "true", "yes", "y", "1"  -> JSON.getNodeFactory().booleanNode(true);
            case "false", "no", "n", "0"  -> JSON.getNodeFactory().booleanNode(false);
            default -> null;
        };
    }
    private static JsonNode tryParseTimestamp(JsonNode v) {
        try {
            if (v.isNumber()) return JSON.getNodeFactory().numberNode(v.asLong());
            long ms = java.time.Instant.parse(v.asText().trim()).toEpochMilli();
            return JSON.getNodeFactory().numberNode(ms);
        } catch (Exception e) { return null; }
    }

    // ------------------------------------------------------------ Lookup
    private static Function<JsonNode, JsonNode> compileLookup(
            String from, String onField, String withKey,
            java.util.List<String> adds,
            com.hitorro.mesh.pipelines.sinks.SinkRegistry registry) {
        // Snapshot the lookup table when the step is first invoked (lazy —
        // upstream node may not have populated it yet at compile time).
        // Then index by onField for O(1) lookups.
        final java.util.Map<String, JsonNode>[] indexRef = new java.util.Map[]{null};
        return row -> {
            if (indexRef[0] == null) {
                java.util.List<JsonNode> src = registry.memoryTable(from);
                java.util.Map<String, JsonNode> idx = new java.util.HashMap<>(src.size());
                for (JsonNode r : src) {
                    JsonNode k = r.get(onField);
                    if (k != null && !k.isNull()) idx.putIfAbsent(k.asText(), r);
                }
                indexRef[0] = idx;
            }
            if (!row.isObject()) return row;
            JsonNode k = pluck(row, withKey);
            if (k == null || k.isNull()) return row;
            JsonNode match = indexRef[0].get(k.asText());
            if (match == null) return row;
            ObjectNode out = ((ObjectNode) row).deepCopy();
            for (String col : adds) {
                JsonNode v = match.get(col);
                if (v != null) out.set(col, v);
            }
            return out;
        };
    }
}
