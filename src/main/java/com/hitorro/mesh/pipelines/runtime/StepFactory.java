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
        return switch (spec) {
            case StepSpec.Filter    s -> compileFilter(s.expr());
            case StepSpec.Project   s -> compileProject(s.cols());
            case StepSpec.Rename    s -> compileRename(s.from(), s.to());
            case StepSpec.SetField  s -> compileSetField(s.name(), s.value());
            case StepSpec.GroovyMap s -> throw new UnsupportedOperationException(
                    "groovy-map is Phase 2 (add the groovy adapter — see docs)");
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
}
