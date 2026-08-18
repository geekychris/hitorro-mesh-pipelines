/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.StepSpec;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct coverage of {@link StepFactory#compile} for the built-in
 * step kinds. The reduce path is covered by {@link ReduceEngineTest};
 * the Groovy step by {@link com.hitorro.mesh.pipelines.GroovyMapStepTest};
 * the JVS adapters by the -jvstype module. This suite fills the remaining
 * surface: filter grammar + project + rename + set-field + ToTyped
 * coercion + Lookup enrichment.
 *
 * <p>Every step is compiled → applied to synthetic rows → the returned
 * row is asserted. Null returns are the filter/drop signal in the step
 * contract — tests assert both accept and drop paths where relevant.</p>
 */
class StepFactoryTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path home;
    SinkRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SinkRegistry(home);
    }

    // -------------------------------------------------- Filter grammar

    @Test
    void filter_stringEquality_bothQuoteStyles() {
        // Single-quoted, double-quoted, and unquoted-literal all work
        // for string RHS.
        Function<JsonNode, JsonNode> byCountry = compile(new StepSpec.Filter("country == 'US'"));
        assertThat(byCountry.apply(row("country", "US")))
                .isNotNull();
        assertThat(byCountry.apply(row("country", "GB")))
                .isNull();

        Function<JsonNode, JsonNode> dq = compile(new StepSpec.Filter("country == \"US\""));
        assertThat(dq.apply(row("country", "US"))).isNotNull();
    }

    @Test
    void filter_notEquals_matchesInverse() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.Filter("status != 'draft'"));
        assertThat(f.apply(row("status", "published"))).isNotNull();
        assertThat(f.apply(row("status", "draft"))).isNull();
    }

    @Test
    void filter_numericComparators_all4Directions() {
        Function<JsonNode, JsonNode> gt  = compile(new StepSpec.Filter("age > 18"));
        Function<JsonNode, JsonNode> gte = compile(new StepSpec.Filter("age >= 18"));
        Function<JsonNode, JsonNode> lt  = compile(new StepSpec.Filter("age < 18"));
        Function<JsonNode, JsonNode> lte = compile(new StepSpec.Filter("age <= 18"));

        assertThat(gt.apply(row("age", 19))).isNotNull();
        assertThat(gt.apply(row("age", 18))).isNull();
        assertThat(gte.apply(row("age", 18))).isNotNull();
        assertThat(lt.apply(row("age", 17))).isNotNull();
        assertThat(lt.apply(row("age", 18))).isNull();
        assertThat(lte.apply(row("age", 18))).isNotNull();
    }

    @Test
    void filter_like_matchesPercentAndUnderscore() {
        // MySQL-ish LIKE: % → .*, _ → .
        Function<JsonNode, JsonNode> starts = compile(new StepSpec.Filter("email LIKE 'admin%'"));
        assertThat(starts.apply(row("email", "admin@example.com"))).isNotNull();
        assertThat(starts.apply(row("email", "guest@example.com"))).isNull();

        Function<JsonNode, JsonNode> contains = compile(new StepSpec.Filter("topics LIKE '%ai%'"));
        assertThat(contains.apply(row("topics", "machine learning ai research"))).isNotNull();
        assertThat(contains.apply(row("topics", "web design"))).isNull();

        Function<JsonNode, JsonNode> underscoreOne = compile(new StepSpec.Filter("code LIKE 'A_C'"));
        assertThat(underscoreOne.apply(row("code", "ABC"))).isNotNull();
        assertThat(underscoreOne.apply(row("code", "ABBC"))).isNull();
    }

    @Test
    void filter_like_isCaseInsensitive() {
        // (?i) prefix in the regex — documenting the observable behaviour.
        Function<JsonNode, JsonNode> f = compile(new StepSpec.Filter("name LIKE '%SMITH%'"));
        assertThat(f.apply(row("name", "john smith"))).isNotNull();
        assertThat(f.apply(row("name", "JOHN SMITH"))).isNotNull();
    }

    @Test
    void filter_like_nullCell_dropsRow() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.Filter("email LIKE '%@%'"));
        assertThat(f.apply(row("other", "x"))).isNull();     // missing field
    }

    @Test
    void filter_dottedPath_walksNested() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.Filter("user.age > 18"));
        ObjectNode nested = JSON.createObjectNode();
        nested.putObject("user").put("age", 21);
        assertThat(f.apply(nested)).isNotNull();
    }

    @Test
    void filter_badGrammar_throwsAtCompile() {
        assertThatThrownBy(() -> compile(new StepSpec.Filter("this is not a filter")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -------------------------------------------------- Project

    @Test
    void project_keepsOnlyNamedFields() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.Project(List.of("a", "b")));
        ObjectNode in = JSON.createObjectNode();
        in.put("a", 1); in.put("b", 2); in.put("c", 3); in.put("d", 4);
        JsonNode out = f.apply(in);
        assertThat(out.fieldNames()).toIterable().containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void project_missingCol_omitsInsteadOfPuttingNull() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.Project(List.of("a", "missing")));
        JsonNode out = f.apply(row("a", "kept"));
        assertThat(out.has("a")).isTrue();
        assertThat(out.has("missing")).isFalse();     // omitted, not null
    }

    // -------------------------------------------------- Rename

    @Test
    void rename_movesField_keepsRest() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.Rename("from_name", "to_name"));
        ObjectNode in = JSON.createObjectNode();
        in.put("from_name", "value");
        in.put("other", "kept");
        JsonNode out = f.apply(in);
        assertThat(out.has("from_name")).isFalse();
        assertThat(out.get("to_name").asText()).isEqualTo("value");
        assertThat(out.get("other").asText()).isEqualTo("kept");
    }

    @Test
    void rename_missingField_isNoOp() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.Rename("nope", "target"));
        JsonNode out = f.apply(row("other", "x"));
        assertThat(out.has("target")).isFalse();
        assertThat(out.get("other").asText()).isEqualTo("x");
    }

    // -------------------------------------------------- SetField

    @Test
    void setField_addsConstantString() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.SetField("source", "pipeline"));
        JsonNode out = f.apply(row("id", "u-1"));
        assertThat(out.get("source").asText()).isEqualTo("pipeline");
        assertThat(out.get("id").asText()).isEqualTo("u-1");
    }

    @Test
    void setField_addsNumericValue_asJsonNumber() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.SetField("version", 3));
        JsonNode out = f.apply(row("id", "u-1"));
        assertThat(out.get("version").isNumber()).isTrue();
        assertThat(out.get("version").asInt()).isEqualTo(3);
    }

    @Test
    void setField_overwritesExisting() {
        Function<JsonNode, JsonNode> f = compile(new StepSpec.SetField("status", "processed"));
        ObjectNode in = JSON.createObjectNode();
        in.put("status", "draft");
        JsonNode out = f.apply(in);
        assertThat(out.get("status").asText()).isEqualTo("processed");
    }

    // -------------------------------------------------- ToTyped coercion

    @Test
    void toTyped_coerces5PrimitiveTypes() {
        var fields = List.of(
                new StepSpec.ToTyped.Field("s", "core_string"),
                new StepSpec.ToTyped.Field("n", "core_long"),
                new StepSpec.ToTyped.Field("d", "core_double"),
                new StepSpec.ToTyped.Field("b", "core_boolean"),
                new StepSpec.ToTyped.Field("t", "core_timestamp"));
        Function<JsonNode, JsonNode> f = compile(new StepSpec.ToTyped(fields, false));

        ObjectNode in = JSON.createObjectNode();
        in.put("s", 42);                                 // number → string
        in.put("n", "123");                              // string → long
        in.put("d", "3.14");                             // string → double
        in.put("b", "yes");                              // yes → true
        in.put("t", "2025-01-01T00:00:00Z");             // ISO → epoch ms

        JsonNode out = f.apply(in);
        assertThat(out.get("s").isTextual()).isTrue();
        assertThat(out.get("s").asText()).isEqualTo("42");
        assertThat(out.get("n").asLong()).isEqualTo(123L);
        assertThat(out.get("d").asDouble()).isEqualTo(3.14);
        assertThat(out.get("b").asBoolean()).isTrue();
        // Timestamp becomes epoch ms.
        assertThat(out.get("t").asLong()).isEqualTo(1735689600000L);
    }

    @Test
    void toTyped_passthroughFalse_dropsUnschemaFields() {
        var fields = List.of(new StepSpec.ToTyped.Field("keep", "core_string"));
        Function<JsonNode, JsonNode> f = compile(new StepSpec.ToTyped(fields, false));

        ObjectNode in = JSON.createObjectNode();
        in.put("keep", "yes");
        in.put("drop_me", "no");

        JsonNode out = f.apply(in);
        assertThat(out.has("keep")).isTrue();
        assertThat(out.has("drop_me")).isFalse();
    }

    @Test
    void toTyped_passthroughTrue_preservesExtras() {
        var fields = List.of(new StepSpec.ToTyped.Field("n", "core_long"));
        Function<JsonNode, JsonNode> f = compile(new StepSpec.ToTyped(fields, true));

        ObjectNode in = JSON.createObjectNode();
        in.put("n", "42");
        in.put("extra", "kept");

        JsonNode out = f.apply(in);
        assertThat(out.get("n").asLong()).isEqualTo(42L);
        assertThat(out.get("extra").asText()).isEqualTo("kept");
    }

    @Test
    void toTyped_arrayForm_coercesEveryElement() {
        var fields = List.of(new StepSpec.ToTyped.Field("tags", "array<core_string>"));
        Function<JsonNode, JsonNode> f = compile(new StepSpec.ToTyped(fields, false));

        // From real array.
        ObjectNode inArr = JSON.createObjectNode();
        inArr.putArray("tags").add(1).add(2).add(3);
        JsonNode outArr = f.apply(inArr);
        assertThat(outArr.get("tags").isArray()).isTrue();
        assertThat(outArr.get("tags").size()).isEqualTo(3);
        assertThat(outArr.get("tags").get(0).isTextual()).isTrue();

        // From comma-separated string.
        JsonNode outCsv = f.apply(row("tags", "a, b, c"));
        assertThat(outCsv.get("tags").isArray()).isTrue();
        assertThat(outCsv.get("tags").size()).isEqualTo(3);
        assertThat(outCsv.get("tags").get(1).asText()).isEqualTo("b");
    }

    @Test
    void toTyped_failedCoercionOnNonNullInput_dropsRow() {
        // Documented behaviour: coercion returns null on non-null input
        // that can't parse → row is dropped (filter-shaped).
        var fields = List.of(new StepSpec.ToTyped.Field("n", "core_long"));
        Function<JsonNode, JsonNode> f = compile(new StepSpec.ToTyped(fields, false));
        JsonNode out = f.apply(row("n", "not-a-number"));
        assertThat(out).isNull();
    }

    @Test
    void toTyped_boolean_recognisesEveryTruthySynonym() {
        var fields = List.of(new StepSpec.ToTyped.Field("b", "core_boolean"));
        Function<JsonNode, JsonNode> f = compile(new StepSpec.ToTyped(fields, false));

        // Every value the coercer accepts as true.
        for (String truthy : List.of("true", "yes", "y", "1", "TRUE", "Yes")) {
            assertThat(f.apply(row("b", truthy)).get("b").asBoolean())
                    .as("truthy: " + truthy).isTrue();
        }
        // ... and every falsey.
        for (String falsey : List.of("false", "no", "n", "0", "FALSE", "No")) {
            assertThat(f.apply(row("b", falsey)).get("b").asBoolean())
                    .as("falsey: " + falsey).isFalse();
        }
    }

    @Test
    void toTyped_dottedPath_readsAndWritesNested() {
        // A path "user.email" is walked into the source and reconstructed
        // on the target, creating intermediate object nodes as needed.
        var fields = List.of(
                new StepSpec.ToTyped.Field("user.name", "core_string"),
                new StepSpec.ToTyped.Field("user.age",  "core_long"));
        Function<JsonNode, JsonNode> f = compile(new StepSpec.ToTyped(fields, false));

        ObjectNode in = JSON.createObjectNode();
        in.putObject("user").put("name", "Alice").put("age", "30");

        JsonNode out = f.apply(in);
        assertThat(out.get("user").get("name").asText()).isEqualTo("Alice");
        assertThat(out.get("user").get("age").asLong()).isEqualTo(30L);
    }

    // -------------------------------------------------- Lookup

    @Test
    void lookup_enrichesFromMemoryTable() {
        // Populate an upstream "countries" memory-table, then a lookup
        // step that adds "country_name" from it based on iso3.
        var lookup = registry.memoryTable("countries");
        lookup.add(row("iso3", "USA", "country_name", "United States"));
        lookup.add(row("iso3", "GBR", "country_name", "United Kingdom"));

        Function<JsonNode, JsonNode> f = StepFactory.compile(
                new StepSpec.Lookup("countries", "iso3", "iso3",
                                    List.of("country_name")),
                registry);

        JsonNode out = f.apply(row("iso3", "USA", "product", "widget"));
        assertThat(out.get("country_name").asText()).isEqualTo("United States");
        assertThat(out.get("product").asText()).isEqualTo("widget");
    }

    @Test
    void lookup_missingKey_passesRowThroughUnchanged() {
        // Row has no match → returns the row as-is (broadcast enrichment,
        // not a filtering JOIN — the row should still flow).
        var lookup = registry.memoryTable("countries");
        lookup.add(row("iso3", "USA", "country_name", "United States"));

        Function<JsonNode, JsonNode> f = StepFactory.compile(
                new StepSpec.Lookup("countries", "iso3", "iso3",
                                    List.of("country_name")),
                registry);

        JsonNode out = f.apply(row("iso3", "ZZZ", "product", "gizmo"));
        assertThat(out.get("iso3").asText()).isEqualTo("ZZZ");
        assertThat(out.has("country_name")).isFalse();
    }

    @Test
    void lookup_emptyTable_stillReturnsRow() {
        Function<JsonNode, JsonNode> f = StepFactory.compile(
                new StepSpec.Lookup("empty", "k", "k", List.of("x")),
                registry);
        JsonNode out = f.apply(row("k", "a"));
        assertThat(out.get("k").asText()).isEqualTo("a");
    }

    // -------------------------------------------------- compile() dispatch

    @Test
    void compile_noRegistry_throwsForNonLookupSteps() {
        // The 1-arg overload is a guard against callers who need a
        // registry — makes the error clear rather than NPE on Lookup.
        assertThatThrownBy(() -> StepFactory.compile(new StepSpec.Filter("x == 1")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -------------------------------------------------- chain semantics

    @Test
    void chain_appliesStepsInOrder_dropsOnAnyNull() {
        // Filter drops row → later steps don't touch it.
        Function<JsonNode, JsonNode> filter = compile(new StepSpec.Filter("n > 5"));
        Function<JsonNode, JsonNode> setter = compile(new StepSpec.SetField("seen", true));

        java.util.List<JsonNode> src = new java.util.ArrayList<>();
        src.add(row("n", 10));
        src.add(row("n", 1));    // drops on filter
        src.add(row("n", 20));

        java.util.Iterator<JsonNode> out = StepFactory.chain(src.iterator(),
                List.of(filter, setter));
        java.util.List<JsonNode> results = new java.util.ArrayList<>();
        out.forEachRemaining(results::add);
        assertThat(results).hasSize(2);
        // Each surviving row got the setter applied.
        assertThat(results).allSatisfy(r ->
                assertThat(r.get("seen").asBoolean()).isTrue());
    }

    // ------------------------------------------------------------ helpers

    private Function<JsonNode, JsonNode> compile(StepSpec spec) {
        return StepFactory.compile(spec, registry);
    }

    /** Alternating (String key, Object value) pairs → ObjectNode. */
    private static ObjectNode row(Object... kv) {
        ObjectNode n = JSON.createObjectNode();
        for (int i = 0; i < kv.length; i += 2) {
            String k = (String) kv[i];
            Object v = kv[i + 1];
            if (v == null)                    n.putNull(k);
            else if (v instanceof Integer x)  n.put(k, x);
            else if (v instanceof Boolean x)  n.put(k, x);
            else if (v instanceof Double  x)  n.put(k, x);
            else                              n.put(k, String.valueOf(v));
        }
        return n;
    }
}
