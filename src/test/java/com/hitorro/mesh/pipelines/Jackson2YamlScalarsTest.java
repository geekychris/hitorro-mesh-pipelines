/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines;

import tools.jackson.databind.*; import tools.jackson.databind.node.*;
import java.io.*; import java.nio.file.*; import java.util.*;
/**
 * Pins YAML scalar typing of {@link com.hitorro.mesh.pipelines.parse.Jackson2Yaml} to Jackson 2.21's
 * {@code new YAMLMapper()}: about 120 scalars (YAML 1.1 booleans, nulls, hex/octal/binary/base-60
 * ints, underscores, floats, .inf/.nan, timestamps, merge keys, oddities) as plain, flow, quoted and
 * map-key values and under every explicit tag, plus whole documents and bean binding.
 * {@code jackson2-yaml-scalars.txt} is the output of the same code with Jackson 2.21.8
 * (generator: ~/hitorro-j3/tools/Jackson2YamlScalarsGolden.java.txt in the migration workspace).
 * {@code jackson3-yaml-lexer.txt} pins the known remaining differences, which are in the YAML
 * lexer rather than scalar typing (snakeyaml-engine is YAML 1.2): NEL/U+2028/U+2029 are no longer
 * line breaks, and a lone {@code -} in a flow sequence is a syntax error.
 */
class Jackson2YamlScalarsTest {
  static final Map<String, String> out = new LinkedHashMap<>();
  static void p(String k, Object v){ out.put(k, String.valueOf(v).replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r")); }
  static boolean jackson(Throwable t){ return t.getClass().getName().startsWith("com.fasterxml.jackson") || t.getClass().getName().startsWith("tools.jackson"); }
  static String tryIt(java.util.concurrent.Callable<Object> c){ try { return String.valueOf(c.call()); } catch(Throwable t){ return "!"+(jackson(t) ? "jackson" : t.getClass().getSimpleName()); } }
  static String node(JsonNode n){ if (n == null) return "null"; if (n.isArray() || n.isObject()) { StringBuilder sb = new StringBuilder(n.isArray() ? "[" : "{"); for (Iterator<Map.Entry<String, JsonNode>> it = n.properties().iterator(); it.hasNext();) { Map.Entry<String, JsonNode> e = it.next(); sb.append(e.getKey()).append('=').append(node(e.getValue())).append(','); }
      if (n.isArray()) for (JsonNode e : n) sb.append(node(e)).append(','); return sb.append(n.isArray() ? "]" : "}").toString(); }
    return n.getClass().getSimpleName().replace("StringNode", "TextNode") + "(" + n + ")"; }
  public static final String[] SCALARS = {"yes", "Yes", "YES", "no", "No", "NO", "on", "On", "ON", "off", "Off", "OFF", "y", "Y", "n", "N", "true", "True", "TRUE", "false", "False", "FALSE", "tRue", "yEs",
    "~", "null", "Null", "NULL", "nULL", "", "0", "-0", "+0", "007", "08", "0o17", "-0o17", "+0O7", "0o", "0x1F", "0X1F", "0x_1F", "-0x1F", "0b101", "0b_1_0", "-0b1", "1_000", "1_000_000", "+12", "-12", "12345678901", "12345678901234567890",
    "123456789012345678901234567890", "1:30", "190:20:30", "1:30.5", "1.5", "1.10", ".5", "-.5", "+.5", "5.", "1e3", "1E3", "1.5e3", "1e+3", "1e-3", "1_0.5_0", ".inf", "-.inf", "+.inf", ".Inf", ".INF", ".nan", ".NaN", ".NAN", "inf", "nan",
    "2026-01-02", "2026-1-2 3:04:05", "2026-01-02T03:04:05Z", "2026-01-02T03:04:05.123+01:00", "12:30", "<<", "=", "-", "+", ".", "0x", "0b", "1__2", "_1", "1_", "a1", "1a", "0.0.1", "1,000", "$1", "#x", "  padded  ",
    "\u00e9", "\u2028", "a\u0085b", "a\u2029b", "-1.5e-10", "00", "0_7", "09", "0o8", "1e", "e3", "0x1G", "1e400", "-1e400", "4.9e-324", "123456789012345678901.5"};
  public static Map<String, String> compute(Path dir) throws Exception {
    out.clear();
    ObjectMapper y = com.hitorro.mesh.pipelines.parse.Jackson2Yaml.builder().build();
    for (int i = 0; i < SCALARS.length; i++) { final String s = SCALARS[i]; final String k = i + ":" + s.replace("=", "(eq)");
      p("plain." + k, tryIt(() -> node(y.readTree("v: " + s + "\n").get("v"))));
      p("flow." + k, tryIt(() -> node(y.readTree("[" + s + "]").get(0))));
      p("single." + k, tryIt(() -> node(y.readTree("v: '" + s + "'\n").get("v"))));
      p("double." + k, tryIt(() -> node(y.readTree("v: \"" + s + "\"\n").get("v"))));
      p("key." + k, tryIt(() -> { JsonNode kn = y.readTree("{" + s + ": 1}"); Iterator<String> it = kn.propertyNames().iterator(); return it.hasNext() ? "[" + it.next() + "]" : "none"; }));
      for (String tag : new String[]{"!!bool", "!!int", "!!float", "!!null", "!!str", "!!binary", "!!timestamp", "!custom", "!"})
        p("tag." + tag + "." + k, tryIt(() -> node(y.readTree("v: " + tag + " " + s + "\n").get("v"))));
    }
    String[][] docs = {{"empty", ""}, {"comment", "# c\n"}, {"nullDoc", "~"}, {"scalarDoc", "42"}, {"multi", "a: 1\n---\nb: 2\n"}, {"anchor", "a: &x {k: yes}\nb: *x\n"}, {"merge", "a: &x {k: 1}\nb: {<<: *x, j: on}\n"},
      {"block", "v: |\n  yes\n  0x1F\n"}, {"folded", "v: >\n  a\n  b\n"}, {"nestedFlow", "{a: [yes, ~, 0x1F, {b: off}]}"}, {"dupKey", "a: 1\na: 2\n"}, {"tabs", "a:\t1\n"}, {"complexKey", "? [a, b]\n: 1\n"},
      {"bad", "a: [\n"}, {"badIndent", "a:\n  b: 1\n c: 2\n"}, {"longNum", "v: " + "9".repeat(1025) + "\n"}, {"longFloat", "v: " + "9".repeat(1023) + ".5\n"}};
    for (String[] d : docs) p("doc." + d[0], tryIt(() -> node(y.readTree(d[1]))));
    // typed binding: what a bean field gets
    p("bind.map", tryIt(() -> new TreeMap<>(y.readValue("a: yes\nb: 0x1F\nc: ~\nd: .5\ne: 1_000\nf: 2026-01-02\ng: 1:30\nh: '007'\ni: 12345678901234567890\nj: 1.10\n", Map.class)).toString()));
    p("bind.list", tryIt(() -> y.readValue("[on, off, y, n, 0b11, 017, .inf, -.inf, .nan, null]", List.class).toString()));
    return out;
  }
  static Map<String, String> load(String name) throws IOException {
    Map<String, String> m = new LinkedHashMap<>();
    try (InputStream in = Jackson2YamlScalarsTest.class.getResourceAsStream(name)) {
      if (in == null) return m;
      for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
        if (line.isEmpty()) continue;
        int i = line.indexOf('=');
        m.put(line.substring(0, i), line.substring(i + 1));
      }
    }
    return m;
  }

  @org.junit.jupiter.api.Test
  void matchesJackson2Output(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
    Map<String, String> expected = load("jackson2-yaml-scalars.txt");
    // Deliberate 4.0.0 differences (see the PR): these keys are pinned to the 4.0.0 output.
    Map<String, String> overrides = load("jackson3-yaml-lexer.txt");
    org.assertj.core.api.Assertions.assertThat(overrides).isNotEmpty();
    org.assertj.core.api.Assertions.assertThat(expected).containsKeys(overrides.keySet().toArray(new String[0]));
    expected.putAll(overrides);
    Map<String, String> actual = new LinkedHashMap<>();
    compute(dir).forEach((k, v) -> actual.put(k, v.replace("StringNode", "TextNode")));
    org.assertj.core.api.Assertions.assertThat(expected).hasSize(1643);
    org.assertj.core.api.Assertions.assertThat(actual).containsExactlyEntriesOf(expected);
  }
}

