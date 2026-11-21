/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines;

import tools.jackson.databind.*; import tools.jackson.databind.node.*;
import com.hitorro.mesh.pipelines.model.*; import com.hitorro.mesh.pipelines.parse.JobSpecYaml; import com.hitorro.mesh.pipelines.runtime.*;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry; import com.hitorro.mesh.pipelines.schedule.*;
import com.hitorro.util.persist.JsonFileEntityStore;
import java.io.*; import java.nio.charset.StandardCharsets; import java.nio.file.*; import java.util.*;
/**
 * Pins what hitorro-mesh-pipelines reads, writes and sends to what 3.1.0 did on Jackson 2.21:
 * job specs parsed from YAML/JSON and re-emitted (bundled jobs, schedule templates, unknown
 * properties/kinds, YAML scalar typing, anchors), local job runs over inline rows through every
 * step kind (filter, project, rename, set-field, to-typed, groovy-map) and reduce aggregation into
 * ndjson/json/csv/memory sinks, sources reading ndjson/json/csv files, the runtime stores
 * (JobHistoryStore, RestartableJobStore, SinkLocationRegistry files and reading odd or corrupt
 * ones) and Schedule persistence through hitorro-util's JsonFileEntityStore.
 * {@code jackson2-pipelines.txt} is the output of the same code run on the 3.1.0 sources
 * (generator: ~/hitorro-j3/tools/Jackson2MeshPipelinesGolden.java.txt in the migration workspace).
 * Run ids, instants and durations are masked.
 */
class Jackson2FormatGoldenTest {
  static final Map<String, String> out = new LinkedHashMap<>();
  static void p(String k, Object v){ out.put(k, String.valueOf(v).replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r")); }
  static boolean jackson(Throwable t){ return t.getClass().getName().startsWith("com.fasterxml.jackson") || t.getClass().getName().startsWith("tools.jackson"); }
  static String desc(Throwable t, boolean top){ boolean ioe = t instanceof IOException;
    if (t.getClass() == IOException.class && t.getCause() != null && jackson(t.getCause())) t = t.getCause();
    String s = (top ? (ioe ? "IOE:" : "RTE:") : "") + (jackson(t) ? "jackson" : t.getClass().getSimpleName());
    if (!jackson(t) && t.getCause() != null) s += "<" + desc(t.getCause(), false) + ">"; return s; }
  static String tryIt(java.util.concurrent.Callable<Object> c){ try { return String.valueOf(c.call()); } catch(Throwable t){ return "!"+desc(t, true); } }
  static String types(JsonNode n){ StringBuilder t=new StringBuilder(); if (n == null) return "null"; if (n.isArray()) { t.append('['); for (JsonNode e : n) t.append(types(e)).append(','); return t.append(']').toString(); }
    if (!n.isObject()) return n.getClass().getSimpleName(); t.append('{'); Iterator<String> it=n.propertyNames().iterator(); while(it.hasNext()){String k=it.next(); t.append(k+":"+types(n.get(k))+",");} return t.append('}').toString(); }
  // run-dependent values: ids, instants, durations
  static String mask(String s){ return s.replaceAll("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d(\\.\\d+)?Z", "<T>").replaceAll("job-\\d+(-\\d+)?", "job-<N>").replaceAll("\"durationMs\":\\d+", "\"durationMs\":<D>"); }
  static String res(String path) throws IOException { try (InputStream in = Jackson2FormatGoldenTest.class.getResourceAsStream(path)) { return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8); } }
  static String files(Path d) throws IOException { if (!Files.exists(d)) return "-"; StringBuilder sb = new StringBuilder();
    try (java.util.stream.Stream<Path> s = Files.walk(d)) { for (Path f : (Iterable<Path>) s.filter(Files::isRegularFile).sorted()::iterator) sb.append(d.relativize(f)).append("=[").append(new String(Files.readAllBytes(f), StandardCharsets.UTF_8)).append("] "); }
    return sb.toString(); }
  static final String ROWS = "[{id: 1, name: \"Alpha\", pop: 1500000000, score: 1.10, flag: true, tags: [a, b], meta: {k: v}, region: Asia, when: \"2026-01-02T03:04:05Z\", n: null},"
    + " {id: 2, name: \"Beta/\u00e9\", pop: \"12\", score: -0.0, flag: \"yes\", region: Europe, when: 1700000000123, big: 12345678901234567890},"
    + " {id: \"3\", name: null, pop: 7.9, score: 1e30, flag: 0, region: Asia, when: \"bad\", dec: 123456789012345678901234567890.5},"
    + " {id: 4, name: \"Gamma, Inc\", pop: 50, region: \"\", nested: {a: [1, {b: 2}]}, flag: false}]";
  public static Map<String, String> compute(Path dir) throws Exception {
    out.clear();
    ObjectMapper om = com.hitorro.util.json.JsonMappers.json();
    // ---- 1. job specs: parse YAML/JSON, re-emit (REST API returns this)
    String[] bundled = {"distributed-reduce-shuffle", "airports-typed-enrich", "airports-groovy", "enrich-and-index", "enriched-articles", "kv-read", "distributed-reduce-pattern",
      "countries-triple-sink", "nats-publisher", "kv-write", "nats-echo", "distributed-fanout"};
    for (String b : bundled) p("spec.bundled." + b, tryIt(() -> JobSpecYaml.toJson(JobSpecYaml.parse(res("/examples/jobs/" + b + ".yaml")))));
    for (String t : new String[]{"kafka-events-to-index", "mac-mail-hourly-rowid", "mac-mail-hourly-time"}) p("spec.template." + t, tryIt(() -> res("/schedule-templates/" + t + ".yaml")));
    String[][] specs = {
      {"unknownProp", "job: u\nextra: 1\nnodes:\n  - id: a\n    zzz: 2\n    pipeline: {source: {kind: inline, rows: [{x: 1}], what: 3}, sinks: [{kind: counting, label: n}]}\n"},
      {"unknownKind", "job: u\nnodes:\n  - id: a\n    pipeline: {source: {kind: nope}, sinks: []}\n"}, {"noKind", "job: u\nnodes:\n  - id: a\n    pipeline: {source: {rows: []}, sinks: []}\n"},
      {"malformedYaml", "job: [\n"}, {"json", "{\"job\":\"j\",\"nodes\":[{\"id\":\"a\",\"pipeline\":{\"source\":{\"kind\":\"inline\",\"rows\":[{\"x\":1.10,\"y\":[1,\"2\"]}]},\"steps\":[{\"kind\":\"set-field\",\"name\":\"z\",\"value\":{\"q\":1}}],\"sinks\":[{\"kind\":\"json-file\",\"url\":\"o.json\",\"pretty\":true}]}}]}"},
      {"jsonTrailing", "{\"job\":\"j\",\"nodes\":[]} {\"x\":1}"}, {"jsonMalformed", "{\"job\":"}, {"empty", ""}, {"yamlTypes", "job: t\nnodes:\n  - id: a\n    pipeline:\n      source: {kind: inline, rows: [{a: yes, b: 0x1F, c: 1_000, d: 2026-01-02, e: ~, f: .5, g: '007', h: 1e3, i: on}]}\n      sinks: [{kind: counting, label: n}]\n"},
      {"anchors", "job: t\ndefs: &s {kind: counting, label: n}\nnodes:\n  - id: a\n    pipeline: {source: {kind: inline, rows: []}, sinks: [*s]}\n"},
      {"reduceAll", "job: r\nnodes:\n  - id: a\n    pipeline:\n      source: {kind: inline, rows: []}\n      reduce: {groupBy: [region], aggs: [{name: n, kind: COUNT}, {name: s, kind: SUM, of: pop}, {name: av, kind: AVG, of: pop}, {name: mi, kind: MIN, of: pop}, {name: ma, kind: MAX, of: pop}, {name: dc, kind: DISTINCT_COUNT, of: name}, {name: f, kind: FIRST, of: name}, {name: l, kind: LAST, of: name}, {name: c, kind: COLLECT, of: pop}]}\n      sinks: [{kind: memory-table, name: m}]\n"},
      {"toTyped", "job: t\nnodes:\n  - id: a\n    pipeline:\n      source: {kind: inline, rows: []}\n      steps: [{kind: to-typed, passthrough: false, fields: [{name: id, type: core_string, role: id}, {name: pop, type: core_long}, {name: score, type: core_double}, {name: flag, type: core_boolean}, {name: when, type: core_timestamp}, {name: tags, type: core_string}]}]\n      sinks: [{kind: counting, label: n}]\n"}};
    for (String[] s : specs) p("spec.parse." + s[0], tryIt(() -> JobSpecYaml.toJson(JobSpecYaml.parse(s[1]))));
    // ---- 2. local runs: inline rows -> steps/reduce -> file + memory sinks
    String sinks = "      sinks:\n        - {kind: ndjson-file, url: \"OUT/a.ndjson\"}\n        - {kind: json-file, url: \"OUT/b.json\", pretty: true}\n        - {kind: json-file, url: \"OUT/c.json\", pretty: false}\n        - {kind: csv-file, url: \"OUT/e.csv\", cols: [id, name, pop, meta]}\n        - {kind: memory-table, name: mem}\n        - {kind: counting, label: cnt}\n";
    String[][] jobs = {
      {"passthrough", ""},
      {"filters", "      steps:\n        - {kind: filter, expr: \"pop > 10\"}\n        - {kind: filter, expr: \"region != Europe\"}\n"},
      {"filterLike", "      steps:\n        - {kind: filter, expr: \"name LIKE a\"}\n"},
      {"filterEq", "      steps:\n        - {kind: filter, expr: \"id == 1\"}\n"},
      {"project", "      steps:\n        - {kind: project, cols: [id, meta, tags, missing]}\n        - {kind: rename, from: meta, to: m2}\n"},
      {"setField", "      steps:\n        - {kind: set-field, name: s, value: \"x\"}\n        - {kind: set-field, name: n2, value: 1.50}\n        - {kind: set-field, name: o, value: {a: [1, true, null]}}\n        - {kind: set-field, name: region, value: null}\n"},
      {"toTyped", "      steps:\n        - {kind: to-typed, passthrough: true, fields: [{name: id, type: core_string, role: id}, {name: pop, type: core_long}, {name: score, type: core_double}, {name: flag, type: core_boolean}, {name: when, type: core_timestamp}, {name: tags, type: core_string}, {name: meta, type: core_long}]}\n"},
      {"groovy", "      steps:\n        - kind: groovy-map\n          script: |\n            row.len = (row.name ?: '').length()\n            row.popType = row.pop?.getClass()?.simpleName\n            row.big2 = row.big\n            row.list = [1, 2.5, 'x', null, [k: 'v']]\n            row.remove('meta')\n            if (row.id == 4) return null\n            true\n"},
      {"groovyReplace", "      steps:\n        - kind: groovy-map\n          script: \"[id: row.id, decType: row.dec?.getClass()?.simpleName, when: row.when, d: 0.1 + 0.2, l: 12345678901L, bd: 1.10G]\"\n"},
      {"reduce", "      reduce: {groupBy: [region], aggs: [{name: n, kind: COUNT}, {name: s, kind: SUM, of: pop}, {name: av, kind: AVG, of: pop}, {name: mi, kind: MIN, of: pop}, {name: ma, kind: MAX, of: pop}, {name: dc, kind: DISTINCT_COUNT, of: name}, {name: f, kind: FIRST, of: name}, {name: l, kind: LAST, of: name}, {name: c, kind: COLLECT, of: pop}]}\n"}};
    for (String[] j : jobs) {
      Path home = dir.resolve("run-" + j[0]); Files.createDirectories(home);
      String yaml = ("job: " + j[0] + "\nnodes:\n  - id: n1\n    pipeline:\n      source: {kind: inline, rows: " + ROWS + "}\n" + j[1] + sinks).replace("OUT", home.toString());
      p("run." + j[0] + ".status", tryIt(() -> { SinkRegistry reg = new SinkRegistry(home); JobStatus st; try (JobRunner r = new JobRunner(reg)) { st = r.run(JobSpecYaml.parse(yaml)); }
        StringBuilder sb = new StringBuilder(st.state + " err=" + (st.error == null ? null : st.error.replace(home.toString(), "HOME")));
        for (Map.Entry<String, JobStatus.NodeStatus> e : st.nodes().entrySet()) sb.append(" ").append(e.getKey()).append(":").append(e.getValue().state).append(":").append(e.getValue().rowsIn).append("/").append(e.getValue().rowsOut);
        sb.append(" mem=");
        for (JsonNode n : reg.memoryTable("mem")) sb.append(n).append(' ').append(types(n)).append(" | ");
        return sb; }));
      p("run." + j[0] + ".files", tryIt(() -> files(home).replace(home.toString(), "HOME")));
    }
    // a two-node DAG through ref
    { Path home = dir.resolve("dag"); Files.createDirectories(home);
      String yaml = ("job: dag\nnodes:\n  - id: a\n    pipeline:\n      source: {kind: inline, rows: " + ROWS + "}\n      sinks: [{kind: memory-table, name: am}]\n  - id: b\n    depends: [a]\n    pipeline:\n      source: {kind: ref, node: a}\n      steps: [{kind: project, cols: [id, pop]}]\n      sinks: [{kind: ndjson-file, url: \"OUT/b.ndjson\"}]\n").replace("OUT", home.toString());
      p("run.dag", tryIt(() -> { try (JobRunner r = new JobRunner(new SinkRegistry(home))) { JobStatus st = r.run(JobSpecYaml.parse(yaml)); return st.state + " " + files(home).replace(home.toString(), "HOME"); } })); }
    // sources read from files 3.x wrote
    { Path home = dir.resolve("src"); Files.createDirectories(home);
      Files.write(home.resolve("in.ndjson"), "{\"a\":1,\"b\":1.10}\n\n{\"a\":\"x\",\"c\":[1,{\"d\":null}]}\nnot json\n{\"big\":12345678901234567890}\n".getBytes(StandardCharsets.UTF_8));
      Files.write(home.resolve("in.json"), "[{\"a\":1},{\"a\":2.5,\"b\":\"\u00e9\"}]".getBytes(StandardCharsets.UTF_8));
      Files.write(home.resolve("in.csv"), "id,name,pop\n1,\"A, B\",12\n2,,\n".getBytes(StandardCharsets.UTF_8));
      for (String k : new String[]{"ndjson-file:in.ndjson", "json-file:in.json", "csv-file:in.csv"}) { String[] kk = k.split(":");
        String yaml = "job: s\nnodes:\n  - id: n\n    pipeline:\n      source: {kind: " + kk[0] + ", url: \"" + home.resolve(kk[1]) + "\"}\n      sinks: [{kind: memory-table, name: m}]\n";
        p("source." + kk[0], tryIt(() -> { SinkRegistry reg = new SinkRegistry(home); try (JobRunner r = new JobRunner(reg)) { JobStatus st = r.run(JobSpecYaml.parse(yaml));
          StringBuilder sb = new StringBuilder(st.state + " "); for (JsonNode n : reg.memoryTable("m")) sb.append(n).append(' ').append(types(n)).append(" | "); return sb; } })); } }
    // ---- 3. runtime stores (files on disk)
    { Path f = dir.resolve("stores/history.ndjson");
      JobHistoryStore hs = new JobHistoryStore(f);
      JobStatus s1 = new JobStatus("job-1", "spec \u00e9"); s1.state = JobStatus.State.SUCCEEDED; s1.node("a").rowsIn = 5; s1.node("a").rowsOut = 3; s1.finishedAt = s1.startedAt.plusMillis(42); s1.restartable = true; s1.lifecycle = "scheduled";
      JobStatus s2 = new JobStatus("job-2", "two"); s2.state = JobStatus.State.FAILED; s2.error = "boom \"quoted\"\nline";
      hs.append(s1); hs.append(s2);
      p("history.file", mask(new String(Files.readAllBytes(f), StandardCharsets.UTF_8)));
      Files.write(f, "{bad\n\n{\"jobId\":\"x\",\"state\":\"S\",\"extra\":1}\n{\"jobId\":\"y\",\"durationMs\":null,\"nodes\":null}\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
      p("history.tail", tryIt(() -> mask(new JobHistoryStore(f).tail(10).toString())));
      p("status.snapshot", tryIt(() -> mask(om.writeValueAsString(s1.snapshot()))));
      p("status.events.history", tryIt(() -> JobSpecYaml.toJson(JobSpecYaml.parse("job: x\nnodes: []\n")))); }
    { Path f = dir.resolve("stores/restart.json"); RestartableJobStore rs = new RestartableJobStore(f);
      JobSpec spec = JobSpecYaml.parse("job: r\nrestartable: true\nnodes:\n  - id: a\n    pipeline: {source: {kind: inline, rows: [{x: 1}]}, sinks: [{kind: counting, label: n}]}\n");
      rs.record("job-1", spec, "job: r\n", "yaml"); rs.record("job-2", spec, "{\"job\":\"r\"}", "json"); rs.remove("job-2"); rs.record("job-3", spec, null, null);
      p("restart.file", tryIt(() -> mask(new String(Files.readAllBytes(f), StandardCharsets.UTF_8))));
      p("restart.reload", tryIt(() -> mask(new RestartableJobStore(f).pending().toString())));
      p("restart.resume", tryIt(() -> { List<String> l = new ArrayList<>(); new RestartableJobStore(f).resumeAll((id, sp) -> l.add(id + ":" + sp.id())); return l; }));
      Files.write(f, "{\"job-9\":{\"jobId\":\"job-9\",\"extra\":true}} trailing".getBytes(StandardCharsets.UTF_8));
      p("restart.reloadOdd", tryIt(() -> mask(new RestartableJobStore(f).pending().toString())));
      Files.write(f, "{bad".getBytes(StandardCharsets.UTF_8));
      p("restart.reloadCorrupt", tryIt(() -> new RestartableJobStore(f).size())); }
    { Path f = dir.resolve("stores/sinks.json"); SinkLocationRegistry sl = new SinkLocationRegistry(f);
      sl.record("kvstore", "b", "agent-2"); sl.record("lucene", "a", "agent-1"); sl.record("kvstore", "b", "agent-3");
      p("sinkloc.file", new String(Files.readAllBytes(f), StandardCharsets.UTF_8));
      p("sinkloc.reload", tryIt(() -> { SinkLocationRegistry r = new SinkLocationRegistry(f); r.record("lucene", "z", "agent-9"); return new String(Files.readAllBytes(f), StandardCharsets.UTF_8); }));
      Files.write(f, "{\"a/b\":\"x\",\"c\":1}".getBytes(StandardCharsets.UTF_8)); p("sinkloc.reloadOdd", tryIt(() -> { SinkLocationRegistry r = new SinkLocationRegistry(f); r.record("k", "n", "ag"); return new String(Files.readAllBytes(f), StandardCharsets.UTF_8); })); }
    // ---- 4. Schedule persisted through hitorro-util's JsonFileEntityStore (as ScheduleStore does)
    { Path f = dir.resolve("schedules.json");
      Files.write(f, ("[{\"name\":\"s1\",\"cron\":\"0 * * * *\",\"lastRunAt\":\"2026-01-02T03:04:05.123Z\",\"totalRuns\":3,\"jobYaml\":\"job: x\\nnodes: []\\n\",\"checkpoint\":\"{\\\"rowid\\\":7}\"},"
        + "{\"name\":\"s2\",\"intervalSeconds\":60,\"lastSuccessAt\":1700000000.5,\"enabled\":false}]").getBytes(StandardCharsets.UTF_8));
      p("schedule.load", tryIt(() -> { JsonFileEntityStore<Schedule> st = new JsonFileEntityStore<>(f, Schedule.class, s -> s.name);
        StringBuilder sb = new StringBuilder(); for (Schedule x : st.all()) sb.append(x.name + "|" + x.cron + "|" + x.intervalSeconds + "|" + x.lastRunAt + "|" + x.lastSuccessAt + "|" + x.enabled + "|" + x.totalRuns + "|" + x.jobYaml + "|" + x.checkpoint + "; ");
        Schedule n = new Schedule(); n.name = "s3"; n.jobYaml = "classpath:/x.yaml"; n.lastFailureAt = java.time.Instant.ofEpochMilli(1700000000123L); n.lastError = "e"; st.put(n);
        return sb + " FILE " + new String(Files.readAllBytes(f), StandardCharsets.UTF_8); }));
    }
    return out;
  }

  static Map<String, String> load(String name) throws IOException {
    Map<String, String> m = new LinkedHashMap<>();
    try (InputStream in = Jackson2FormatGoldenTest.class.getResourceAsStream(name)) {
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
    Map<String, String> expected = load("jackson2-pipelines.txt");
    
    Map<String, String> actual = new LinkedHashMap<>();
    compute(dir).forEach((k, v) -> actual.put(k, v.replace("StringNode", "TextNode")));
    org.assertj.core.api.Assertions.assertThat(expected).hasSize(64);
    org.assertj.core.api.Assertions.assertThat(actual).containsExactlyEntriesOf(expected);
  }
}

