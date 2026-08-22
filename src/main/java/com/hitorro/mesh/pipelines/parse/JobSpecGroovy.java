/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.parse;

import com.hitorro.mesh.pipelines.model.AggSpec;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.model.PipelineSpec;
import com.hitorro.mesh.pipelines.model.ReduceSpec;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.model.StepSpec;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.codehaus.groovy.control.CompilerConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Groovy DSL front-end for building {@link JobSpec}s. Complements the
 * YAML/JSON path in {@link JobSpecYaml} — same output type; different
 * authoring surface. Preferred over YAML when the job wants control
 * flow, comments, computed values, or shared closures.
 *
 * <h3>Example</h3>
 * <pre>{@code
 * job('countries-example') {
 *     node('seed') {
 *         source inline: [[iso3: 'USA', pop: 331_000_000],
 *                         [iso3: 'CHN', pop: 1_400_000_000]]
 *         step filter: 'pop > 50000000'
 *         sink counting: 'seed'
 *     }
 *
 *     node('rollup', depends: ['seed']) {
 *         source ref: 'seed'
 *         reduce groupBy: ['iso3'], aggs: [
 *             [name: 'total_pop', kind: 'SUM', of: 'pop'],
 *             [name: 'n',         kind: 'COUNT']
 *         ]
 *         sink ndjson: 'target/out.ndjson'
 *     }
 * }
 * }</pre>
 *
 * <h3>Kind vocabulary</h3>
 * The DSL uses camelCase names matching the YAML {@code kind} ids
 * (which are kebab-case in YAML — {@code ndjson-file}). Map both:
 * <ul>
 *   <li>sources: {@code inline / ndjson / ndjsonFile / json / jsonFile /
 *       csv / csvFile / ref / kvstore / lucene / sql / nats / kafka}</li>
 *   <li>steps: {@code filter / project / rename / setField / groovyMap /
 *       jvsGroovy / toTyped / lookup}</li>
 *   <li>sinks: {@code ndjson / ndjsonFile / csv / csvFile / json /
 *       jsonFile / kvstore / lucene / jvsLucene / nats / kafka /
 *       counting / memoryTable}</li>
 * </ul>
 *
 * <p>Needs {@code org.apache.groovy:groovy} on the classpath (same
 * optional dep as the {@code groovy-map} step). Without it the parse
 * call fails with a clear {@code UnsupportedOperationException}.</p>
 */
public final class JobSpecGroovy {

    private JobSpecGroovy() { }

    /** Parse a Groovy DSL script into a {@link JobSpec}. */
    public static JobSpec parse(String scriptText) {
        try {
            CompilerConfiguration cfg = new CompilerConfiguration();
            cfg.setScriptBaseClass(JobDslScript.class.getName());
            GroovyShell shell = new GroovyShell(
                    JobSpecGroovy.class.getClassLoader(), cfg);
            JobDslScript script = (JobDslScript) shell.parse(scriptText);
            script.run();
            if (script.built == null) {
                throw new IllegalArgumentException(
                        "Groovy job script must call `job('name') { ... }` at top level");
            }
            return script.built;
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    "Groovy DSL needs org.apache.groovy:groovy on the classpath "
                    + "— add the optional dependency", e);
        }
    }

    // ================================================== DSL script base

    /**
     * Groovy scripts extend this class (via CompilerConfiguration) and
     * gain the top-level {@code job(name, closure)} DSL entry point.
     */
    public abstract static class JobDslScript extends Script {
        JobSpec built;

        public void job(String name, Closure<?> body) {
            JobBuilder jb = new JobBuilder(name);
            body.setDelegate(jb);
            body.setResolveStrategy(Closure.DELEGATE_FIRST);
            body.call();
            built = jb.build();
        }
    }

    // ================================================== Job builder

    /** DSL delegate exposed to the job closure — collects nodes. */
    public static final class JobBuilder {
        private final String name;
        private final List<NodeSpec> nodes = new ArrayList<>();
        private boolean restartable = false;

        JobBuilder(String name) { this.name = name; }

        /**
         * {@code restartable true} at job scope marks the whole job as
         * restartable — driver persists the spec on start, resumes on
         * boot. See {@link com.hitorro.mesh.pipelines.model.JobSpec#restartable()}
         * for the safety caveats (rows re-process on resume; only
         * idempotent sinks stay clean).
         */
        public void restartable(boolean v) { this.restartable = v; }

        /** {@code node('id') { … }} — no dependencies. */
        public void node(String id, Closure<?> body) {
            node(id, Collections.<String, Object>emptyMap(), body);
        }

        /** {@code node('id', depends: [...]) { … }}. Groovy places the
         *  named-arg map FIRST when both are passed, so this overload
         *  matches {@code node(depends: [...], 'id') { … }} too. */
        public void node(Map<String, Object> opts, String id, Closure<?> body) {
            node(id, opts, body);
        }

        /** Programmatic form: {@code node('id', [depends: [...]]) { … }}. */
        @SuppressWarnings("unchecked")
        public void node(String id, Map<String, Object> opts, Closure<?> body) {
            List<String> depends = (List<String>) opts.getOrDefault("depends", List.of());
            NodeBuilder nb = new NodeBuilder(id, depends);
            body.setDelegate(nb);
            body.setResolveStrategy(Closure.DELEGATE_FIRST);
            body.call();
            nodes.add(nb.build());
        }

        JobSpec build() { return new JobSpec(name, "1", nodes, restartable); }
    }

    // ================================================== Node builder

    /** DSL delegate exposed to each node closure. */
    public static final class NodeBuilder {
        private final String id;
        private final List<String> depends;
        private SourceSpec source;
        private final List<StepSpec> steps = new ArrayList<>();
        private ReduceSpec reduce;
        private final List<SinkSpec> sinks = new ArrayList<>();

        NodeBuilder(String id, List<String> depends) {
            this.id = id;
            this.depends = depends;
        }

        public void source(Map<String, Object> args) { source = DslMappers.source(args); }
        public void step(Map<String, Object> args)   { steps.add(DslMappers.step(args)); }
        public void reduce(Map<String, Object> args) { reduce = DslMappers.reduce(args); }
        public void sink(Map<String, Object> args)   { sinks.add(DslMappers.sink(args)); }

        NodeSpec build() {
            if (source == null) {
                throw new IllegalArgumentException(
                        "node '" + id + "' has no source — every pipeline needs one");
            }
            return new NodeSpec(id, null,
                    new PipelineSpec(source, steps, reduce, sinks), depends);
        }
    }

    // ================================================== Map → Spec mappers

    private static final class DslMappers {
        private DslMappers() { }

        // ---------- source
        @SuppressWarnings("unchecked")
        static SourceSpec source(Map<String, Object> args) {
            if (has(args, "inline"))
                return new SourceSpec.Inline((List<Map<String, Object>>) args.get("inline"));
            if (has(args, "ndjson"))    return new SourceSpec.NdjsonFile(str(args, "ndjson"));
            if (has(args, "ndjsonFile"))return new SourceSpec.NdjsonFile(str(args, "ndjsonFile"));
            if (has(args, "json"))      return new SourceSpec.JsonFile(str(args, "json"));
            if (has(args, "jsonFile"))  return new SourceSpec.JsonFile(str(args, "jsonFile"));
            if (has(args, "csv"))       return new SourceSpec.CsvFile(str(args, "csv"));
            if (has(args, "csvFile"))   return new SourceSpec.CsvFile(str(args, "csvFile"));
            if (has(args, "ref"))       return new SourceSpec.Ref(str(args, "ref"));
            if (has(args, "kvstore"))   return new SourceSpec.KvStore(str(args, "kvstore"));
            if (has(args, "lucene"))    return new SourceSpec.Lucene(str(args, "lucene"),
                    strOrNull(args, "query"));
            if (has(args, "sql"))       return new SourceSpec.Sql(str(args, "sql"));
            if (has(args, "sqlite"))    return new SourceSpec.Sqlite(
                    str(args, "sqlite"),
                    strOrDefault(args, "query", "SELECT 1"),
                    (List<Object>) args.getOrDefault("params", List.of()));
            if (has(args, "nats"))      return new SourceSpec.Nats(str(args, "nats"),
                    strOrDefault(args, "servers", "nats://localhost:4222"));
            if (has(args, "kafka"))     return new SourceSpec.Kafka(
                    strOrDefault(args, "bootstrap", "localhost:9092"),
                    str(args, "kafka"),
                    strOrNull(args, "groupId"));
            throw dslError("source", args,
                    "inline | ndjson | json | csv | ref | kvstore | lucene | sql | sqlite | nats | kafka");
        }

        // ---------- step
        @SuppressWarnings("unchecked")
        static StepSpec step(Map<String, Object> args) {
            if (has(args, "filter"))    return new StepSpec.Filter(str(args, "filter"));
            if (has(args, "project"))   return new StepSpec.Project((List<String>) args.get("project"));
            if (has(args, "rename"))    return new StepSpec.Rename(str(args, "rename"), str(args, "to"));
            if (has(args, "setField"))  return new StepSpec.SetField(str(args, "setField"), args.get("value"));
            if (has(args, "groovyMap")) return new StepSpec.GroovyMap(str(args, "groovyMap"));
            if (has(args, "jvsGroovy")) return new StepSpec.JvsGroovy(str(args, "jvsGroovy"),
                    strOrNull(args, "typeJson"));
            if (has(args, "toTyped")) {
                List<Map<String, Object>> fieldMaps = (List<Map<String, Object>>) args.get("toTyped");
                List<StepSpec.ToTyped.Field> fields = new ArrayList<>(fieldMaps.size());
                for (var fm : fieldMaps) {
                    fields.add(new StepSpec.ToTyped.Field(
                            (String) fm.get("name"),
                            (String) fm.get("type"),
                            (String) fm.get("role")));
                }
                boolean passthrough = Boolean.TRUE.equals(args.getOrDefault("passthrough", false));
                return new StepSpec.ToTyped(fields, passthrough);
            }
            if (has(args, "lookup"))    return new StepSpec.Lookup(
                    str(args, "lookup"),
                    strOrDefault(args, "onField", "id"),
                    strOrDefault(args, "withKey", "id"),
                    (List<String>) args.get("adds"));
            throw dslError("step", args,
                    "filter | project | rename | setField | groovyMap | jvsGroovy | toTyped | lookup");
        }

        // ---------- reduce
        @SuppressWarnings("unchecked")
        static ReduceSpec reduce(Map<String, Object> args) {
            List<String> groupBy = (List<String>) args.getOrDefault("groupBy", List.of());
            List<Map<String, Object>> aggMaps =
                    (List<Map<String, Object>>) args.getOrDefault("aggs", List.of());
            List<AggSpec> aggs = new ArrayList<>(aggMaps.size());
            for (var am : aggMaps) {
                aggs.add(new AggSpec(
                        (String) am.get("name"),
                        AggSpec.AggKind.valueOf(((String) am.get("kind")).toUpperCase()),
                        (String) am.get("of")));
            }
            boolean shuffle = Boolean.TRUE.equals(args.getOrDefault("shuffle", false));
            int buckets = ((Number) args.getOrDefault("buckets", 2)).intValue();
            return new ReduceSpec(groupBy, aggs, shuffle, buckets);
        }

        // ---------- sink
        @SuppressWarnings("unchecked")
        static SinkSpec sink(Map<String, Object> args) {
            if (has(args, "ndjson"))     return new SinkSpec.NdjsonFile(str(args, "ndjson"));
            if (has(args, "ndjsonFile")) return new SinkSpec.NdjsonFile(str(args, "ndjsonFile"));
            if (has(args, "csv"))        return new SinkSpec.CsvFile(str(args, "csv"),
                    (List<String>) args.get("cols"));
            if (has(args, "csvFile"))    return new SinkSpec.CsvFile(str(args, "csvFile"),
                    (List<String>) args.get("cols"));
            if (has(args, "json"))       return new SinkSpec.JsonFile(str(args, "json"),
                    Boolean.TRUE.equals(args.getOrDefault("pretty", false)));
            if (has(args, "jsonFile"))   return new SinkSpec.JsonFile(str(args, "jsonFile"),
                    Boolean.TRUE.equals(args.getOrDefault("pretty", false)));
            if (has(args, "kvstore"))    return new SinkSpec.KvStore(str(args, "kvstore"),
                    strOrDefault(args, "keyExpr", "id"),
                    (Boolean) args.getOrDefault("registerAsTable", Boolean.FALSE));
            if (has(args, "lucene"))     return SinkSpec.Lucene.fromJson(
                    str(args, "lucene"),
                    (Boolean) args.get("storeSource"));
            if (has(args, "jvsLucene"))  return SinkSpec.JvsLucene.fromJson(
                    str(args, "jvsLucene"),
                    strOrNull(args, "typeJsonResource"),
                    (Boolean) args.get("storeSource"));
            if (has(args, "nats"))       return new SinkSpec.Nats(str(args, "nats"),
                    strOrDefault(args, "servers", "nats://localhost:4222"));
            if (has(args, "kafka"))      return new SinkSpec.Kafka(
                    strOrDefault(args, "bootstrap", "localhost:9092"),
                    str(args, "kafka"),
                    strOrNull(args, "keyExpr"));
            if (has(args, "counting"))    return new SinkSpec.Counting(str(args, "counting"));
            if (has(args, "memoryTable")) return new SinkSpec.MemoryTable(str(args, "memoryTable"));
            throw dslError("sink", args,
                    "ndjson | csv | json | kvstore | lucene | jvsLucene | nats | kafka | counting | memoryTable");
        }

        // ---------- tiny map helpers
        private static boolean has(Map<String, Object> m, String k) {
            return m.get(k) != null;
        }
        private static String str(Map<String, Object> m, String k) {
            Object v = m.get(k);
            if (v == null) throw new IllegalArgumentException("missing required arg: " + k);
            return String.valueOf(v);
        }
        private static String strOrNull(Map<String, Object> m, String k) {
            Object v = m.get(k);
            return v == null ? null : String.valueOf(v);
        }
        private static String strOrDefault(Map<String, Object> m, String k, String d) {
            Object v = m.get(k);
            return v == null ? d : String.valueOf(v);
        }
        private static IllegalArgumentException dslError(String what, Map<String, Object> args, String allowed) {
            return new IllegalArgumentException(
                    "unrecognised " + what + " kind — args " + args
                    + " must contain exactly one of: " + allowed);
        }
    }
}
