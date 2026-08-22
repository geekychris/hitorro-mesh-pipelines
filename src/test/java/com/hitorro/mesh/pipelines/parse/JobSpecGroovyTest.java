/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.parse;

import com.hitorro.mesh.pipelines.model.AggSpec;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.model.StepSpec;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Coverage for the Groovy DSL front-end. Same output type as
 * {@link JobSpecYaml} — every test builds a JobSpec via the DSL and
 * asserts the resulting record structure.
 *
 * <p>Kind-name coverage is exhaustive but shallow: every recognised
 * source / step / sink token gets one test proving it maps to the
 * right Spec record. Semantic correctness of the specs themselves
 * (filter grammar, coercion, reduce math, …) is covered by their
 * respective factories' tests.</p>
 */
class JobSpecGroovyTest {

    // -------------------------------------------------- top-level shape

    @Test
    void minimal_singleNode_buildsJobSpec() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('minimal') {
                    node('only') {
                        source inline: [[n: 1]]
                        sink counting: 'c'
                    }
                }
                """);
        assertThat(spec.id()).isEqualTo("minimal");
        assertThat(spec.nodes()).hasSize(1);
        assertThat(spec.nodes().get(0).id()).isEqualTo("only");
        assertThat(spec.nodes().get(0).depends()).isEmpty();
    }

    @Test
    void twoNodes_dependsWiring() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('two') {
                    node('a') {
                        source inline: [[x: 1]]
                        sink counting: 'a'
                    }
                    node('b', depends: ['a']) {
                        source ref: 'a'
                        sink counting: 'b'
                    }
                }
                """);
        NodeSpec b = spec.nodes().get(1);
        assertThat(b.id()).isEqualTo("b");
        assertThat(b.depends()).containsExactly("a");
        assertThat(b.pipeline().source()).isInstanceOf(SourceSpec.Ref.class);
    }

    @Test
    void controlFlow_worksInsideScript() {
        // The whole reason for a Groovy DSL — write jobs with loops
        // and variables that YAML can't express natively.
        JobSpec spec = JobSpecGroovy.parse("""
                job('generated') {
                    3.times { i ->
                        node("n${i}") {
                            source inline: [[i: i]]
                            sink counting: "c${i}"
                        }
                    }
                }
                """);
        assertThat(spec.nodes()).hasSize(3);
        assertThat(spec.nodes()).extracting(NodeSpec::id)
                .containsExactly("n0", "n1", "n2");
    }

    // -------------------------------------------------- sources

    @Test
    void source_inline_ndjson_json_csv_ref() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('inl')     { source inline: [[a: 1]];             sink counting: 'c' }
                    node('nd')      { source ndjson: 'in.ndjson';           sink counting: 'c' }
                    node('js')      { source json: 'in.json';               sink counting: 'c' }
                    node('cs')      { source csv:  'in.csv';                sink counting: 'c' }
                    node('rf')      { source ref:  'inl';                   sink counting: 'c' }
                }
                """);
        assertThat(kind(spec, "inl")).isEqualTo(SourceSpec.Inline.class);
        assertThat(kind(spec, "nd")).isEqualTo(SourceSpec.NdjsonFile.class);
        assertThat(kind(spec, "js")).isEqualTo(SourceSpec.JsonFile.class);
        assertThat(kind(spec, "cs")).isEqualTo(SourceSpec.CsvFile.class);
        assertThat(kind(spec, "rf")).isEqualTo(SourceSpec.Ref.class);
    }

    @Test
    void source_sqlite_withOrWithoutParams() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('a') {
                        source sqlite: '/tmp/x.db', query: 'SELECT 1'
                        sink counting: 'c'
                    }
                    node('b') {
                        source sqlite: '/tmp/x.db',
                               query: 'SELECT * FROM t WHERE id > ?',
                               params: [42]
                        sink counting: 'c'
                    }
                }
                """);
        var a = (SourceSpec.Sqlite) spec.nodes().get(0).pipeline().source();
        assertThat(a.path()).isEqualTo("/tmp/x.db");
        assertThat(a.query()).isEqualTo("SELECT 1");
        assertThat(a.params()).isEmpty();

        var b = (SourceSpec.Sqlite) spec.nodes().get(1).pipeline().source();
        assertThat(b.params()).containsExactly(42);
    }

    @Test
    void source_kvstore_lucene_sql_nats_kafka() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('kv') { source kvstore: 'users';                 sink counting: 'c' }
                    node('lu') { source lucene:  'idx', query: 'foo';      sink counting: 'c' }
                    node('sq') { source sql: 'SELECT 1';                   sink counting: 'c' }
                    node('na') { source nats: 'ingest.evt';                sink counting: 'c' }
                    node('ka') { source kafka: 'events', bootstrap: 'localhost:9092', groupId: 'g'; sink counting: 'c' }
                }
                """);
        assertThat(kind(spec, "kv")).isEqualTo(SourceSpec.KvStore.class);
        assertThat(kind(spec, "lu")).isEqualTo(SourceSpec.Lucene.class);
        assertThat(kind(spec, "sq")).isEqualTo(SourceSpec.Sql.class);
        assertThat(kind(spec, "na")).isEqualTo(SourceSpec.Nats.class);
        assertThat(kind(spec, "ka")).isEqualTo(SourceSpec.Kafka.class);
    }

    // -------------------------------------------------- steps

    @Test
    void step_filter_project_rename_setField() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        source inline: [[x: 1]]
                        step filter: 'x > 0'
                        step project: ['x', 'y']
                        step rename: 'x', to: 'z'
                        step setField: 'source', value: 'dsl'
                        sink counting: 'c'
                    }
                }
                """);
        var steps = spec.nodes().get(0).pipeline().steps();
        assertThat(steps).hasSize(4);
        assertThat(steps.get(0)).isInstanceOf(StepSpec.Filter.class);
        assertThat(steps.get(1)).isInstanceOf(StepSpec.Project.class);
        assertThat(steps.get(2)).isInstanceOf(StepSpec.Rename.class);
        assertThat(steps.get(3)).isInstanceOf(StepSpec.SetField.class);
        assertThat(((StepSpec.Rename) steps.get(2)).to()).isEqualTo("z");
        assertThat(((StepSpec.SetField) steps.get(3)).value()).isEqualTo("dsl");
    }

    @Test
    void step_groovyMap_jvsGroovy_lookup() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        source inline: [[x: 1]]
                        step groovyMap: 'row.n = 1'
                        step jvsGroovy: 'copyAll()'
                        step lookup: 'countries', onField: 'iso3', withKey: 'iso3', adds: ['name']
                        sink counting: 'c'
                    }
                }
                """);
        var steps = spec.nodes().get(0).pipeline().steps();
        assertThat(steps.get(0)).isInstanceOf(StepSpec.GroovyMap.class);
        assertThat(steps.get(1)).isInstanceOf(StepSpec.JvsGroovy.class);
        assertThat(steps.get(2)).isInstanceOf(StepSpec.Lookup.class);
    }

    @Test
    void step_toTyped_withFieldList() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        source inline: [[x: '1']]
                        step toTyped: [
                            [name: 'x', type: 'core_long'],
                            [name: 's', type: 'core_string']
                        ], passthrough: true
                        sink counting: 'c'
                    }
                }
                """);
        var tt = (StepSpec.ToTyped) spec.nodes().get(0).pipeline().steps().get(0);
        assertThat(tt.fields()).hasSize(2);
        assertThat(tt.fields().get(0).name()).isEqualTo("x");
        assertThat(tt.fields().get(0).type()).isEqualTo("core_long");
        assertThat(tt.passthrough()).isTrue();
    }

    // -------------------------------------------------- reduce

    @Test
    void reduce_groupByAndAggs() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        source inline: [[iso: 'US', v: 10]]
                        reduce groupBy: ['iso'], aggs: [
                            [name: 'total', kind: 'SUM', of: 'v'],
                            [name: 'n',     kind: 'COUNT']
                        ]
                        sink ndjson: 'target/tmp.ndjson'
                    }
                }
                """);
        var r = spec.nodes().get(0).pipeline().reduce();
        assertThat(r.groupBy()).containsExactly("iso");
        assertThat(r.aggs()).hasSize(2);
        assertThat(r.aggs().get(0).kind()).isEqualTo(AggSpec.AggKind.SUM);
        assertThat(r.aggs().get(0).of()).isEqualTo("v");
        assertThat(r.aggs().get(1).kind()).isEqualTo(AggSpec.AggKind.COUNT);
    }

    @Test
    void reduce_shuffleAndBuckets() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        source inline: [[iso: 'US', v: 10]]
                        reduce groupBy: ['iso'], aggs: [[name: 'n', kind: 'COUNT']],
                               shuffle: true, buckets: 4
                        sink counting: 'c'
                    }
                }
                """);
        var r = spec.nodes().get(0).pipeline().reduce();
        assertThat(r.shuffle()).isTrue();
        assertThat(r.buckets()).isEqualTo(4);
    }

    // -------------------------------------------------- sinks

    @Test
    void sink_ndjson_csv_json_kvstore_lucene() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        source inline: [[a: 1]]
                        sink ndjson: 'a.ndjson'
                        sink csv: 'b.csv', cols: ['a']
                        sink json: 'c.json', pretty: true
                        sink kvstore: 'kv-name', keyExpr: 'id'
                        sink lucene: 'idx'
                    }
                }
                """);
        var sinks = spec.nodes().get(0).pipeline().sinks();
        assertThat(sinks.get(0)).isInstanceOf(SinkSpec.NdjsonFile.class);
        assertThat(sinks.get(1)).isInstanceOf(SinkSpec.CsvFile.class);
        assertThat(sinks.get(2)).isInstanceOf(SinkSpec.JsonFile.class);
        assertThat(sinks.get(3)).isInstanceOf(SinkSpec.KvStore.class);
        assertThat(sinks.get(4)).isInstanceOf(SinkSpec.Lucene.class);
        // storeSource default from the fix in task 458 still applies.
        assertThat(((SinkSpec.Lucene) sinks.get(4)).storeSource()).isTrue();
        assertThat(((SinkSpec.JsonFile) sinks.get(2)).pretty()).isTrue();
    }

    @Test
    void sink_nats_kafka_counting_memoryTable_jvsLucene() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        source inline: [[a: 1]]
                        sink nats: 'topic', servers: 'nats://x:4222'
                        sink kafka: 'topic', bootstrap: 'localhost:9092'
                        sink counting: 'c'
                        sink memoryTable: 'm'
                        sink jvsLucene: 'jidx', typeJsonResource: 'classpath:/x.json'
                    }
                }
                """);
        var sinks = spec.nodes().get(0).pipeline().sinks();
        assertThat(sinks.get(0)).isInstanceOf(SinkSpec.Nats.class);
        assertThat(sinks.get(1)).isInstanceOf(SinkSpec.Kafka.class);
        assertThat(sinks.get(2)).isInstanceOf(SinkSpec.Counting.class);
        assertThat(sinks.get(3)).isInstanceOf(SinkSpec.MemoryTable.class);
        assertThat(sinks.get(4)).isInstanceOf(SinkSpec.JvsLucene.class);
    }

    // -------------------------------------------------- restartable

    @Test
    void restartable_defaultsFalse() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('batch') {
                    node('n') {
                        source inline: [[a: 1]]
                        sink counting: 'c'
                    }
                }
                """);
        assertThat(spec.restartable()).isFalse();
    }

    @Test
    void restartable_explicitTrue_atJobScope() {
        JobSpec spec = JobSpecGroovy.parse("""
                job('streamer') {
                    restartable true
                    node('n') {
                        source nats: 'ingest.evt'
                        sink counting: 'c'
                    }
                }
                """);
        assertThat(spec.restartable()).isTrue();
    }

    // -------------------------------------------------- error paths

    @Test
    void missingJobCall_throwsClearError() {
        // Script must reach the top-level `job('name') { ... }` — an
        // empty or wrong-shape script fails at parse-time with a
        // pointer to the API.
        assertThatThrownBy(() -> JobSpecGroovy.parse("""
                println 'hello'
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("job('name')");
    }

    @Test
    void missingSource_throwsClearError() {
        assertThatThrownBy(() -> JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        sink counting: 'c'
                    }
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no source");
    }

    @Test
    void unknownSourceKind_listsValidChoices() {
        // Typo like `source flopflop: 'x'` — DSL should not silently
        // produce a null source. Error names the accepted kinds.
        assertThatThrownBy(() -> JobSpecGroovy.parse("""
                job('t') {
                    node('n') {
                        source flopflop: 'x'
                        sink counting: 'c'
                    }
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inline | ndjson");
    }

    @Test
    void badGroovySyntax_bubblesAsException() {
        assertThatThrownBy(() -> JobSpecGroovy.parse("this is [[ not valid groovy"))
                .isInstanceOf(Exception.class);
    }

    // ------------------------------------------------------------ helpers

    private static Class<?> kind(JobSpec spec, String nodeId) {
        for (var n : spec.nodes()) {
            if (nodeId.equals(n.id())) return n.pipeline().source().getClass();
        }
        throw new AssertionError("no node " + nodeId);
    }
}
