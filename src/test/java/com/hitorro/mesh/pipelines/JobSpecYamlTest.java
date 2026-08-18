/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines;

import com.hitorro.mesh.pipelines.model.*;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class JobSpecYamlTest {

    @Test
    void luceneSink_omittingStoreSource_defaultsTrue() throws Exception {
        // Regression against the "empty hits" gotcha — the default when
        // storeSource is not in the YAML must be true so search-back
        // returns content, not {}. Jackson's @JsonCreator on
        // SinkSpec.Lucene enforces this. Same for JvsLucene.
        String yaml = """
                job: t
                nodes:
                  - id: n
                    pipeline:
                      source: {kind: inline, rows: []}
                      sinks:
                        - {kind: lucene, name: idx}
                        - {kind: jvs-lucene, name: jidx, typeJsonResource: "classpath:/x.json"}
                """;
        JobSpec spec = JobSpecYaml.parse(yaml);
        var sinks = spec.nodes().get(0).pipeline().sinks();
        var lucene   = (SinkSpec.Lucene)    sinks.get(0);
        var jvsLucene = (SinkSpec.JvsLucene) sinks.get(1);
        assertThat(lucene.storeSource()).isTrue();
        assertThat(jvsLucene.storeSource()).isTrue();
    }

    @Test
    void luceneSink_explicitStoreSourceFalse_isRespected() throws Exception {
        // Users who explicitly opt into the smaller/no-content mode
        // must still get it — the default-true change is additive,
        // not a policy override.
        String yaml = """
                job: t
                nodes:
                  - id: n
                    pipeline:
                      source: {kind: inline, rows: []}
                      sinks:
                        - {kind: lucene, name: idx, storeSource: false}
                """;
        var lucene = (SinkSpec.Lucene) JobSpecYaml.parse(yaml)
                .nodes().get(0).pipeline().sinks().get(0);
        assertThat(lucene.storeSource()).isFalse();
    }

    @Test
    void parses_example_yaml_with_all_source_step_sink_kinds() throws Exception {
        JobSpec spec = JobSpecYaml.parseFile(Path.of("src/test/resources/example-job.yaml"));

        assertThat(spec.id()).isEqualTo("countries-triple-sink");
        assertThat(spec.nodes()).hasSize(3);

        NodeSpec seed = spec.nodes().get(0);
        assertThat(seed.id()).isEqualTo("seed");
        assertThat(seed.depends()).isEmpty();
        assertThat(seed.pipeline().source()).isInstanceOf(SourceSpec.NdjsonFile.class);
        assertThat(seed.pipeline().steps()).hasSize(2)
                .anySatisfy(s -> assertThat(s).isInstanceOf(StepSpec.Filter.class))
                .anySatisfy(s -> assertThat(s).isInstanceOf(StepSpec.Project.class));
        assertThat(seed.pipeline().sinks()).hasSize(2)
                .anySatisfy(s -> assertThat(s).isInstanceOf(SinkSpec.MemoryTable.class))
                .anySatisfy(s -> assertThat(s).isInstanceOf(SinkSpec.Counting.class));

        NodeSpec rollup = spec.nodes().get(1);
        assertThat(rollup.depends()).containsExactly("seed");
        assertThat(rollup.pipeline().source()).isInstanceOf(SourceSpec.Ref.class);
        assertThat(((SourceSpec.Ref) rollup.pipeline().source()).node()).isEqualTo("seed");
        assertThat(rollup.pipeline().reduce()).isNotNull();
        assertThat(rollup.pipeline().reduce().groupBy()).containsExactly("region");
        assertThat(rollup.pipeline().reduce().aggs()).hasSize(3);
        assertThat(rollup.pipeline().reduce().aggs())
                .extracting(AggSpec::kind)
                .containsExactly(AggSpec.AggKind.COUNT, AggSpec.AggKind.SUM, AggSpec.AggKind.MAX);

        NodeSpec index = spec.nodes().get(2);
        assertThat(index.pipeline().sinks()).hasSize(2)
                .anySatisfy(s -> assertThat(s).isInstanceOf(SinkSpec.MemoryTable.class))
                .anySatisfy(s -> assertThat(s).isInstanceOf(SinkSpec.Counting.class));
    }

    @Test
    void parses_inline_json_body() throws Exception {
        String json = """
                {
                  "job": "tiny",
                  "nodes": [{
                    "id": "n1",
                    "pipeline": {
                      "source": {"kind": "inline", "rows": [{"x": 1}, {"x": 2}]},
                      "sinks":  [{"kind": "counting", "label": "n"}]
                    }
                  }]
                }
                """;
        JobSpec spec = JobSpecYaml.parse(json);
        assertThat(spec.id()).isEqualTo("tiny");
        assertThat(spec.nodes().get(0).pipeline().source()).isInstanceOf(SourceSpec.Inline.class);
    }

    @Test
    void round_trips_to_json_and_back() throws Exception {
        JobSpec original = JobSpecYaml.parseFile(Path.of("src/test/resources/example-job.yaml"));
        String json = JobSpecYaml.toJson(original);
        JobSpec restored = JobSpecYaml.parse(json);
        assertThat(restored.id()).isEqualTo(original.id());
        assertThat(restored.nodes()).hasSameSizeAs(original.nodes());
    }

    @Test
    void rejects_unknown_source_kind_gracefully() {
        String yaml = """
                job: bad
                nodes:
                  - id: n
                    pipeline:
                      source: {kind: martians, url: "moon://x"}
                      sinks:  [{kind: counting, label: n}]
                """;
        assertThatThrownBy(() -> JobSpecYaml.parse(yaml))
                .hasMessageContaining("martians");
    }
}
