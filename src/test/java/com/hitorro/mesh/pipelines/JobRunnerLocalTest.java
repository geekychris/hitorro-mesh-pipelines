/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.JobStatus;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class JobRunnerLocalTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void runs_three_node_dag_end_to_end(@TempDir Path home) throws Exception {
        // Rewrite the example spec so the ndjson-file sink lands under the
        // temp dir; keep everything else identical to the shipped example.
        String yaml = Files.readString(Path.of("src/test/resources/example-job.yaml"))
                .replace("target/pipelines-test/region-rollup.ndjson",
                         home.resolve("region-rollup.ndjson").toString());
        JobSpec spec = JobSpecYaml.parse(yaml);

        SinkRegistry reg = new SinkRegistry(home);
        JobStatus status;
        try (JobRunner runner = new JobRunner(reg)) {
            status = runner.run(spec);
        }

        assertThat(status.state).isEqualTo(JobStatus.State.SUCCEEDED);
        assertThat(status.error).isNull();

        // seed: 15 rows in → filter to population > 50M → 12 pass → project 4 cols
        assertThat(status.node("seed").state).isEqualTo(JobStatus.State.SUCCEEDED);
        assertThat(status.node("seed").rowsIn).isEqualTo(15);
        assertThat(status.node("seed").rowsOut).isEqualTo(12);

        // rollup: takes 12 rows in, groups by region (Africa, Americas, Asia, Europe)
        assertThat(status.node("rollup").state).isEqualTo(JobStatus.State.SUCCEEDED);
        assertThat(status.node("rollup").rowsIn).isEqualTo(12);
        assertThat(status.node("rollup").rowsOut).isEqualTo(4);
        assertThat(Files.exists(home.resolve("region-rollup.ndjson"))).isTrue();

        // index: 12 rows fanned out to memory-table + counting sinks.
        // Real KV / Lucene fan-out is covered by the RoundTrip tests in
        // hitorro-mesh-pipelines-kvstore and -lucene — those adapters
        // aren't on this module's test classpath by design (SinkRegistry
        // throws with a clear message when the adapter jars are missing).
        assertThat(status.node("index").state).isEqualTo(JobStatus.State.SUCCEEDED);
        assertThat(status.node("index").rowsOut).isEqualTo(12);
        assertThat(reg.memoryTable("countries-mem")).hasSize(12);

        // Rollup values — verify Asia sum matches.
        List<String> rollup = Files.readAllLines(home.resolve("region-rollup.ndjson"));
        assertThat(rollup).hasSize(4);
        JsonNode asia = null;
        for (String line : rollup) {
            JsonNode r = JSON.readTree(line);
            if ("Asia".equals(r.get("region").asText())) { asia = r; break; }
        }
        assertThat(asia).isNotNull();
        // Asia: CHN 1412M + IND 1408M + IDN 278M + PAK 231M + JPN 125M = 3454M
        assertThat(asia.get("n").asLong()).isEqualTo(5);
        assertThat(asia.get("total_pop").asDouble()).isEqualTo(3454_000_000d);
        assertThat(asia.get("max_pop").asDouble()).isEqualTo(1412_000_000d);
    }

    @Test
    void fails_cleanly_on_dag_cycle() {
        String yaml = """
                job: cyclic
                nodes:
                  - id: a
                    depends: [b]
                    pipeline: {source: {kind: inline, rows: []}, sinks: [{kind: counting, label: n}]}
                  - id: b
                    depends: [a]
                    pipeline: {source: {kind: inline, rows: []}, sinks: [{kind: counting, label: n}]}
                """;
        try (JobRunner r = new JobRunner(new SinkRegistry(Path.of("target/tmp")))) {
            JobStatus s = r.run(assertParse(yaml));
            assertThat(s.state).isEqualTo(JobStatus.State.FAILED);
            assertThat(s.error).contains("cycle");
        }
    }

    @Test
    void inline_source_plus_counting_sink() {
        String yaml = """
                job: tiny
                nodes:
                  - id: n1
                    pipeline:
                      source: {kind: inline, rows: [{x: 1}, {x: 2}, {x: 3}]}
                      steps: [{kind: filter, expr: "x > 1"}]
                      sinks: [{kind: counting, label: n}]
                """;
        try (JobRunner r = new JobRunner(new SinkRegistry(Path.of("target/tmp")))) {
            JobStatus s = r.run(assertParse(yaml));
            assertThat(s.state).isEqualTo(JobStatus.State.SUCCEEDED);
            assertThat(s.node("n1").rowsIn).isEqualTo(3);
            assertThat(s.node("n1").rowsOut).isEqualTo(2);
        }
    }

    private static JobSpec assertParse(String yaml) {
        try { return JobSpecYaml.parse(yaml); }
        catch (IOException e) { throw new AssertionError(e); }
    }
}
