/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;
import com.hitorro.mesh.pipelines.runtime.BundledJobs;
import com.hitorro.mesh.pipelines.runtime.GroovyMapStep;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.JobStatus;
import com.hitorro.mesh.pipelines.sinks.MemoryTableSink;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GroovyMapStepTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void groovy_script_mutates_row_and_returns_it() throws Exception {
        var step = GroovyMapStep.compile("""
                row.doubled = row.n * 2
                row.tag = 'processed'
                return row
                """);
        JsonNode input = JSON.readTree("{\"n\": 5}");
        JsonNode out = step.apply(input);
        assertThat(out.get("n").asInt()).isEqualTo(5);
        assertThat(out.get("doubled").asInt()).isEqualTo(10);
        assertThat(out.get("tag").asText()).isEqualTo("processed");
    }

    @Test
    void groovy_script_returning_null_drops_the_row() throws Exception {
        var step = GroovyMapStep.compile("row.n > 10 ? row : null");
        assertThat(step.apply(JSON.readTree("{\"n\": 5}"))).isNull();
        assertThat(step.apply(JSON.readTree("{\"n\": 20}"))).isNotNull();
    }

    @Test
    void gen_helper_provides_uuid_and_sequence() throws Exception {
        var step = GroovyMapStep.compile("""
                row.id = gen.uuid()
                row.seq = gen.next('order')
                return row
                """);
        JsonNode a = step.apply(JSON.readTree("{}"));
        JsonNode b = step.apply(JSON.readTree("{}"));
        assertThat(a.get("id").asText()).hasSize(36);   // UUID string length
        assertThat(a.get("seq").asLong()).isEqualTo(1);
        assertThat(b.get("seq").asLong()).isEqualTo(2); // monotonic
        assertThat(a.get("id").asText()).isNotEqualTo(b.get("id").asText());
    }

    @Test
    void bundled_airports_groovy_runs_end_to_end(@TempDir Path home) throws Exception {
        // Rewrite the ndjson-file url so it lands in temp dir.
        String yaml = BundledJobs.load("airports-groovy")
                .replace("target/airports-groovy.ndjson",
                         home.resolve("out.ndjson").toString());
        JobSpec spec = JobSpecYaml.parse(yaml);
        SinkRegistry reg = new SinkRegistry(home);
        JobStatus status;
        try (JobRunner runner = new JobRunner(reg)) {
            status = runner.run(spec);
        }
        assertThat(status.state).isEqualTo(JobStatus.State.SUCCEEDED);
        assertThat(status.node("process").rowsIn).isEqualTo(15);
        assertThat(status.node("process").rowsOut).isEqualTo(15);
        // Alpine = elevation >= 3000. From the shipped CSV: MEX (7316),
        // JNB (5558), GRU (2459 — highland, not alpine). So 2 alpines.
        assertThat(status.node("alpine-only").rowsOut).isEqualTo(2);
        // Every processed row has the Groovy-added fields.
        List<JsonNode> processed = reg.memoryTable("airports-processed");
        assertThat(processed).allSatisfy(row -> {
            assertThat(row.hasNonNull("altitude_bucket")).isTrue();
            assertThat(row.hasNonNull("event_id")).isTrue();
            assertThat(row.hasNonNull("seq")).isTrue();
            assertThat(row.get("event_id").asText()).hasSize(36);
        });
    }
}
