/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.model.PipelineSpec;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.model.StepSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineLifecycleTest {

    @Test
    void ndjson_source_is_terminating() {
        JobSpec spec = job(new SourceSpec.NdjsonFile("/tmp/x.ndjson"));
        assertThat(JobRunner.lifecycleOf(spec)).isEqualTo("TERMINATING");
    }

    @Test
    void kafka_source_marks_job_continuous() {
        JobSpec spec = job(new SourceSpec.Kafka("localhost:9092", "events", "grp"));
        assertThat(JobRunner.lifecycleOf(spec)).isEqualTo("CONTINUOUS");
    }

    @Test
    void nats_source_marks_job_continuous() {
        JobSpec spec = job(new SourceSpec.Nats("events.*", "nats://localhost:4222"));
        assertThat(JobRunner.lifecycleOf(spec)).isEqualTo("CONTINUOUS");
    }

    @Test
    void any_streaming_source_infects_the_whole_job() {
        // Two nodes: batch source + kafka source. Kafka wins → continuous,
        // because you can't safely treat a job with a live tail as "done."
        JobSpec spec = new JobSpec("mixed", "1", List.of(
                node("batch", new SourceSpec.NdjsonFile("/tmp/x")),
                node("stream", new SourceSpec.Kafka("l:9092", "t", "g"))
        ));
        assertThat(JobRunner.lifecycleOf(spec)).isEqualTo("CONTINUOUS");
    }

    @Test
    void empty_spec_is_terminating() {
        JobSpec spec = new JobSpec("empty", "1", List.of());
        assertThat(JobRunner.lifecycleOf(spec)).isEqualTo("TERMINATING");
    }

    private JobSpec job(SourceSpec src) {
        return new JobSpec("t", "1", List.of(node("only", src)));
    }

    private NodeSpec node(String id, SourceSpec src) {
        return new NodeSpec(id, null,
                new PipelineSpec(src, List.<StepSpec>of(), null, List.<SinkSpec>of()),
                List.<String>of());
    }
}
