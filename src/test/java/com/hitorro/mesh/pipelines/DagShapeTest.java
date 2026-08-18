/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines;

import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.JobStatus;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the DAG-shape metadata JobRunner writes onto each
 * {@link JobStatus.NodeStatus} before any node runs — {@code deps} +
 * {@code rank}. This is what the driver-app UI reads to render a
 * topological graph (columns per rank, arrows between deps).
 *
 * <p>Shape mismatch here is a real UI regression — pollers would draw
 * nodes in wrong columns or lose arrows.</p>
 */
class DagShapeTest {

    @Test
    void diamond_dag_populates_deps_and_ranks(@TempDir Path home) throws Exception {
        // Classic diamond:  A → B , A → C , B → D , C → D
        // Ranks: A=0, {B,C}=1, D=2
        String yaml = """
                job: diamond
                nodes:
                  - id: A
                    pipeline: {source: {kind: inline, rows: [{n: 1}]}, sinks: []}
                  - id: B
                    depends: [A]
                    pipeline: {source: {kind: ref, node: A}, sinks: []}
                  - id: C
                    depends: [A]
                    pipeline: {source: {kind: ref, node: A}, sinks: []}
                  - id: D
                    depends: [B, C]
                    pipeline: {source: {kind: ref, node: B}, sinks: []}
                """;
        JobSpec spec = JobSpecYaml.parse(yaml);
        try (JobRunner runner = new JobRunner(new SinkRegistry(home))) {
            JobStatus status = runner.run(spec);

            assertThat(status.state).isEqualTo(JobStatus.State.SUCCEEDED);

            // Ranks — Kahn assigns 0 to sources, then propagates.
            assertThat(status.node("A").rank).isEqualTo(0);
            assertThat(status.node("B").rank).isEqualTo(1);
            assertThat(status.node("C").rank).isEqualTo(1);
            assertThat(status.node("D").rank).isEqualTo(2);

            // Deps — verbatim what the spec declared.
            assertThat(status.node("A").deps).isEmpty();
            assertThat(status.node("B").deps).containsExactly("A");
            assertThat(status.node("C").deps).containsExactly("A");
            assertThat(status.node("D").deps).containsExactly("B", "C");

            // Snapshot round-trips the same info (this is what the UI consumes).
            var snap = status.snapshot();
            var byId = snap.nodes().stream().collect(
                    java.util.stream.Collectors.toMap(n -> n.id(), n -> n));
            assertThat(byId.get("D").deps()).containsExactly("B", "C");
            assertThat(byId.get("D").rank()).isEqualTo(2);
            assertThat(byId.get("A").deps()).isEmpty();
            assertThat(byId.get("A").rank()).isEqualTo(0);
        }
    }

    @Test
    void single_node_job_has_rank_zero_and_no_deps(@TempDir Path home) throws Exception {
        String yaml = """
                job: solo
                nodes:
                  - id: only
                    pipeline: {source: {kind: inline, rows: [{n: 1}]}, sinks: []}
                """;
        try (JobRunner runner = new JobRunner(new SinkRegistry(home))) {
            JobStatus status = runner.run(JobSpecYaml.parse(yaml));
            assertThat(status.node("only").rank).isEqualTo(0);
            assertThat(status.node("only").deps).isEmpty();
        }
    }

    @Test
    void ranks_and_deps_populated_before_any_node_runs(@TempDir Path home) throws Exception {
        // Deeper chain to prove rank increments correctly.  a → b → c → d
        String yaml = """
                job: chain
                nodes:
                  - id: a
                    pipeline: {source: {kind: inline, rows: [{n: 1}]}, sinks: []}
                  - id: b
                    depends: [a]
                    pipeline: {source: {kind: ref, node: a}, sinks: []}
                  - id: c
                    depends: [b]
                    pipeline: {source: {kind: ref, node: b}, sinks: []}
                  - id: d
                    depends: [c]
                    pipeline: {source: {kind: ref, node: c}, sinks: []}
                """;
        try (JobRunner runner = new JobRunner(new SinkRegistry(home))) {
            JobStatus status = runner.run(JobSpecYaml.parse(yaml));
            assertThat(status.node("a").rank).isEqualTo(0);
            assertThat(status.node("b").rank).isEqualTo(1);
            assertThat(status.node("c").rank).isEqualTo(2);
            assertThat(status.node("d").rank).isEqualTo(3);
        }
    }
}
