/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Coverage for the persistent restartable-jobs registry.
 *
 * <p>Invariants pinned by these tests:</p>
 * <ul>
 *   <li>Only {@code restartable=true} specs get persisted — batch jobs
 *       are silently ignored (the store stays small)</li>
 *   <li>Snapshot survives fresh instance construction — the whole
 *       point of the store</li>
 *   <li>{@link RestartableJobStore#remove} clears the entry so a
 *       terminal job doesn't resurrect on next boot</li>
 *   <li>{@link RestartableJobStore#resumeAll} re-parses persisted text
 *       through the right front-end (YAML vs Groovy) and hands the
 *       resulting JobSpec back to the caller-provided submitter</li>
 *   <li>A corrupt snapshot file at boot doesn't crash the driver</li>
 * </ul>
 */
class RestartableJobStoreTest {

    private static final String RESTARTABLE_YAML = """
            job: streaming-forever
            restartable: true
            nodes:
              - id: n
                pipeline:
                  source: {kind: inline, rows: [{n: 1}]}
                  sinks: [{kind: counting, label: c}]
            """;

    private static final String BATCH_YAML = """
            job: one-shot
            nodes:
              - id: n
                pipeline:
                  source: {kind: inline, rows: []}
                  sinks: [{kind: counting, label: c}]
            """;

    @Test
    void record_onlyPersists_restartableTrue(@TempDir Path tmp) throws Exception {
        RestartableJobStore store = new RestartableJobStore(tmp.resolve("r.json"));

        store.record("job-1", JobSpecYaml.parse(RESTARTABLE_YAML), RESTARTABLE_YAML, "yaml");
        store.record("job-2", JobSpecYaml.parse(BATCH_YAML),       BATCH_YAML,       "yaml");

        assertThat(store.size()).isEqualTo(1);
        assertThat(store.pending()).extracting(RestartableJobStore.Entry::jobId)
                .containsExactly("job-1");
    }

    @Test
    void record_survivesReloadAsFreshInstance(@TempDir Path tmp) throws Exception {
        Path f = tmp.resolve("r.json");
        RestartableJobStore a = new RestartableJobStore(f);
        a.record("job-1", JobSpecYaml.parse(RESTARTABLE_YAML), RESTARTABLE_YAML, "yaml");
        assertThat(Files.exists(f)).isTrue();

        // Fresh instance — simulates a driver restart.
        RestartableJobStore b = new RestartableJobStore(f);
        assertThat(b.size()).isEqualTo(1);
        assertThat(b.pending().get(0).specName()).isEqualTo("streaming-forever");
    }

    @Test
    void remove_clearsEntry_soTerminalJobIsntResumed(@TempDir Path tmp) throws Exception {
        RestartableJobStore store = new RestartableJobStore(tmp.resolve("r.json"));
        store.record("job-1", JobSpecYaml.parse(RESTARTABLE_YAML), RESTARTABLE_YAML, "yaml");
        assertThat(store.size()).isEqualTo(1);

        store.remove("job-1");
        assertThat(store.size()).isEqualTo(0);

        // And the on-disk snapshot reflects the removal — a driver
        // restart after remove() must NOT see the entry come back.
        RestartableJobStore reloaded = new RestartableJobStore(tmp.resolve("r.json"));
        assertThat(reloaded.size()).isEqualTo(0);
    }

    @Test
    void remove_unknownJobId_isNoOp(@TempDir Path tmp) {
        RestartableJobStore store = new RestartableJobStore(tmp.resolve("r.json"));
        store.remove("never-registered");           // must not throw
        assertThat(store.size()).isEqualTo(0);
    }

    @Test
    void resumeAll_reparsesAndCallsSubmitter(@TempDir Path tmp) throws Exception {
        RestartableJobStore store = new RestartableJobStore(tmp.resolve("r.json"));
        store.record("job-1", JobSpecYaml.parse(RESTARTABLE_YAML), RESTARTABLE_YAML, "yaml");

        List<String> submitted = new ArrayList<>();
        List<String> resumedIds = store.resumeAll((jobId, spec) -> {
            submitted.add(jobId);
            assertThat(spec.id()).isEqualTo("streaming-forever");
            assertThat(spec.restartable()).isTrue();
        });

        assertThat(submitted).containsExactly("job-1");
        assertThat(resumedIds).containsExactly("job-1");
    }

    @Test
    void resumeAll_groovySpec_reparsedViaGroovyFrontEnd(@TempDir Path tmp) throws Exception {
        String groovy = """
                job('gstream') {
                    node('n') {
                        source inline: [[x: 1]]
                        sink counting: 'c'
                    }
                }
                """;
        // The spec built here is restartable=true — set on the parsed
        // JobSpec, not in the script (Groovy DSL doesn't expose the flag
        // yet; that's future work). Simulate the run-groovy path where
        // we're passed a manually-marked restartable spec.
        JobSpec parsed = com.hitorro.mesh.pipelines.parse.JobSpecGroovy.parse(groovy);
        JobSpec restartable = new JobSpec(parsed.id(), parsed.version(),
                parsed.nodes(), true);

        RestartableJobStore store = new RestartableJobStore(tmp.resolve("r.json"));
        store.record("job-g", restartable, groovy, "groovy");

        List<JobSpec> got = new ArrayList<>();
        store.resumeAll((jobId, spec) -> got.add(spec));
        assertThat(got).hasSize(1);
        assertThat(got.get(0).id()).isEqualTo("gstream");
    }

    @Test
    void resumeAll_skipsCorruptEntry_continuesOthers(@TempDir Path tmp) throws Exception {
        RestartableJobStore store = new RestartableJobStore(tmp.resolve("r.json"));
        store.record("job-good", JobSpecYaml.parse(RESTARTABLE_YAML), RESTARTABLE_YAML, "yaml");
        // Inject a corrupt entry directly.
        store.record("job-bad", JobSpecYaml.parse(RESTARTABLE_YAML),
                "not valid yaml [[[", "yaml");

        List<String> submitted = new ArrayList<>();
        store.resumeAll((jobId, spec) -> submitted.add(jobId));
        // Good one submitted; bad one skipped (logged to stderr, not thrown).
        assertThat(submitted).contains("job-good");
        assertThat(submitted).doesNotContain("job-bad");
    }

    @Test
    void corruptSnapshotFile_startsFresh(@TempDir Path tmp) throws Exception {
        Path f = tmp.resolve("r.json");
        Files.writeString(f, "not { valid json [[[");
        RestartableJobStore store = new RestartableJobStore(f);
        assertThat(store.size()).isEqualTo(0);
        // Next record() overwrites the corrupt bytes with real content.
        store.record("job-1", JobSpecYaml.parse(RESTARTABLE_YAML), RESTARTABLE_YAML, "yaml");
        assertThat(new RestartableJobStore(f).size()).isEqualTo(1);
    }
}
