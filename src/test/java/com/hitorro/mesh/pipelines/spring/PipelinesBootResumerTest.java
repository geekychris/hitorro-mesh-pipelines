/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.spring;

import com.hitorro.mesh.pipelines.parse.JobSpecYaml;
import com.hitorro.mesh.pipelines.runtime.JobRegistry;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.JobStatus;
import com.hitorro.mesh.pipelines.runtime.RestartableJobStore;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link PipelinesAutoConfiguration.PipelinesBootResumer}
 * directly — no Spring container needed. Simulates a driver restart
 * by pre-populating a RestartableJobStore, constructing a fresh
 * runner + registry + resumer, and asserting the pending job:
 *
 * <ul>
 *   <li>Gets re-parsed from persistent text</li>
 *   <li>Runs to completion via the injected JobRunner</li>
 *   <li>Is removed from the store on terminal (no double-resume on
 *       the next boot)</li>
 * </ul>
 */
class PipelinesBootResumerTest {

    private static final String RESTARTABLE_YAML = """
            job: streamer
            restartable: true
            nodes:
              - id: n
                pipeline:
                  source: {kind: inline, rows: [{n: 1}, {n: 2}]}
                  sinks: [{kind: counting, label: c}]
            """;

    @Test
    void bootResumer_resurrectsPending_thenClearsStoreOnTerminal(@TempDir Path home) throws Exception {
        // Simulate a prior driver having persisted a restartable job.
        Path storeFile = home.resolve("r.json");
        RestartableJobStore store = new RestartableJobStore(storeFile);
        store.record("job-orig-42", JobSpecYaml.parse(RESTARTABLE_YAML),
                     RESTARTABLE_YAML, "yaml");
        assertThat(store.size()).isEqualTo(1);

        // Fresh boot — new runner + registry pointing at the same store.
        SinkRegistry sinks = new SinkRegistry(home);
        try (JobRunner runner = new JobRunner(sinks)) {
            JobRegistry registry = new JobRegistry(10, null, store);

            var resumer = new PipelinesAutoConfiguration.PipelinesBootResumer(
                    runner, registry, store);
            resumer.afterPropertiesSet();       // Spring calls this on boot

            // Immediately after resume the job is registered — its jobId
            // is the ORIGINAL one (preserved so log/metric correlation
            // survives the restart).
            long deadline = System.currentTimeMillis() + 5_000;
            JobStatus s;
            while ((s = registry.get("job-orig-42")) == null
                    && System.currentTimeMillis() < deadline) Thread.sleep(20);
            assertThat(s).as("resumed job should appear in the registry").isNotNull();

            // Wait for the resumed job to hit a terminal state.
            deadline = System.currentTimeMillis() + 5_000;
            while (s.state != JobStatus.State.SUCCEEDED
                    && s.state != JobStatus.State.FAILED
                    && System.currentTimeMillis() < deadline) Thread.sleep(20);
            assertThat(s.state).isEqualTo(JobStatus.State.SUCCEEDED);

            // Store is now empty — no second-boot double-resume.
            deadline = System.currentTimeMillis() + 2_000;
            while (store.size() > 0 && System.currentTimeMillis() < deadline) Thread.sleep(20);
            assertThat(store.size()).isEqualTo(0);
        }
    }

    @Test
    void bootResumer_noPendingJobs_isNoOp(@TempDir Path home) {
        RestartableJobStore store = new RestartableJobStore(home.resolve("r.json"));
        try (JobRunner runner = new JobRunner(new SinkRegistry(home))) {
            JobRegistry registry = new JobRegistry(10, null, store);
            var resumer = new PipelinesAutoConfiguration.PipelinesBootResumer(
                    runner, registry, store);
            resumer.afterPropertiesSet();
            assertThat(registry.list()).isEmpty();
        }
    }
}
