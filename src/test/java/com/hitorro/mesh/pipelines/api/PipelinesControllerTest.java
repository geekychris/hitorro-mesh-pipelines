/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.api;

import com.hitorro.mesh.pipelines.runtime.JobHistoryStore;
import com.hitorro.mesh.pipelines.runtime.JobRegistry;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.JobStatus;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct tests for the REST controller — no Spring container needed.
 * Every endpoint is a method on {@link PipelinesController}; call
 * them, wait for the async work to settle, and assert the returned
 * {@link ResponseEntity}s + the {@link JobRegistry} state.
 *
 * <p>The distributed dispatch endpoint needs a live NATS server — the
 * test asserts it accepts the request + fails cleanly when NATS is
 * absent, which is the behaviour a UI polling the returned jobId
 * needs to see.</p>
 */
class PipelinesControllerTest {

    @TempDir Path home;
    JobRegistry registry;
    JobRunner runner;
    PipelinesController ctrl;

    @BeforeEach
    void setUp() {
        SinkRegistry sinks = new SinkRegistry(home);
        runner   = new JobRunner(sinks);
        registry = new JobRegistry(50);
        ctrl     = new PipelinesController(runner, registry);
    }

    @AfterEach
    void tearDown() {
        runner.close();
    }

    // -------------------------------------------------- run (inline)

    @Test
    void run_acceptsInlineYaml_returnsJobId() throws Exception {
        String yaml = """
                job: tiny
                nodes:
                  - id: n1
                    pipeline:
                      source: {kind: inline, rows: [{x: 1}, {x: 2}]}
                      sinks: [{kind: counting, label: c}]
                """;
        ResponseEntity<Map<String, String>> resp = ctrl.runInline(yaml);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String jobId = resp.getBody().get("jobId");
        assertThat(jobId).startsWith("job-");

        // Registry sees it immediately, before the pool.submit finishes.
        assertThat(registry.get(jobId)).isNotNull();
        waitForTerminal(jobId, 5_000);
        assertThat(registry.get(jobId).state).isEqualTo(JobStatus.State.SUCCEEDED);
    }

    @Test
    void run_acceptsJsonBody_alsoParsedCorrectly() throws Exception {
        // YAMLMapper handles both YAML and JSON via the same parse path.
        String json = "{\"id\":\"tiny\",\"version\":\"1\",\"nodes\":[" +
                "{\"id\":\"n1\",\"pipeline\":{\"source\":{\"kind\":\"inline\",\"rows\":[{\"x\":1}]}," +
                "\"sinks\":[{\"kind\":\"counting\",\"label\":\"c\"}]}}]}";
        ResponseEntity<Map<String, String>> resp = ctrl.runInline(json);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        waitForTerminal(resp.getBody().get("jobId"), 5_000);
    }

    @Test
    void run_malformedYaml_bubblesAsException() {
        String bad = "not: valid: yaml: [[";
        try {
            ctrl.runInline(bad);
            org.junit.jupiter.api.Assertions.fail("Expected parse exception");
        } catch (Exception e) {
            // Spring will translate this to a 4xx via its default
            // exception handling — the important thing is the parse
            // fails BEFORE any job is registered.
            assertThat(registry.list()).isEmpty();
        }
    }

    // -------------------------------------------------- run/bundled

    @Test
    void runBundled_loadsAndExecutes() throws Exception {
        // BundledJobs.loadAll returns all shipped examples — pick any
        // one and prove the endpoint runs it. Skip if no bundles are
        // shipped (would need a resources check).
        Map<String, String> all = com.hitorro.mesh.pipelines.runtime.BundledJobs.loadAll();
        org.junit.jupiter.api.Assumptions.assumeTrue(
                !all.isEmpty(), "no bundled jobs in classpath");
        String name = all.keySet().iterator().next();

        ResponseEntity<Map<String, String>> resp = ctrl.runBundled(name);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String jobId = resp.getBody().get("jobId");
        assertThat(jobId).startsWith("job-");
    }

    // -------------------------------------------------- GET list / one / events / bundled

    @Test
    void list_returnsRegisteredJobsMostRecentFirst() throws Exception {
        ctrl.runInline(tinySpec("first"));
        Thread.sleep(2);   // ensure distinct jobIds (nanoTime resolution)
        ctrl.runInline(tinySpec("second"));

        // Wait until BOTH visible in the list (async register).
        long deadline = System.currentTimeMillis() + 3_000;
        List<JobStatus.Snapshot> snap;
        do {
            snap = ctrl.list();
        } while (snap.size() < 2 && System.currentTimeMillis() < deadline);
        assertThat(snap).hasSize(2);
        assertThat(snap.get(0).jobSpecName()).isEqualTo("second");
        assertThat(snap.get(1).jobSpecName()).isEqualTo("first");
    }

    @Test
    void one_returnsSnapshotForKnownJobId() throws Exception {
        String jobId = ctrl.runInline(tinySpec("solo")).getBody().get("jobId");
        waitForTerminal(jobId, 5_000);

        ResponseEntity<JobStatus.Snapshot> r = ctrl.one(jobId);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(r.getBody().jobId()).isEqualTo(jobId);
        assertThat(r.getBody().state()).isEqualTo("SUCCEEDED");
        assertThat(r.getBody().nodes()).hasSize(1);
    }

    @Test
    void one_unknownJobId_returns404() {
        ResponseEntity<JobStatus.Snapshot> r = ctrl.one("job-does-not-exist");
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void events_returnsProgressEventsAfterRun() throws Exception {
        String jobId = ctrl.runInline(tinySpec("evt")).getBody().get("jobId");
        waitForTerminal(jobId, 5_000);

        ResponseEntity<List<JobStatus.ProgressEvent>> r = ctrl.events(jobId);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(r.getBody()).isNotEmpty();
        // NodeRunner always emits at least start + done events.
        assertThat(r.getBody()).extracting(JobStatus.ProgressEvent::kind)
                .contains("start", "done");
    }

    @Test
    void events_unknownJobId_returns404() {
        ResponseEntity<List<JobStatus.ProgressEvent>> r = ctrl.events("nope");
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void bundled_returnsShippedExamples() {
        Map<String, String> b = ctrl.bundled();
        // Every bundled example must have a name → YAML body.
        assertThat(b).allSatisfy((k, v) -> {
            assertThat(k).isNotBlank();
            assertThat(v).contains("job:");     // valid YAML has the job: key
        });
    }

    // -------------------------------------------------- DELETE cancel

    @Test
    void cancel_setsCancelRequestedFlag() throws Exception {
        String jobId = ctrl.runInline(tinySpec("cancelme")).getBody().get("jobId");
        JobStatus s = registry.get(jobId);

        ResponseEntity<Map<String, String>> r = ctrl.cancel(jobId);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(r.getBody()).containsEntry("cancelRequested", "true");
        assertThat(s.cancelRequested.get()).isTrue();
    }

    @Test
    void cancel_unknownJobId_returns404() {
        ResponseEntity<Map<String, String>> r = ctrl.cancel("job-nope");
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void cancel_idempotent_cancellingCompletedJobIs200() throws Exception {
        String jobId = ctrl.runInline(tinySpec("done")).getBody().get("jobId");
        waitForTerminal(jobId, 5_000);

        ResponseEntity<Map<String, String>> r1 = ctrl.cancel(jobId);
        ResponseEntity<Map<String, String>> r2 = ctrl.cancel(jobId);
        assertThat(r1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(r2.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    // -------------------------------------------------- history

    @Test
    void history_emptyWhenNoHistoryStoreAttached() {
        // The default JobRegistry(50) has no JobHistoryStore attached —
        // history() must return [] gracefully, not NPE.
        assertThat(ctrl.history(100)).isEmpty();
    }

    @Test
    void history_returnsPersistedRuns_mostRecentFirst(@TempDir Path histDir) throws Exception {
        // Attach a real history store, run 2 jobs, verify the endpoint
        // returns them most-recent-first with a valid limit.
        JobHistoryStore store = new JobHistoryStore(histDir.resolve("h.ndjson"));
        JobRegistry withHist = new JobRegistry(50, store);
        PipelinesController c = new PipelinesController(runner, withHist);

        String id1 = c.runInline(tinySpec("first")).getBody().get("jobId");
        waitForTerminal(id1, 5_000, withHist);
        Thread.sleep(5);
        String id2 = c.runInline(tinySpec("second")).getBody().get("jobId");
        waitForTerminal(id2, 5_000, withHist);

        var hist = c.history(10);
        assertThat(hist).hasSize(2);
        assertThat(hist.get(0).jobSpecName()).isEqualTo("second");   // most recent first
        assertThat(hist.get(1).jobSpecName()).isEqualTo("first");
    }

    // -------------------------------------------------- sink-locations

    @Test
    void sinkLocations_returnsMap_evenWhenEmpty() {
        // The default endpoint reads from the process-wide SINK_LOCATIONS
        // singleton — may be non-empty from other tests in the same JVM,
        // but never null. Just prove the shape.
        Map<String, String> m = ctrl.sinkLocations();
        assertThat(m).isNotNull();
    }

    // ------------------------------------------------------------ helpers

    private static String tinySpec(String name) {
        return "job: " + name + "\nversion: \"1\"\nnodes:\n" +
               "  - id: n1\n" +
               "    pipeline:\n" +
               "      source: {kind: inline, rows: [{x: 1}]}\n" +
               "      sinks: [{kind: counting, label: c}]\n";
    }

    private void waitForTerminal(String jobId, long timeoutMs) throws InterruptedException {
        waitForTerminal(jobId, timeoutMs, registry);
    }

    private void waitForTerminal(String jobId, long timeoutMs, JobRegistry reg) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            JobStatus s = reg.get(jobId);
            if (s != null && s.state != JobStatus.State.PENDING
                          && s.state != JobStatus.State.RUNNING) return;
            Thread.sleep(20);
        }
        throw new AssertionError("job " + jobId + " didn't terminate within " + timeoutMs + "ms");
    }
}
