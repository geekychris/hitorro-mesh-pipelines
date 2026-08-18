/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ring-buffer + history-replay semantics for the in-memory job
 * registry. Two invariants worth pinning:
 *
 * <ul>
 *   <li>Ring-buffer eviction never drops a RUNNING job — otherwise a
 *       long-lived streaming job could get evicted mid-run and the
 *       driver would lose track of it.</li>
 *   <li>On construction with a persistent {@link JobHistoryStore},
 *       the tail of that history replays as compact placeholder
 *       JobStatus entries so the UI shows recent runs immediately
 *       after a driver restart.</li>
 * </ul>
 */
class JobRegistryTest {

    @Test
    void register_getRoundTrips() {
        JobRegistry r = new JobRegistry(10);
        JobStatus s = new JobStatus("job-1", "spec-a");
        r.register(s);
        assertThat(r.get("job-1")).isSameAs(s);
    }

    @Test
    void list_returnsMostRecentFirst() {
        JobRegistry r = new JobRegistry(10);
        r.register(new JobStatus("job-1", "spec"));
        r.register(new JobStatus("job-2", "spec"));
        r.register(new JobStatus("job-3", "spec"));

        List<JobStatus> listed = r.list();
        assertThat(listed).extracting(js -> js.jobId)
                .containsExactly("job-3", "job-2", "job-1");
    }

    @Test
    void ringBuffer_evictsOldestCompleted_whenAtCap() {
        JobRegistry r = new JobRegistry(3);
        for (int i = 1; i <= 5; i++) {
            JobStatus s = new JobStatus("job-" + i, "spec");
            s.state = JobStatus.State.SUCCEEDED;
            r.register(s);
        }
        assertThat(r.list()).extracting(js -> js.jobId)
                .containsExactly("job-5", "job-4", "job-3");
        // Older completed runs are gone.
        assertThat(r.get("job-1")).isNull();
        assertThat(r.get("job-2")).isNull();
    }

    @Test
    void ringBuffer_neverEvictsRunning() {
        // A long-lived streaming job registered early must stay live
        // even when the cap is exceeded — otherwise the driver loses
        // its handle on active work.
        JobRegistry r = new JobRegistry(3);
        JobStatus running = new JobStatus("stream-1", "spec");
        running.state = JobStatus.State.RUNNING;
        r.register(running);

        for (int i = 1; i <= 10; i++) {
            JobStatus s = new JobStatus("batch-" + i, "spec");
            s.state = JobStatus.State.SUCCEEDED;
            r.register(s);
        }
        assertThat(r.get("stream-1")).isNotNull();
        assertThat(r.get("stream-1").state).isEqualTo(JobStatus.State.RUNNING);
    }

    @Test
    void onTerminal_persistsToHistory_whenAttached(@TempDir Path tmp) {
        JobHistoryStore hist = new JobHistoryStore(tmp.resolve("jobs.ndjson"));
        JobRegistry r = new JobRegistry(10, hist);

        JobStatus done = new JobStatus("job-1", "spec");
        done.state = JobStatus.State.SUCCEEDED;
        done.finishedAt = Instant.now();
        r.register(done);
        r.onTerminal(done);

        assertThat(hist.tail(10)).extracting(JobHistoryStore.HistoryEntry::jobId)
                .containsExactly("job-1");
    }

    @Test
    void onTerminal_doesNotPersistNonTerminalStates(@TempDir Path tmp) {
        // Guardrail: appending a RUNNING or PENDING job to the log
        // would pollute the boot replay with jobs that never actually
        // finished.
        JobHistoryStore hist = new JobHistoryStore(tmp.resolve("jobs.ndjson"));
        JobRegistry r = new JobRegistry(10, hist);

        JobStatus running = new JobStatus("job-r", "spec");
        running.state = JobStatus.State.RUNNING;
        r.onTerminal(running);

        JobStatus pending = new JobStatus("job-p", "spec");
        pending.state = JobStatus.State.PENDING;
        r.onTerminal(pending);

        assertThat(hist.tail(10)).isEmpty();
    }

    @Test
    void onTerminal_nullHistory_isSafeNoOp() {
        // No history attached → onTerminal is a no-op, doesn't NPE.
        JobRegistry r = new JobRegistry(10);
        JobStatus s = new JobStatus("job-1", "spec");
        s.state = JobStatus.State.SUCCEEDED;
        r.onTerminal(s);   // must not throw
    }

    @Test
    void construct_replaysHistoryTailIntoMemory(@TempDir Path tmp) {
        // Simulate a prior driver: write 3 completed jobs to history,
        // then boot a fresh JobRegistry pointing at that same history.
        // The registry must expose those 3 jobs via list() immediately,
        // so the UI's recent-runs view isn't blank until the first new
        // job runs.
        JobHistoryStore hist = new JobHistoryStore(tmp.resolve("jobs.ndjson"));
        for (int i = 1; i <= 3; i++) {
            JobStatus s = new JobStatus("job-" + i, "spec-a");
            s.state = JobStatus.State.SUCCEEDED;
            s.finishedAt = Instant.now();
            hist.append(s);
        }

        JobRegistry booted = new JobRegistry(10, hist);
        List<JobStatus> visible = booted.list();
        assertThat(visible).hasSize(3);
        assertThat(visible).extracting(js -> js.jobId)
                .containsExactly("job-3", "job-2", "job-1");   // reversed
        // Every replayed job carries a terminal state — no PENDING.
        assertThat(visible).allSatisfy(js -> assertThat(js.state)
                .isEqualTo(JobStatus.State.SUCCEEDED));
    }

    @Test
    void construct_replayHonoursCapacity(@TempDir Path tmp) {
        // History has 5 entries but the ring cap is 3 — replay should
        // load only the last 3 (matching what a fresh boot would need
        // to render).
        JobHistoryStore hist = new JobHistoryStore(tmp.resolve("jobs.ndjson"));
        for (int i = 1; i <= 5; i++) {
            JobStatus s = new JobStatus("job-" + i, "spec");
            s.state = JobStatus.State.SUCCEEDED;
            s.finishedAt = Instant.now();
            hist.append(s);
        }

        JobRegistry booted = new JobRegistry(3, hist);
        assertThat(booted.list()).extracting(js -> js.jobId)
                .containsExactly("job-5", "job-4", "job-3");
    }

    // -------------------------------------------------- restartable-store wiring

    @Test
    void registerRestartable_persists_thenOnTerminalRemoves(@TempDir Path tmp) throws Exception {
        RestartableJobStore store = new RestartableJobStore(tmp.resolve("r.json"));
        JobRegistry r = new JobRegistry(10, null, store);

        String yaml = """
                job: streaming
                restartable: true
                nodes:
                  - id: n
                    pipeline: {source: {kind: inline, rows: []}, sinks: []}
                """;
        var spec = com.hitorro.mesh.pipelines.parse.JobSpecYaml.parse(yaml);
        r.registerRestartable("job-42", spec, yaml, "yaml");
        assertThat(store.size()).isEqualTo(1);

        // Simulate the job terminating — must be removed from the
        // restart set so the next driver boot doesn't resurrect it.
        JobStatus done = new JobStatus("job-42", "streaming");
        done.state = JobStatus.State.SUCCEEDED;
        r.onTerminal(done);
        assertThat(store.size()).isEqualTo(0);
    }

    @Test
    void onTerminal_nullRestartableStore_isSafeNoOp() {
        // Registry constructed without a restartable store must not
        // NPE on terminal callback.
        JobRegistry r = new JobRegistry(10);
        JobStatus s = new JobStatus("job-1", "spec");
        s.state = JobStatus.State.SUCCEEDED;
        r.onTerminal(s);   // must not throw
    }

    @Test
    void construct_replay_survivesUnrecognisedStateLabel(@TempDir Path tmp) {
        // A future driver ships a new State enum value (say, PAUSED);
        // rollback to an older driver must not crash reading the log.
        // The parser defaults to SUCCEEDED per the code — proving that
        // rather than a hard failure.
        JobHistoryStore hist = new JobHistoryStore(tmp.resolve("jobs.ndjson"));
        // Hand-write a HistoryEntry with a bogus state value.
        var raw = new JobHistoryStore.HistoryEntry(
                "job-x", "spec", "PAUSED_FUTURE",
                Instant.now().toString(), Instant.now().toString(), 5L,
                null, 0L, 0L, 0, List.of());
        try {
            var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            java.nio.file.Files.writeString(hist.file(),
                    mapper.writeValueAsString(raw) + "\n");
        } catch (Exception e) { throw new RuntimeException(e); }

        JobRegistry booted = new JobRegistry(5, hist);
        JobStatus recovered = booted.get("job-x");
        assertThat(recovered).isNotNull();
        assertThat(recovered.state).isEqualTo(JobStatus.State.SUCCEEDED);   // fallback
    }
}
