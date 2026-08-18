/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the NDJSON append-only history log the JobRegistry replays on
 * boot. The invariant: appending a terminal JobStatus always produces
 * exactly one parseable line; {@link JobHistoryStore#tail(int)} returns
 * the last N well-formed entries in file order; corrupt lines are
 * silently skipped so a partial-write on a previous crash doesn't
 * poison the UI.
 */
class JobHistoryStoreTest {

    @Test
    void appendThenTail_roundTrips(@TempDir Path tmp) {
        Path file = tmp.resolve("jobs.ndjson");
        JobHistoryStore store = new JobHistoryStore(file);

        store.append(terminal("job-1", "spec-a", JobStatus.State.SUCCEEDED, null));
        store.append(terminal("job-2", "spec-b", JobStatus.State.FAILED, "boom"));

        List<JobHistoryStore.HistoryEntry> entries = store.tail(10);
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).jobId()).isEqualTo("job-1");
        assertThat(entries.get(0).state()).isEqualTo("SUCCEEDED");
        assertThat(entries.get(1).jobId()).isEqualTo("job-2");
        assertThat(entries.get(1).error()).isEqualTo("boom");
    }

    @Test
    void tail_returnsLastN_notFirstN(@TempDir Path tmp) {
        JobHistoryStore store = new JobHistoryStore(tmp.resolve("jobs.ndjson"));
        for (int i = 0; i < 10; i++) {
            store.append(terminal("job-" + i, "spec", JobStatus.State.SUCCEEDED, null));
        }
        List<JobHistoryStore.HistoryEntry> tail3 = store.tail(3);
        assertThat(tail3).extracting(JobHistoryStore.HistoryEntry::jobId)
                .containsExactly("job-7", "job-8", "job-9");
    }

    @Test
    void tail_missingFile_returnsEmpty(@TempDir Path tmp) {
        // Fresh driver, no history file yet — must not crash.
        JobHistoryStore store = new JobHistoryStore(tmp.resolve("nope.ndjson"));
        assertThat(store.tail(10)).isEmpty();
    }

    @Test
    void tail_skipsMalformedLines(@TempDir Path tmp) throws IOException {
        // Simulate a partial-write on a previous crash: one good line,
        // one garbled, one good. The bad line must be skipped, not fail
        // the whole tail() call.
        Path file = tmp.resolve("jobs.ndjson");
        JobHistoryStore store = new JobHistoryStore(file);
        store.append(terminal("job-1", "s", JobStatus.State.SUCCEEDED, null));
        Files.writeString(file, "this is not json\n",
                StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);
        store.append(terminal("job-2", "s", JobStatus.State.SUCCEEDED, null));

        List<JobHistoryStore.HistoryEntry> entries = store.tail(10);
        assertThat(entries).extracting(JobHistoryStore.HistoryEntry::jobId)
                .containsExactly("job-1", "job-2");
    }

    @Test
    void append_writesNodeSnapshots(@TempDir Path tmp) {
        // The whole point of the history log — remembering which nodes
        // ran, their row counts, so the UI's history view can show per-
        // node detail without keeping every JobStatus in memory.
        JobStatus s = terminal("job-1", "spec", JobStatus.State.SUCCEEDED, null);
        var n = s.node("first");
        n.state = JobStatus.State.SUCCEEDED;
        n.rowsIn = 100;
        n.rowsOut = 42;

        JobHistoryStore store = new JobHistoryStore(tmp.resolve("jobs.ndjson"));
        store.append(s);

        var entry = store.tail(1).get(0);
        assertThat(entry.nodeCount()).isEqualTo(1);
        assertThat(entry.totalRowsIn()).isEqualTo(100);
        assertThat(entry.totalRowsOut()).isEqualTo(42);
        assertThat(entry.nodes()).hasSize(1);
        assertThat(entry.nodes().get(0).id()).isEqualTo("first");
        assertThat(entry.nodes().get(0).rowsOut()).isEqualTo(42);
    }

    @Test
    void append_recordsDuration(@TempDir Path tmp) throws InterruptedException {
        JobStatus s = new JobStatus("job-1", "spec");
        Thread.sleep(15);          // ensure finishedAt > startedAt
        s.finishedAt = Instant.now();
        s.state = JobStatus.State.SUCCEEDED;

        JobHistoryStore store = new JobHistoryStore(tmp.resolve("jobs.ndjson"));
        store.append(s);
        var entry = store.tail(1).get(0);
        assertThat(entry.durationMs()).isGreaterThan(0);
    }

    @Test
    void historyEntry_startedAtEpochMs_parsesIsoInstant(@TempDir Path tmp) {
        JobStatus s = terminal("job-1", "spec", JobStatus.State.SUCCEEDED, null);
        Instant expected = s.startedAt;
        new JobHistoryStore(tmp.resolve("jobs.ndjson")).append(s);
        var entry = new JobHistoryStore(tmp.resolve("jobs.ndjson")).tail(1).get(0);
        assertThat(entry.startedAtEpochMs()).isEqualTo(expected.toEpochMilli());
    }

    @Test
    void historyEntry_startedAtEpochMs_returnsZeroOnBadInput() {
        var bad = new JobHistoryStore.HistoryEntry(
                "j", "s", "SUCCEEDED",
                "not-an-instant", null, 0L, null, 0L, 0L, 0, List.of());
        assertThat(bad.startedAtEpochMs()).isEqualTo(0L);
    }

    // ------------------------------------------------------------ helpers

    private static JobStatus terminal(String jobId, String specName,
                                      JobStatus.State state, String error) {
        JobStatus s = new JobStatus(jobId, specName);
        s.state = state;
        s.error = error;
        s.finishedAt = Instant.now();
        return s;
    }
}
