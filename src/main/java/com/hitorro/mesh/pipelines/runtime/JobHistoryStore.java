/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * NDJSON-backed append-only log of every completed pipeline run. One
 * line per job — {@code JobStatus.Snapshot} + a couple of run-summary
 * counters. Survives driver restarts; the {@link JobRegistry} replays
 * the tail of this file into its in-memory ring on boot so the Pipelines
 * / Jobs UI still shows recent history immediately.
 *
 * <p>All writes are best-effort (a broken filesystem should not fail a
 * job that otherwise succeeded). Reads are used only at startup.</p>
 */
public final class JobHistoryStore {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Path file;

    public JobHistoryStore(Path file) {
        this.file = file;
        try { Files.createDirectories(file.getParent()); } catch (IOException ignore) {}
    }

    /** Append one job's terminal snapshot as a single NDJSON line. */
    public synchronized void append(JobStatus s) {
        try {
            JobStatus.Snapshot snap = s.snapshot();
            // Sum sink counts into a compact "sinks" summary field so
            // the UI can render totals without expanding every node.
            long totalRowsIn = 0, totalRowsOut = 0;
            for (var n : snap.nodes()) {
                totalRowsIn  += n.rowsIn();
                totalRowsOut += n.rowsOut();
            }
            long durationMs = s.finishedAt == null ? 0
                    : java.time.Duration.between(s.startedAt, s.finishedAt).toMillis();
            var entry = new HistoryEntry(
                    snap.jobId(), snap.jobSpecName(), snap.state(),
                    snap.startedAt(), snap.finishedAt(), durationMs,
                    snap.error(), totalRowsIn, totalRowsOut,
                    snap.nodes().size(), snap.nodes());
            String line = JSON.writeValueAsString(entry) + "\n";
            Files.write(file, line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            // Never fail the job because history-write failed.
            System.err.println("[JobHistoryStore] append failed: " + e.getMessage());
        }
    }

    /** Load the tail N entries (or fewer if the file is smaller). */
    public List<HistoryEntry> tail(int n) {
        if (!Files.isRegularFile(file)) return List.of();
        try {
            List<String> all = Files.readAllLines(file, StandardCharsets.UTF_8);
            int start = Math.max(0, all.size() - n);
            List<HistoryEntry> out = new ArrayList<>();
            for (int i = start; i < all.size(); i++) {
                String line = all.get(i).trim();
                if (line.isEmpty()) continue;
                try { out.add(JSON.readValue(line, HistoryEntry.class)); }
                catch (Exception ignore) { /* skip malformed */ }
            }
            return out;
        } catch (IOException e) {
            return List.of();
        }
    }

    public Path file() { return file; }

    public record HistoryEntry(
            String jobId, String jobSpecName, String state,
            String startedAt, String finishedAt, long durationMs,
            String error, long totalRowsIn, long totalRowsOut,
            int nodeCount, List<JobStatus.NodeStatusSnapshot> nodes
    ) {
        /** Compact constructor for Jackson — record's canonical constructor. */
        public HistoryEntry {
            if (nodes == null) nodes = List.of();
        }
        /** Approx "timestamp" for sort — startedAt as epoch ms, 0 on parse fail. */
        public long startedAtEpochMs() {
            try { return Instant.parse(startedAt).toEpochMilli(); } catch (Exception e) { return 0; }
        }
    }
}
