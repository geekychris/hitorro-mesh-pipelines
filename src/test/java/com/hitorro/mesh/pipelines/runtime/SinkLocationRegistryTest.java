/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the sink→agent locality registry used by
 * {@link PipelineScheduler#pickAgent} to co-locate downstream nodes
 * with the agent holding their upstream KV / Lucene sink. The
 * invariant this class guarantees: once agent-X wrote to
 * (kvstore, users), a later {@code locate("kvstore", "users")} must
 * return "agent-X" for the lifetime of the registry — AND across
 * driver restarts when a snapshotPath is configured.
 */
class SinkLocationRegistryTest {

    @Test
    void inMemory_recordAndLocate() {
        SinkLocationRegistry r = new SinkLocationRegistry();
        r.record("kvstore", "users", "agent-us-east");
        r.record("lucene",  "search", "agent-us-west");

        assertThat(r.locate("kvstore", "users")).isEqualTo("agent-us-east");
        assertThat(r.locate("lucene",  "search")).isEqualTo("agent-us-west");
        assertThat(r.locate("kvstore", "unknown")).isNull();
    }

    @Test
    void locate_nullName_returnsNull() {
        SinkLocationRegistry r = new SinkLocationRegistry();
        assertThat(r.locate("kvstore", null)).isNull();
    }

    @Test
    void record_nullNameOrAgent_isIgnored() {
        // Silent no-op instead of NPE — the scheduler calls this per node
        // and a spec with an unnamed sink shouldn't crash the run.
        SinkLocationRegistry r = new SinkLocationRegistry();
        r.record("kvstore", null, "agent-1");
        r.record("kvstore", "x",  null);
        assertThat(r.snapshot()).isEmpty();
    }

    @Test
    void kindDisambiguates_sameName() {
        // A KV store and a Lucene index can share a name — the composite
        // key ("kind:name") must keep them distinct.
        SinkLocationRegistry r = new SinkLocationRegistry();
        r.record("kvstore", "articles", "agent-1");
        r.record("lucene",  "articles", "agent-2");
        assertThat(r.locate("kvstore", "articles")).isEqualTo("agent-1");
        assertThat(r.locate("lucene",  "articles")).isEqualTo("agent-2");
    }

    @Test
    void record_secondWrite_overwritesAgent() {
        // Re-run: kvstore:users was on agent-1, now moved to agent-2.
        // Last-writer-wins keeps locality tracking accurate as data moves.
        SinkLocationRegistry r = new SinkLocationRegistry();
        r.record("kvstore", "users", "agent-1");
        r.record("kvstore", "users", "agent-2");
        assertThat(r.locate("kvstore", "users")).isEqualTo("agent-2");
    }

    @Test
    void snapshot_isDefensiveCopy() {
        SinkLocationRegistry r = new SinkLocationRegistry();
        r.record("kvstore", "users", "agent-1");
        var snap = r.snapshot();
        // snapshot() must return Map.copyOf — mutation would fail.
        assertThat(snap).isUnmodifiable();
        assertThat(snap).containsEntry("kvstore:users", "agent-1");
    }

    // -------------------------------------------------- persistence

    @Test
    void onDisk_recordSurvivesReload(@TempDir Path tmp) {
        Path snap = tmp.resolve("sink-locs.json");
        SinkLocationRegistry a = new SinkLocationRegistry(snap);
        a.record("kvstore", "users",   "agent-a");
        a.record("lucene",  "search",  "agent-b");
        assertThat(Files.exists(snap)).isTrue();

        // Fresh instance reads the snapshot on construction.
        SinkLocationRegistry b = new SinkLocationRegistry(snap);
        assertThat(b.locate("kvstore", "users")).isEqualTo("agent-a");
        assertThat(b.locate("lucene",  "search")).isEqualTo("agent-b");
    }

    @Test
    void onDisk_corruptSnapshot_startsFresh(@TempDir Path tmp) throws Exception {
        // A garbled file (partial write, disk full, wrong version) must
        // not crash the driver at boot. Registry starts empty; next
        // successful record overwrites the corrupt bytes.
        Path snap = tmp.resolve("sink-locs.json");
        Files.writeString(snap, "not valid json {[");

        SinkLocationRegistry r = new SinkLocationRegistry(snap);
        assertThat(r.snapshot()).isEmpty();
        r.record("kvstore", "u", "agent-1");
        assertThat(r.locate("kvstore", "u")).isEqualTo("agent-1");

        // File is now a real snapshot.
        SinkLocationRegistry reloaded = new SinkLocationRegistry(snap);
        assertThat(reloaded.locate("kvstore", "u")).isEqualTo("agent-1");
    }

    @Test
    void onDisk_snapshotDirAutoCreated(@TempDir Path tmp) {
        // record() calls persist() which calls Files.createDirectories on
        // the parent. A snapshotPath deep under a nonexistent dir must
        // still work on first write.
        Path deep = tmp.resolve("nested/deep/sink-locs.json");
        SinkLocationRegistry r = new SinkLocationRegistry(deep);
        r.record("kvstore", "x", "agent-1");
        assertThat(Files.exists(deep)).isTrue();
    }
}
