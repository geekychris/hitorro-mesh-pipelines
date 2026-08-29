/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointStoreTest {

    @Test
    void empty_when_never_set(@TempDir Path tmp) throws IOException {
        CheckpointStore c = new CheckpointStore(tmp);
        assertThat(c.get("unset")).isEmpty();
    }

    @Test
    void put_get_round_trip(@TempDir Path tmp) throws IOException {
        CheckpointStore c = new CheckpointStore(tmp);
        c.put("s", "2026-08-29T12:00:00Z");
        assertThat(c.get("s")).contains("2026-08-29T12:00:00Z");
    }

    @Test
    void put_null_stores_empty_string(@TempDir Path tmp) throws IOException {
        CheckpointStore c = new CheckpointStore(tmp);
        c.put("s", null);
        assertThat(c.get("s")).contains("");
    }

    @Test
    void unsafe_names_are_sanitized_on_disk(@TempDir Path tmp) throws IOException {
        CheckpointStore c = new CheckpointStore(tmp);
        c.put("weird/name with spaces & symbols!", "cp");
        assertThat(c.get("weird/name with spaces & symbols!")).contains("cp");
        // No traversal — file must live under checkpoints/
        Path cpDir = tmp.resolve("checkpoints");
        try (var files = Files.list(cpDir)) {
            long count = files.count();
            assertThat(count).isEqualTo(1);
        }
    }

    @Test
    void remove_deletes_file(@TempDir Path tmp) throws IOException {
        CheckpointStore c = new CheckpointStore(tmp);
        c.put("s", "cp");
        assertThat(c.get("s")).isPresent();
        c.remove("s");
        assertThat(c.get("s")).isEmpty();
    }
}
