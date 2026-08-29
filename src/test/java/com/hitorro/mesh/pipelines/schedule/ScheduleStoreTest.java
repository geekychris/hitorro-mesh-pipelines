/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ScheduleStoreTest {

    @Test
    void round_trip_creates_directories_and_persists_json(@TempDir Path tmp) throws IOException {
        ScheduleStore store = new ScheduleStore(tmp);
        Schedule s = base("mac-mail", "0 0 * * * *");
        store.put(s);

        assertThat(Files.exists(tmp.resolve("schedules.json"))).isTrue();
        // Re-open — data survives
        ScheduleStore reopened = new ScheduleStore(tmp);
        assertThat(reopened.get("mac-mail")).isPresent();
        assertThat(reopened.get("mac-mail").get().cron).isEqualTo("0 0 * * * *");
    }

    @Test
    void put_sets_createdAt_on_first_write_and_updatedAt_every_write(@TempDir Path tmp) throws Exception {
        ScheduleStore store = new ScheduleStore(tmp);
        Schedule s = base("x", "* * * * * *");
        Schedule first = store.put(s);
        assertThat(first.createdAt).isNotNull();
        Instant createdFirst = first.createdAt;
        Thread.sleep(5);
        Schedule second = store.put(s);
        assertThat(second.createdAt).isEqualTo(createdFirst); // preserved
        assertThat(second.updatedAt).isAfter(createdFirst);
    }

    @Test
    void update_mutates_in_place_and_persists(@TempDir Path tmp) throws IOException {
        ScheduleStore store = new ScheduleStore(tmp);
        store.put(base("s", null));
        Schedule after = store.update("s", x -> { x.enabled = false; x.checkpoint = "cp-1"; });
        assertThat(after.enabled).isFalse();
        assertThat(after.checkpoint).isEqualTo("cp-1");

        Schedule reloaded = new ScheduleStore(tmp).get("s").orElseThrow();
        assertThat(reloaded.enabled).isFalse();
        assertThat(reloaded.checkpoint).isEqualTo("cp-1");
    }

    @Test
    void update_throws_for_unknown_name(@TempDir Path tmp) throws IOException {
        ScheduleStore store = new ScheduleStore(tmp);
        try {
            store.update("nope", x -> {});
            org.assertj.core.api.Assertions.fail("expected exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("no schedule");
        }
    }

    @Test
    void remove_returns_true_only_when_something_was_removed(@TempDir Path tmp) throws IOException {
        ScheduleStore store = new ScheduleStore(tmp);
        store.put(base("keep", null));
        store.put(base("gone", null));
        assertThat(store.remove("gone")).isTrue();
        assertThat(store.remove("gone")).isFalse();
        assertThat(store.all()).extracting(x -> x.name).containsExactly("keep");
    }

    @Test
    void put_rejects_blank_name(@TempDir Path tmp) throws IOException {
        ScheduleStore store = new ScheduleStore(tmp);
        try {
            Schedule bad = new Schedule();
            bad.jobYaml = "job: x\nnodes: []";
            store.put(bad);
            org.assertj.core.api.Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
    }

    private Schedule base(String name, String cron) {
        Schedule s = new Schedule();
        s.name = name;
        s.jobYaml = "job: dummy\nnodes: []";
        s.cron = cron;
        s.enabled = true;
        s.maxConcurrent = 1;
        return s;
    }
}
