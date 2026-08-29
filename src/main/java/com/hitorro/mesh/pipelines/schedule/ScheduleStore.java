/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Durable per-file JSON store for {@link Schedule}s. Whole-file rewrite
 * on every save — cheap given schedule counts are small (dozens at
 * most) and gives atomic swap semantics via move-with-replace.
 *
 * <p>Not thread-safe against external writers (multiple processes),
 * but internally coordinated via a read/write lock so the scheduler
 * thread and the REST controller can share one instance safely.</p>
 *
 * <p>File layout: {@code <home>/schedules.json} — array of Schedule
 * objects, ordered by insertion for stable diffs. Checkpoints live in
 * a sibling {@link CheckpointStore} so hot per-run advances don't
 * churn this file.</p>
 */
public class ScheduleStore {

    private static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final Path file;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, Schedule> byName = new LinkedHashMap<>();

    public ScheduleStore(Path home) throws IOException {
        Files.createDirectories(home);
        this.file = home.resolve("schedules.json");
        load();
    }

    public List<Schedule> all() {
        lock.readLock().lock();
        try { return new ArrayList<>(byName.values()); }
        finally { lock.readLock().unlock(); }
    }

    public Optional<Schedule> get(String name) {
        lock.readLock().lock();
        try { return Optional.ofNullable(byName.get(name)); }
        finally { lock.readLock().unlock(); }
    }

    /** Insert or replace by name. Sets createdAt/updatedAt as appropriate. */
    public Schedule put(Schedule s) throws IOException {
        if (s.name == null || s.name.isBlank())
            throw new IllegalArgumentException("schedule name is required");
        lock.writeLock().lock();
        try {
            Schedule existing = byName.get(s.name);
            java.time.Instant now = java.time.Instant.now();
            if (existing == null) {
                if (s.createdAt == null) s.createdAt = now;
            } else {
                if (s.createdAt == null) s.createdAt = existing.createdAt;
            }
            s.updatedAt = now;
            byName.put(s.name, s);
            persist();
            return s;
        } finally { lock.writeLock().unlock(); }
    }

    public boolean remove(String name) throws IOException {
        lock.writeLock().lock();
        try {
            if (byName.remove(name) == null) return false;
            persist();
            return true;
        } finally { lock.writeLock().unlock(); }
    }

    /** In-place mutation with atomic disk write. */
    public Schedule update(String name, java.util.function.Consumer<Schedule> mutator) throws IOException {
        lock.writeLock().lock();
        try {
            Schedule s = byName.get(name);
            if (s == null) throw new IllegalArgumentException("no schedule named " + name);
            mutator.accept(s);
            s.updatedAt = java.time.Instant.now();
            persist();
            return s;
        } finally { lock.writeLock().unlock(); }
    }

    // ---- IO ----------------------------------------------------------

    private void load() throws IOException {
        if (!Files.exists(file)) return;
        Schedule[] arr = JSON.readValue(Files.readAllBytes(file), Schedule[].class);
        for (Schedule s : arr) byName.put(s.name, s);
    }

    private void persist() throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(tmp, JSON.writeValueAsBytes(byName.values().toArray(new Schedule[0])));
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
