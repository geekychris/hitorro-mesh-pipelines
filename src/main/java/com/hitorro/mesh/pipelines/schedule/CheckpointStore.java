/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One text file per schedule under {@code <home>/checkpoints/<safeName>.txt}.
 * Kept separate from {@link ScheduleStore} so per-run checkpoint advances
 * don't rewrite the entire schedules file every hour.
 *
 * <p>Value is a UTF-8 string — the scheduler treats it opaquely and
 * substitutes it into job templates as {@code ${CHECKPOINT}}. Users
 * choose the semantic (ISO instant, unix seconds, monotonic row id,
 * whatever their job's source understands).</p>
 */
public class CheckpointStore {

    private final Path root;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public CheckpointStore(Path home) throws IOException {
        this.root = home.resolve("checkpoints");
        Files.createDirectories(root);
    }

    public Optional<String> get(String scheduleName) {
        Path p = fileFor(scheduleName);
        if (!Files.exists(p)) return Optional.empty();
        try { return Optional.of(Files.readString(p, StandardCharsets.UTF_8).stripTrailing()); }
        catch (IOException e) { throw new UncheckedIO(e); }
    }

    public void put(String scheduleName, String value) throws IOException {
        Path p = fileFor(scheduleName);
        Path tmp = p.resolveSibling(p.getFileName() + ".tmp");
        ReentrantLock l = locks.computeIfAbsent(scheduleName, k -> new ReentrantLock());
        l.lock();
        try {
            Files.writeString(tmp, value == null ? "" : value, StandardCharsets.UTF_8);
            Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } finally { l.unlock(); }
    }

    public void remove(String scheduleName) throws IOException {
        Files.deleteIfExists(fileFor(scheduleName));
    }

    private Path fileFor(String scheduleName) {
        String safe = scheduleName.replaceAll("[^A-Za-z0-9._-]", "_");
        return root.resolve(safe + ".txt");
    }

    public static final class UncheckedIO extends RuntimeException {
        public UncheckedIO(IOException e) { super(e); }
    }
}
