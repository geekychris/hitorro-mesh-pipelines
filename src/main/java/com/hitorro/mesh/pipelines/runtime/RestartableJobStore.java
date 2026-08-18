/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persistent registry of {@code restartable=true} jobs pending resume.
 * Backed by a JSON snapshot file; every write rewrites the whole file
 * atomically (via temp-then-rename). Cheap because restartable jobs
 * are always a small set (streaming jobs, not batch jobs).
 *
 * <p>Lifecycle:</p>
 * <ul>
 *   <li>{@code JobRunner.run(spec, status)} calls {@link #record} when
 *       the spec is {@link JobSpec#restartable()}.</li>
 *   <li>{@code JobRegistry.onTerminal} calls {@link #remove} on any
 *       terminal transition (SUCCEEDED / FAILED / CANCELLED).</li>
 *   <li>{@code PipelineAgentAutoConfiguration} (or an equivalent
 *       Spring bean) calls {@link #pending} on driver boot to
 *       re-submit every entry that survived the restart.</li>
 * </ul>
 *
 * <p>Not full checkpoint/resume — the job restarts from the beginning
 * of its source. For non-idempotent sinks this may cause duplicates;
 * sinks that override {@link com.hitorro.util.core.iterator.sinks.Sink#addIdempotent}
 * (e.g. {@code KvStoreSink}) stay safe. Full mid-stream checkpointing
 * is a follow-up Phase-4 concern.</p>
 */
public final class RestartableJobStore {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Path file;
    private final Map<String, Entry> byJobId = new ConcurrentHashMap<>();

    public RestartableJobStore(Path file) {
        this.file = file;
        if (file != null && Files.isRegularFile(file)) reload();
    }

    /** Default location under the pipelines home. */
    public static RestartableJobStore defaultOnDisk() {
        String home = System.getProperty("hitorro.pipelines.home",
                System.getProperty("user.home") + "/.hitorro/pipelines");
        return new RestartableJobStore(Path.of(home).resolve("restartable-jobs.json"));
    }

    /** Persist the spec text (YAML or JSON) so we can re-parse on boot. */
    public synchronized void record(String jobId, JobSpec spec, String specText, String specKind) {
        if (spec == null || !spec.restartable()) return;
        byJobId.put(jobId, new Entry(jobId, spec.id(),
                java.time.Instant.now().toString(), specText, specKind));
        persist();
    }

    /** Remove on terminal — no more resume attempts for this jobId. */
    public synchronized void remove(String jobId) {
        if (byJobId.remove(jobId) != null) persist();
    }

    /** Everything the store currently thinks needs a resume. */
    public List<Entry> pending() {
        return List.copyOf(byJobId.values());
    }

    public Path file() { return file; }
    public int size()  { return byJobId.size(); }

    // ---------------------------------------------------------- I/O

    private void reload() {
        try {
            var raw = JSON.readValue(file.toFile(),
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Entry>>() { });
            byJobId.putAll(raw);
        } catch (IOException ignored) {
            // Corrupt file — start fresh; next persist() overwrites it.
        }
    }

    private void persist() {
        if (file == null) return;
        try {
            Files.createDirectories(file.getParent());
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(tmp,
                    JSON.writerWithDefaultPrettyPrinter().writeValueAsString(byJobId),
                    StandardCharsets.UTF_8);
            Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception ignore) {
            // Best-effort — a jammed disk shouldn't kill a running job.
        }
    }

    // ---------------------------------------------------------- Public record

    /**
     * Serialisable pending entry. {@code specText} + {@code specKind}
     * survive because JobSpec's record structure isn't itself round-
     * trippable in all edge cases (Jackson polymorphic subtypes across
     * source/step/sink hierarchies); re-parsing from text is the
     * simpler contract.
     *
     * @param jobId     original jobId — reused on resume so any
     *                  external log / metric correlation stays intact
     * @param specName  human-readable name from {@code spec.id()}
     * @param recordedAt ISO-8601 timestamp when the job was first started
     * @param specText  original body (YAML / JSON / Groovy)
     * @param specKind  one of {@code "yaml"}, {@code "json"}, {@code "groovy"} —
     *                  tells the resumer which parser to invoke
     */
    public record Entry(String jobId, String specName, String recordedAt,
                        String specText, String specKind) { }

    // ---------------------------------------------------------- Resume helper

    /**
     * Convenience for the boot flow: read every pending entry, re-parse
     * the spec, and submit through the caller-provided runner. Returns
     * the list of jobIds resubmitted (so the caller can log a boot
     * summary).
     */
    public List<String> resumeAll(java.util.function.BiConsumer<String, JobSpec> submit) {
        List<String> resumed = new ArrayList<>();
        for (Entry e : pending()) {
            try {
                JobSpec spec = parse(e.specText, e.specKind);
                submit.accept(e.jobId, spec);
                resumed.add(e.jobId);
            } catch (Exception ex) {
                // Skip this one — its spec is probably corrupt or the
                // adapter it depends on is no longer on classpath.
                System.err.println("[RestartableJobStore] skip resume of "
                        + e.jobId + ": " + ex.getMessage());
            }
        }
        return resumed;
    }

    private static JobSpec parse(String text, String kind) throws IOException {
        switch (kind == null ? "yaml" : kind.toLowerCase()) {
            case "groovy":
                return com.hitorro.mesh.pipelines.parse.JobSpecGroovy.parse(text);
            case "yaml":
            case "json":
            default:
                return JobSpecYaml.parse(text);
        }
    }

    // Suppress unused (kept for future iteration helper).
    @SuppressWarnings("unused")
    private Iterator<Entry> iterator() { return new LinkedHashMap<>(byJobId).values().iterator(); }
}
