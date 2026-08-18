/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.api;

import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.parse.JobSpecYaml;
import com.hitorro.mesh.pipelines.runtime.BundledJobs;
import com.hitorro.mesh.pipelines.runtime.JobRegistry;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.JobStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * REST surface for pipelines.
 *
 * <ul>
 *   <li>{@code POST /mesh/jobs/run} — YAML or JSON body, kicks off a run
 *       async. Returns {@code {jobId}}.</li>
 *   <li>{@code POST /mesh/jobs/run/bundled/{name}} — run a bundled example.</li>
 *   <li>{@code GET  /mesh/jobs} — all past + running runs (most recent first).</li>
 *   <li>{@code GET  /mesh/jobs/{jobId}} — snapshot for one run.</li>
 *   <li>{@code GET  /mesh/jobs/bundled} — list bundled example names + YAML.</li>
 * </ul>
 */
@RestController
@RequestMapping("/mesh/jobs")
public class PipelinesController {

    private final JobRunner runner;
    private final JobRegistry registry;
    private final ExecutorService pool = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "pipelines-http"); t.setDaemon(true); return t;
    });

    @Autowired
    public PipelinesController(JobRunner runner, JobRegistry registry) {
        this.runner = runner;
        this.registry = registry;
    }

    @PostMapping(value = "/run", consumes = {"application/x-yaml", "application/yaml",
            MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE, "*/*"})
    public ResponseEntity<Map<String, String>> runInline(@RequestBody String body) throws Exception {
        JobSpec spec = JobSpecYaml.parse(body);
        return acceptAndRun(spec, body, "yaml");
    }

    @PostMapping("/run/bundled/{name}")
    public ResponseEntity<Map<String, String>> runBundled(@PathVariable String name) throws Exception {
        JobSpec spec = JobSpecYaml.parse(BundledJobs.load(name));
        return acceptAndRun(spec);
    }

    /**
     * Accepts a Groovy DSL script (see {@link com.hitorro.mesh.pipelines.parse.JobSpecGroovy}).
     * Same output as {@code /run} — kicks the job async, returns
     * {@code {jobId}}. Separate endpoint from {@code /run} because
     * YAML/JSON and Groovy don't disambiguate cleanly from a bare
     * request body.
     *
     * <p>Content-type is intentionally permissive — Groovy scripts
     * don't have a settled media type in the wild. Accepts
     * {@code application/groovy}, {@code text/x-groovy}, or the
     * plain-text catch-all.</p>
     */
    @PostMapping(value = "/run-groovy",
            consumes = {"application/groovy", "text/x-groovy",
                    MediaType.TEXT_PLAIN_VALUE, "*/*"})
    public ResponseEntity<Map<String, String>> runGroovy(@RequestBody String body) {
        JobSpec spec = com.hitorro.mesh.pipelines.parse.JobSpecGroovy.parse(body);
        return acceptAndRun(spec, body, "groovy");
    }

    /**
     * Distributed dispatch — hands each node of the spec to a mesh agent
     * that advertises the {@code pipeline-node} capability. Round-robin
     * assignment for now. Requires at least one agent running the
     * {@code hitorro-mesh-agent-pipelines} module.
     *
     * <p>Falls back with a clean 4xx if no capability match exists — the
     * driver-local {@code /run} endpoint is always available.</p>
     */
    @PostMapping(value = "/run-distributed",
            consumes = {"application/x-yaml", "application/yaml",
                    MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE, "*/*"})
    public ResponseEntity<Map<String, String>> runDistributed(@RequestBody String body) throws Exception {
        JobSpec spec = JobSpecYaml.parse(body);
        String jobId = "job-" + System.nanoTime();
        JobStatus live = new JobStatus(jobId, spec.id());
        registry.register(live);
        pool.submit(() -> {
            try (com.hitorro.mesh.pipelines.runtime.PipelineScheduler s =
                    new com.hitorro.mesh.pipelines.runtime.PipelineScheduler(
                            System.getProperty("hitorro.pipelines.natsUrl", "nats://localhost:4222"),
                            System.getProperty("hitorro.pipelines.driverUrl", "http://localhost:8085"),
                            SINK_LOCATIONS)) {
                s.dispatch(spec, live, null);
            } catch (Exception e) {
                live.state = JobStatus.State.FAILED;
                live.error = e.getClass().getSimpleName() + ": " + e.getMessage();
            } finally {
                registry.onTerminal(live);
            }
        });
        return ResponseEntity.accepted().body(Map.of("jobId", jobId, "mode", "distributed"));
    }

    /** Shared persistent sink-location registry — reloaded on startup. */
    private static final com.hitorro.mesh.pipelines.runtime.SinkLocationRegistry SINK_LOCATIONS =
            com.hitorro.mesh.pipelines.runtime.SinkLocationRegistry.defaultOnDisk();

    /**
     * Which agent holds each named persistent sink. Empty until at
     * least one distributed job has written to a kvstore or lucene
     * sink. Reloaded from ~/.hitorro/pipelines/sink-locations.json on
     * driver startup.
     */
    @GetMapping("/sink-locations")
    public Map<String, String> sinkLocations() {
        return SINK_LOCATIONS.snapshot();
    }

    @GetMapping
    public List<JobStatus.Snapshot> list() {
        return registry.list().stream().map(JobStatus::snapshot).toList();
    }

    /**
     * Full persistent job history — one entry per completed run, most
     * recent first. Reads from {@code ~/.hitorro/pipelines/jobs.ndjson}
     * (or wherever {@link com.hitorro.mesh.pipelines.runtime.JobHistoryStore}
     * was pointed). Survives driver restarts.
     */
    @GetMapping("/history")
    public List<com.hitorro.mesh.pipelines.runtime.JobHistoryStore.HistoryEntry> history(
            @RequestParam(value = "limit", defaultValue = "200") int limit) {
        var store = registry.history();
        if (store == null) return List.of();
        var all = store.tail(limit);
        // reverse — most recent first
        java.util.Collections.reverse(all);
        return all;
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobStatus.Snapshot> one(@PathVariable String jobId) {
        JobStatus s = registry.get(jobId);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.snapshot());
    }

    @GetMapping("/{jobId}/events")
    public ResponseEntity<List<JobStatus.ProgressEvent>> events(@PathVariable String jobId) {
        JobStatus s = registry.get(jobId);
        if (s == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.events());
    }

    @GetMapping("/bundled")
    public Map<String, String> bundled() {
        return BundledJobs.loadAll();
    }

    /**
     * Cooperative cancel. Sets the job's cancelRequested flag; NodeRunner
     * checks it between each row and stops cleanly. Idempotent — cancelling
     * a completed job is a no-op that still returns 200.
     */
    @DeleteMapping("/{jobId}")
    public ResponseEntity<Map<String, String>> cancel(@PathVariable String jobId) {
        JobStatus s = registry.get(jobId);
        if (s == null) return ResponseEntity.notFound().build();
        s.cancelRequested.set(true);
        return ResponseEntity.ok(Map.of("jobId", jobId, "cancelRequested", "true"));
    }

    /** Original 1-arg form for bundled jobs where we don't have the raw
     *  body text at hand (loaded from classpath). Non-restartable path. */
    private ResponseEntity<Map<String, String>> acceptAndRun(JobSpec spec) {
        return acceptAndRun(spec, null, null);
    }

    /**
     * @param specText  raw request body — persisted verbatim for restartable
     *                  jobs so the boot resumer re-parses the same text
     * @param specKind  {@code "yaml"} / {@code "json"} / {@code "groovy"} —
     *                  tells the resumer which parser to invoke
     */
    private ResponseEntity<Map<String, String>> acceptAndRun(JobSpec spec, String specText, String specKind) {
        // Live status object — the runner mutates this directly so pollers
        // see progress in real time. Critical for streaming jobs whose
        // run() never returns.
        String jobId = "job-" + System.nanoTime();
        JobStatus live = new JobStatus(jobId, spec.id());
        registry.register(live);
        // Restartable jobs get persisted BEFORE the pool.submit so a
        // crash between register + submit is still recoverable.
        if (spec.restartable() && specText != null) {
            registry.registerRestartable(jobId, spec, specText, specKind);
        }
        pool.submit(() -> {
            try { runner.run(spec, live); }
            finally { registry.onTerminal(live); }
        });
        return ResponseEntity.accepted().body(Map.of("jobId", jobId));
    }
}
