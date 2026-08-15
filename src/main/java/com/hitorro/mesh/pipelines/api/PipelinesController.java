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
        return acceptAndRun(spec);
    }

    @PostMapping("/run/bundled/{name}")
    public ResponseEntity<Map<String, String>> runBundled(@PathVariable String name) throws Exception {
        JobSpec spec = JobSpecYaml.parse(BundledJobs.load(name));
        return acceptAndRun(spec);
    }

    @GetMapping
    public List<JobStatus.Snapshot> list() {
        return registry.list().stream().map(JobStatus::snapshot).toList();
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

    private ResponseEntity<Map<String, String>> acceptAndRun(JobSpec spec) {
        // Live status object — the runner mutates this directly so pollers
        // see progress in real time. Critical for streaming jobs whose
        // run() never returns.
        String jobId = "job-" + System.nanoTime();
        JobStatus live = new JobStatus(jobId, spec.id());
        registry.register(live);
        pool.submit(() -> runner.run(spec, live));
        return ResponseEntity.accepted().body(Map.of("jobId", jobId));
    }
}
