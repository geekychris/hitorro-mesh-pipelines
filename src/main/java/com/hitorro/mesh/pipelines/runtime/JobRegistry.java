/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Bounded in-memory registry of past + running jobs. Ring-buffer semantics:
 * the oldest completed run is evicted once the cap is hit. Running jobs
 * are never evicted.
 *
 * <p>When a {@link JobHistoryStore} is attached, every terminal transition
 * (SUCCEEDED / FAILED / CANCELLED) is appended to disk so the run history
 * survives driver restarts. On construction, the tail of that store is
 * replayed into memory as compact placeholder JobStatus entries so the
 * UI immediately shows the last few runs after a boot.</p>
 */
public final class JobRegistry {

    private final int capacity;
    private final JobHistoryStore history;
    private final RestartableJobStore restartable;
    private final ConcurrentMap<String, JobStatus> byId = new ConcurrentHashMap<>();
    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    public JobRegistry(int capacity) {
        this(capacity, null, null);
    }

    public JobRegistry(int capacity, JobHistoryStore history) {
        this(capacity, history, null);
    }

    /**
     * Full constructor — attaches BOTH a job history log (recent runs
     * for the UI) AND a restartable-jobs store (persistent registration
     * of long-running streaming jobs so they resume after driver restart).
     */
    public JobRegistry(int capacity, JobHistoryStore history, RestartableJobStore restartable) {
        this.capacity = capacity;
        this.history = history;
        this.restartable = restartable;
        if (history != null) replay();
    }

    /** Load the tail of the persistent history into in-memory placeholders. */
    private void replay() {
        for (JobHistoryStore.HistoryEntry e : history.tail(capacity)) {
            JobStatus s = new JobStatus(e.jobId(), e.jobSpecName());
            try {
                s.state = JobStatus.State.valueOf(e.state());
            } catch (Exception ignore) { s.state = JobStatus.State.SUCCEEDED; }
            if (e.finishedAt() != null) {
                try { s.finishedAt = java.time.Instant.parse(e.finishedAt()); }
                catch (Exception ignore) {}
            }
            s.error = e.error();
            // Node-level detail is preserved in the snapshot() view on demand;
            // we don't reconstruct the full NodeStatus map here because the
            // volatile counters would be misleading (job is already terminal).
            byId.put(s.jobId, s);
            synchronized (order) { order.add(s.jobId); }
        }
    }

    public JobStatus register(JobStatus s) {
        byId.put(s.jobId, s);
        synchronized (order) {
            order.add(s.jobId);
            while (order.size() > capacity) {
                String candidate = order.get(0);
                JobStatus cand = byId.get(candidate);
                if (cand != null && cand.state == JobStatus.State.RUNNING) break;
                order.remove(0);
                if (cand != null) byId.remove(candidate);
            }
        }
        return s;
    }

    /** Called by the runner when a job hits a terminal state — persists to
     *  disk. Also removes the job from the restartable store (if
     *  registered): a terminal run should NOT be resumed on next boot. */
    public void onTerminal(JobStatus s) {
        if (history != null && s.state != JobStatus.State.RUNNING
                            && s.state != JobStatus.State.PENDING) {
            history.append(s);
        }
        if (restartable != null && s.state != JobStatus.State.RUNNING
                                && s.state != JobStatus.State.PENDING) {
            restartable.remove(s.jobId);
        }
    }

    /**
     * Register a job for restart before it starts running. Called by
     * the REST controller (or any submit path) when the spec has
     * {@code restartable=true}. No-op if no store is attached.
     */
    public void registerRestartable(String jobId, com.hitorro.mesh.pipelines.model.JobSpec spec,
                                    String specText, String specKind) {
        if (restartable != null) restartable.record(jobId, spec, specText, specKind);
    }

    /** Store handle for the boot-time resume flow. Nullable. */
    public RestartableJobStore restartableStore() { return restartable; }

    public JobStatus get(String jobId) { return byId.get(jobId); }

    public List<JobStatus> list() {
        synchronized (order) {
            List<JobStatus> out = new ArrayList<>(order.size());
            for (String id : order) {
                JobStatus s = byId.get(id);
                if (s != null) out.add(s);
            }
            Collections.reverse(out);   // most recent first
            return out;
        }
    }

    /** History source if attached (nullable). */
    public JobHistoryStore history() { return history; }
}
