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
 */
public final class JobRegistry {

    private final int capacity;
    private final ConcurrentMap<String, JobStatus> byId = new ConcurrentHashMap<>();
    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    public JobRegistry(int capacity) {
        this.capacity = capacity;
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
}
