/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

/**
 * Per-run handle exposed to step scripts (bound as {@code ctx} in
 * {@code groovy-map} / {@code jvs-groovy}) so a step can push values
 * back into the containing {@link JobStatus} — most importantly a
 * checkpoint the scheduler should persist as the "last good key"
 * for the next run.
 *
 * <p>Used to close the gap where the scheduler otherwise blindly
 * stamps {@code ${NOW}} as the new checkpoint after every successful
 * run — fine for time-cursor sources, useless for max-row-id cursors
 * where the job itself is the only entity that knows the value it
 * consumed up to.</p>
 *
 * <p>Not thread-safe against concurrent step invocations on the same
 * status — call {@link #setCheckpoint} at most once per row, or from
 * a serial post-processing step at the tail of the pipeline (usually
 * cleaner). Last-writer-wins if called more than once.</p>
 */
public final class PipelineContext {

    private final JobStatus status;
    private final String nodeId;

    public PipelineContext(JobStatus status, String nodeId) {
        this.status = status;
        this.nodeId = nodeId;
    }

    /**
     * Record a checkpoint value the scheduler should persist as
     * {@code ${CHECKPOINT}} for the next run of this schedule.
     *
     * <p>Only meaningful when the job was launched by the scheduler —
     * an ad-hoc {@code /mesh/jobs/run} POST ignores the override.
     * When multiple steps or rows set the checkpoint, the last write
     * wins (steps should typically set it once at the end of the
     * batch, e.g. from a reduce with {@code collect} + {@code max}).</p>
     */
    public void setCheckpoint(String value) {
        status.scheduledCheckpointOverride = value;
    }

    /** Current checkpoint the runner was invoked with — read-only. */
    public String currentCheckpoint() {
        return status.scheduledCheckpointOverride;
    }

    public String jobId()  { return status.jobId; }
    public String nodeId() { return nodeId; }
}
