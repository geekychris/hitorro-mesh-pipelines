/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * A durable pipeline schedule. One row = one recurring job.
 *
 * <p>The scheduler advances {@link #lastSuccessAt} + {@link #checkpoint}
 * after every successful run and re-invokes the job on the caller's
 * cadence. If the process is down when a fire would have happened, the
 * scheduler's boot-time catch-up detects the gap and dispatches a single
 * make-up run — the job uses {@code ${CHECKPOINT}} in its YAML to know
 * where to resume from, so a week's absence still results in ONE run
 * that processes since-last-checkpoint, not 168 runs.</p>
 *
 * <h3>Scheduling cadence</h3>
 * Either {@code cron} or {@code intervalSeconds} must be set. If both,
 * cron wins (interval is ignored). If neither, the schedule is
 * "manual" — only fires via {@code POST /mesh/schedules/{name}/run-now}.
 *
 * <h3>Checkpoint</h3>
 * Opaque string owned by the job's semantic. Substituted into the job
 * template as {@code ${CHECKPOINT}} (raw). If the string parses as an
 * ISO instant, the derived {@code ${CHECKPOINT_UNIX_SECONDS}} +
 * {@code ${CHECKPOINT_UNIX_MILLIS}} tokens are also available. On
 * successful run, the scheduler advances the checkpoint to the run's
 * start time (ISO string) UNLESS the job or an operator has explicitly
 * PUT a different value via {@code PUT /mesh/schedules/{name}/checkpoint}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Schedule {

    /** Stable, human-readable id. Used in URLs and CLI. */
    public String name;
    public String description;

    /** Pipeline job in YAML. Inline text OR a {@code classpath:} / {@code file:}
     *  reference resolved at trigger time. */
    public String jobYaml;

    /** Spring cron expression (6-field with seconds). Wins over intervalSeconds. */
    public String cron;

    /** Fixed interval trigger. Ignored when {@link #cron} is set. */
    public Long   intervalSeconds;

    /** Grace period past the interval before a boot catch-up fires.
     *  Prevents startup thrash when the process was down for < grace. */
    public long   catchupGraceSeconds = 60;

    /** Max in-flight runs. 1 = strict serial (default). */
    public int    maxConcurrent = 1;

    public boolean enabled = true;

    /** Opaque per-schedule offset. Substituted into the job template. */
    public String checkpoint;

    /** Bookkeeping — updated by the scheduler after each run. Never set by users. */
    public Instant lastRunAt;
    public Instant lastSuccessAt;
    public Instant lastFailureAt;
    public String  lastError;
    public long    totalRuns = 0;
    public long    successfulRuns = 0;

    public Instant createdAt;
    public Instant updatedAt;
}
