/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.hitorro.util.scheduler.durable.DurableSchedule;

/**
 * A mesh pipeline schedule. Adds a single mesh-specific field
 * ({@link #jobYaml}) to the generic {@link DurableSchedule}. Every
 * other lifecycle field (cron, checkpoint, catchup, bookkeeping)
 * lives on the base class and is owned by
 * {@link com.hitorro.util.scheduler.durable.DurableScheduler}.
 *
 * <p>Jackson serialises inherited public fields transparently — no
 * annotations needed to preserve the on-disk shape after the base
 * class extraction. Existing persisted {@code schedules.json} files
 * still deserialise cleanly.</p>
 *
 * <p>Checkpoint semantics: opaque string owned by the job's source
 * kind. Substituted into the job template as {@code ${CHECKPOINT}}
 * (raw). If the string parses as an ISO instant, derived tokens
 * {@code ${CHECKPOINT_UNIX_SECONDS}} + {@code ${CHECKPOINT_UNIX_MILLIS}}
 * are also available — see
 * {@link TemplateSubstitution}. On successful run the scheduler
 * advances the checkpoint to the run's start time UNLESS a
 * {@code groovy-map} step called {@code ctx.setCheckpoint(...)}
 * during the run (that value wins).</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Schedule extends DurableSchedule {

    /** Pipeline job in YAML. Inline text OR a {@code classpath:} / {@code file:}
     *  reference resolved at trigger time. */
    public String jobYaml;
}
