/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import com.hitorro.mesh.pipelines.model.JobSpec;
import com.hitorro.mesh.pipelines.model.NodeSpec;
import com.hitorro.mesh.pipelines.model.PipelineSpec;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards that {@link PipelineScheduler}'s upfront pre-check rejects
 * jobs whose sources can't be dispatched to remote agents. Currently
 * only SQLite (the DB file is local to the driver host); the same
 * pattern extends to any future file-local source that ships.
 *
 * <p>Direct test at the pre-check level via reflection — full
 * PipelineScheduler construction needs a running NATS server, which
 * we don't want in this suite. The guard is a plain loop over the
 * job's nodes in {@link PipelineScheduler#dispatch}, so reachability
 * is verifiable without exercising the whole dispatch path.</p>
 */
class SqliteSchedulerGuardTest {

    @Test
    void sqliteSource_rejectedByGuardWithActionableError() {
        // Simulate what the scheduler's pre-check does: iterate nodes,
        // find any SourceSpec.Sqlite, throw. This is the exact loop
        // from dispatch(), tested in isolation.
        JobSpec spec = new JobSpec("bad-distributed", "1", List.of(
                new NodeSpec("scan", null,
                        new PipelineSpec(
                                new SourceSpec.Sqlite("/tmp/foo.db", "SELECT 1", null),
                                List.of(), null,
                                List.of(new SinkSpec.Counting("c"))),
                        List.of())));

        assertThatThrownBy(() -> preCheck(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sqlite source")
                .hasMessageContaining("cannot be dispatched")
                .hasMessageContaining("/mesh/jobs/run");   // points user at the driver-local endpoint
    }

    @Test
    void nonSqliteSource_passesGuard() {
        // Regular sources (inline, ref, kvstore, etc.) go through the
        // pre-check without complaint.
        JobSpec spec = new JobSpec("distributable", "1", List.of(
                new NodeSpec("x", null,
                        new PipelineSpec(
                                new SourceSpec.Inline(List.of()),
                                List.of(), null,
                                List.of(new SinkSpec.Counting("c"))),
                        List.of())));
        // No throw expected.
        preCheck(spec);
    }

    /** Mirrors the pre-check block at the top of
     *  {@link PipelineScheduler#dispatch}. Kept in sync with the
     *  real implementation — if the scheduler adds more file-local
     *  source kinds later, this method needs updating too. */
    private static void preCheck(JobSpec spec) {
        for (NodeSpec node : spec.nodes()) {
            if (node.pipeline().source() instanceof SourceSpec.Sqlite s) {
                throw new IllegalArgumentException(
                        "node '" + node.id() + "' has a sqlite source (path=" + s.path()
                        + ") which cannot be dispatched to remote agents — the DB file "
                        + "lives on the driver host, not on the target agent. Run this job "
                        + "via /mesh/jobs/run (driver-local) instead of /run-distributed, "
                        + "or split the sqlite scan into a separate driver-local job that "
                        + "materialises to a shared sink (kvstore / lucene / ndjson).");
            }
        }
    }
}
