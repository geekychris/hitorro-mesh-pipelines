/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.spring;

import com.hitorro.mesh.pipelines.api.PipelinesController;
import com.hitorro.mesh.pipelines.runtime.JobHistoryStore;
import com.hitorro.mesh.pipelines.runtime.JobRegistry;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.JobStatus;
import com.hitorro.mesh.pipelines.runtime.RestartableJobStore;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;

import java.nio.file.Paths;
import java.util.List;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot autoconfig — kicks in only when the module is on the
 * classpath. Registers a {@link SinkRegistry} at the default pipelines
 * home, a {@link JobRunner}, an in-memory {@link JobRegistry}, and the
 * REST controller.
 */
@AutoConfiguration
@ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
public class PipelinesAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SinkRegistry pipelinesSinkRegistry() {
        return SinkRegistry.withDefaultHome();
    }

    @Bean
    @ConditionalOnMissingBean
    public JobRunner pipelinesJobRunner(SinkRegistry sinkRegistry) {
        return new JobRunner(sinkRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public JobHistoryStore pipelinesJobHistoryStore() {
        String home = System.getProperty("hitorro.pipelines.home",
                System.getenv().getOrDefault("HITORRO_PIPELINES_HOME",
                        System.getProperty("user.home") + "/.hitorro/pipelines"));
        return new JobHistoryStore(Paths.get(home, "jobs.ndjson"));
    }

    @Bean
    @ConditionalOnMissingBean
    public RestartableJobStore pipelinesRestartableStore() {
        return RestartableJobStore.defaultOnDisk();
    }

    @Bean
    @ConditionalOnMissingBean
    public JobRegistry pipelinesJobRegistry(JobHistoryStore history, RestartableJobStore restartable) {
        return new JobRegistry(64, history, restartable);
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelinesController pipelinesController(JobRunner runner, JobRegistry registry) {
        return new PipelinesController(runner, registry);
    }

    /**
     * On driver boot, resurrect every {@code restartable=true} job that
     * survived the restart. Runs after all beans are ready (JobRunner +
     * JobRegistry both injected). Each resumed job gets a fresh
     * {@link JobStatus} but keeps its original jobId — external log /
     * metric correlation is intact.
     *
     * <p>The resume path runs the whole job from the source's beginning
     * — no mid-stream checkpointing (Phase 4). Sinks that override
     * {@code addIdempotent} stay clean; others may see duplicates. The
     * safety contract is documented on
     * {@link com.hitorro.mesh.pipelines.model.JobSpec#restartable()}.</p>
     */
    @Bean
    @ConditionalOnMissingBean(name = "pipelinesBootResumer")
    public PipelinesBootResumer pipelinesBootResumer(JobRunner runner,
                                                     JobRegistry registry,
                                                     RestartableJobStore store) {
        return new PipelinesBootResumer(runner, registry, store);
    }

    /**
     * Nested bean so the @PostConstruct fires exactly once after Spring
     * has finished wiring the runner + registry + store. Keeping it as
     * a separate class (rather than a method on the outer) makes the
     * dependency chain explicit and easier to trace.
     */
    public static class PipelinesBootResumer implements InitializingBean {
        private final JobRunner runner;
        private final JobRegistry registry;
        private final RestartableJobStore store;

        @Autowired
        public PipelinesBootResumer(JobRunner runner, JobRegistry registry,
                                    RestartableJobStore store) {
            this.runner = runner;
            this.registry = registry;
            this.store = store;
        }

        /** Spring calls this once all deps are wired — safer than
         *  @PostConstruct across the jakarta / javax boundary. */
        @Override public void afterPropertiesSet() { resume(); }

        public void resume() {
            List<String> resumed = store.resumeAll((jobId, spec) -> {
                JobStatus live = new JobStatus(jobId, spec.id());
                registry.register(live);
                // Same async pattern as PipelinesController — the resume
                // runs on a fresh thread so the boot doesn't block on a
                // long-running streaming source.
                new Thread(() -> {
                    try { runner.run(spec, live); }
                    finally { registry.onTerminal(live); }
                }, "pipelines-resume-" + jobId).start();
            });
            if (!resumed.isEmpty()) {
                System.err.println("[PipelinesBootResumer] resumed "
                        + resumed.size() + " restartable job(s): " + resumed);
            }
        }
    }
}
