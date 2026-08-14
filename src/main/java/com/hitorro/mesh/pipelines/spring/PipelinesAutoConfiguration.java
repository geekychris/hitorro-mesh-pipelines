/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.spring;

import com.hitorro.mesh.pipelines.api.PipelinesController;
import com.hitorro.mesh.pipelines.runtime.JobRegistry;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
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
    public JobRegistry pipelinesJobRegistry() {
        return new JobRegistry(64);
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelinesController pipelinesController(JobRunner runner, JobRegistry registry) {
        return new PipelinesController(runner, registry);
    }
}
