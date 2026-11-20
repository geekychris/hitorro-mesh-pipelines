/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.spring;

import com.hitorro.mesh.pipelines.api.PipelinesController;
import com.hitorro.mesh.pipelines.runtime.JobHistoryStore;
import com.hitorro.mesh.pipelines.runtime.JobRegistry;
import com.hitorro.mesh.pipelines.runtime.JobRunner;
import com.hitorro.mesh.pipelines.runtime.RestartableJobStore;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@link PipelinesAutoConfiguration} loads under Spring Boot 4 the way a
 * host app discovers it: from {@code META-INF/spring/...AutoConfiguration.imports}
 * (Boot 4 no longer reads {@code spring.factories} for auto-configuration).
 * The pipelines home is pointed at a temp dir so nothing lands in ~/.hitorro.
 */
class PipelinesAutoConfigurationTest {

    @TempDir Path home;
    private String previousHome;

    @BeforeEach
    void pointHomeAtTempDir() {
        previousHome = System.getProperty("hitorro.pipelines.home");
        System.setProperty("hitorro.pipelines.home", home.toString());
    }

    @AfterEach
    void restoreHome() {
        if (previousHome == null) System.clearProperty("hitorro.pipelines.home");
        else System.setProperty("hitorro.pipelines.home", previousHome);
    }

    private static Class<?>[] pipelinesAutoConfigurationsFromImportsFile() throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();
        for (String name : ImportCandidates.load(AutoConfiguration.class,
                PipelinesAutoConfigurationTest.class.getClassLoader())) {
            if (name.startsWith("com.hitorro.mesh.pipelines.")) classes.add(Class.forName(name));
        }
        return classes.toArray(Class<?>[]::new);
    }

    @Test
    void runsOnSpringBoot4() {
        assertThat(SpringBootVersion.getVersion()).startsWith("4.");
    }

    @Test
    void importsFileListsTheAutoConfiguration() throws Exception {
        assertThat(pipelinesAutoConfigurationsFromImportsFile()).contains(PipelinesAutoConfiguration.class);
    }

    @Test
    void autoConfigurationCreatesThePipelineBeans() throws Exception {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(pipelinesAutoConfigurationsFromImportsFile()))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx).hasSingleBean(SinkRegistry.class);
                    assertThat(ctx).hasSingleBean(JobRunner.class);
                    assertThat(ctx).hasSingleBean(JobHistoryStore.class);
                    assertThat(ctx).hasSingleBean(RestartableJobStore.class);
                    assertThat(ctx).hasSingleBean(JobRegistry.class);
                    assertThat(ctx).hasSingleBean(PipelinesController.class);
                    assertThat(ctx).hasBean("pipelinesBootResumer");
                    assertThat(ctx.getBean(SinkRegistry.class).home()).isEqualTo(home);
                });
    }

    @Test
    void backsOffWithoutSpringWeb() throws Exception {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(pipelinesAutoConfigurationsFromImportsFile()))
                .withClassLoader(new FilteredClassLoader("org.springframework.web.bind.annotation"))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx).doesNotHaveBean(JobRunner.class);
                });
    }
}
