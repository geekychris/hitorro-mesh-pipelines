/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.util.core.iterator.sinks.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the decorator plug-in point on {@link SinkRegistry}. The
 * decorator hook is the mechanism the driver-app uses to auto-register
 * NDJSON sinks with the mesh runtime-table registry on close — this
 * suite verifies the wiring in isolation from any driver-app deps.
 */
class SinkRegistryDecoratorTest {

    @Test
    void decorator_wrapsBuiltSink_seesEverySinkCreated(@TempDir Path tmp) {
        SinkRegistry reg = new SinkRegistry(tmp);
        AtomicInteger wrapCount = new AtomicInteger();
        reg.registerDecorator((spec, base) -> {
            wrapCount.incrementAndGet();
            return base;
        });

        var sink = reg.create(new SinkSpec.NdjsonFile(tmp.resolve("a.ndjson").toUri().toString()));
        assertThat(sink).isNotNull();
        assertThat(wrapCount.get()).isEqualTo(1);
    }

    @Test
    void decorator_canReplaceSink(@TempDir Path tmp) throws Exception {
        SinkRegistry reg = new SinkRegistry(tmp);
        // Replacement sink that just counts add()s — no file I/O.
        AtomicInteger seen = new AtomicInteger();
        Sink<JsonNode> replacement = new Sink<>() {
            @Override public boolean init(JsonNode n) { return true; }
            @Override public boolean start() { return true; }
            @Override public boolean add(JsonNode o) { seen.incrementAndGet(); return true; }
            @Override public boolean stop()  { return true; }
            @Override public void close() { }
        };
        reg.registerDecorator((spec, base) -> replacement);

        Sink<JsonNode> sink = reg.create(new SinkSpec.NdjsonFile("file:/never-touched.ndjson"));
        sink.add(new ObjectMapper().readTree("{\"x\":1}"));
        sink.add(new ObjectMapper().readTree("{\"x\":2}"));
        sink.close();
        assertThat(seen.get()).isEqualTo(2);
        // Original file should NOT exist — the decorator's replacement skipped it.
        assertThat(Files.exists(tmp.resolve("never-touched.ndjson"))).isFalse();
    }

    @Test
    void multipleDecorators_chainInRegistrationOrder(@TempDir Path tmp) {
        SinkRegistry reg = new SinkRegistry(tmp);
        List<String> order = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        reg.registerDecorator((spec, base) -> { order.add("outer"); return base; });
        reg.registerDecorator((spec, base) -> { order.add("inner"); return base; });

        reg.create(new SinkSpec.NdjsonFile(tmp.resolve("chain.ndjson").toUri().toString()));
        // Registration order = call order (each sees the previous's output).
        assertThat(order).containsExactly("outer", "inner");
    }

    @Test
    void registerAsTable_isNullByDefault_backCompat() {
        // Six-arg ndjson-file spec (no auto-register options) — the
        // back-compat single-arg ctor must keep registerAsTable null so
        // decorators see the "no auto-register requested" signal.
        SinkSpec.NdjsonFile spec = new SinkSpec.NdjsonFile("file:/x.ndjson");
        assertThat(spec.registerAsTable()).isNull();
    }

    @Test
    void registerAsTable_carriesOptions() {
        var opts = new SinkSpec.RegisterAsTable("my_table", null, true, null);
        SinkSpec.NdjsonFile spec = new SinkSpec.NdjsonFile("file:/x.ndjson", opts);
        assertThat(spec.registerAsTable()).isEqualTo(opts);
        assertThat(spec.registerAsTable().broadcastOrDefault()).isTrue();
    }

    @Test
    void broadcastOrDefault_treatsNullAsTrue() {
        var opts = new SinkSpec.RegisterAsTable("t", null, null, null);
        assertThat(opts.broadcastOrDefault()).isTrue();
    }

    @Test
    void broadcastOrDefault_respectsExplicitFalse() {
        var opts = new SinkSpec.RegisterAsTable("t", null, Boolean.FALSE, "all");
        assertThat(opts.broadcastOrDefault()).isFalse();
    }

    @Test
    void undecoratedRegistry_stillBuildsSinks(@TempDir Path tmp) throws IOException {
        SinkRegistry reg = new SinkRegistry(tmp);
        Path f = tmp.resolve("plain.ndjson");
        var sink = reg.create(new SinkSpec.NdjsonFile(f.toUri().toString()));
        sink.start();
        sink.close();
        // File exists = the base sink was invoked (no decorator interference).
        assertThat(Files.exists(f)).isTrue();
    }
}
