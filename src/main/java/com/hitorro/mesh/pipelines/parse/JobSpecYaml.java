/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.parse;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.DeserializationFeature;
import com.hitorro.util.json.JsonMappers;
import com.hitorro.mesh.pipelines.model.JobSpec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Parses {@link JobSpec} from YAML or JSON. Accepts both formats
 * transparently — YAML for hand-authored specs, JSON for REST posts.
 *
 * <p>Sealed source/step/sink hierarchies deserialise via Jackson's
 * {@code @JsonTypeInfo(property="kind")} — each YAML {@code kind:} literal
 * maps to a concrete record subtype.</p>
 */
public final class JobSpecYaml {

    // 3.x: new YAMLMapper()/new ObjectMapper() with FAIL_ON_UNKNOWN_PROPERTIES disabled.
    // Jackson2Yaml: Jackson 2's YAML 1.1 scalar typing, defaults and stream limits.
    private static final ObjectMapper YAML = Jackson2Yaml.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private static final ObjectMapper JSON = JsonMappers.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private JobSpecYaml() { }

    public static JobSpec parse(String text) throws IOException {
        String trimmed = text.trim();
        ObjectMapper m = (trimmed.startsWith("{") || trimmed.startsWith("[")) ? JSON : YAML;
        try {
            return m.readValue(text, JobSpec.class);
        } catch (JacksonException e) {
            // 3.x: a bad spec was a JsonProcessingException, an IOException.
            throw new IOException(e.getMessage(), e);
        }
    }

    public static JobSpec parse(InputStream in) throws IOException {
        // Read fully so we can peek at the first character. Job specs are
        // small (KBs), so buffering is fine.
        String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        return parse(text);
    }

    public static JobSpec parseFile(Path file) throws IOException {
        return parse(Files.readString(file, StandardCharsets.UTF_8));
    }

    /** Load a bundled example spec from the classpath. */
    public static JobSpec loadBundled(String resourcePath) throws IOException {
        try (InputStream in = JobSpecYaml.class.getResourceAsStream(resourcePath)) {
            if (in == null) throw new IOException("no bundled spec at " + resourcePath);
            return parse(in);
        }
    }

    /** Re-emit as JSON — used by the REST API to return the parsed spec. */
    public static String toJson(JobSpec spec) throws IOException {
        try {
            return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(spec);
        } catch (JacksonException e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
