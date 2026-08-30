/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reads {@code classpath:schedule-templates/manifest.json} and inlines
 * each entry's YAML body so REST clients get one self-contained
 * response per template.
 *
 * <p>Loading is one-shot: the registry snapshot is built at
 * construction and then read-only. Ship a new jar to change templates
 * — this is deliberate (templates are curated content, not runtime
 * config).</p>
 *
 * <p>Failure to load an individual template (missing YAML file, bad
 * manifest entry) is logged to stderr and skipped — the rest of the
 * registry still comes up, so one broken template doesn't take down
 * the whole "New from template..." picker.</p>
 */
public final class ScheduleTemplateRegistry {

    private static final String MANIFEST = "schedule-templates/manifest.json";
    private static final String DIR      = "schedule-templates/";

    private final Map<String, ScheduleTemplate> byId;

    public ScheduleTemplateRegistry() {
        this.byId = load();
    }

    public List<ScheduleTemplate> all() {
        return new ArrayList<>(byId.values());
    }

    public Optional<ScheduleTemplate> get(String id) {
        return Optional.ofNullable(byId.get(id));
    }

    private static Map<String, ScheduleTemplate> load() {
        Map<String, ScheduleTemplate> out = new LinkedHashMap<>();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) cl = ScheduleTemplateRegistry.class.getClassLoader();
        try (InputStream in = cl.getResourceAsStream(MANIFEST)) {
            if (in == null) {
                System.err.println("[ScheduleTemplateRegistry] no " + MANIFEST + " on classpath — templates disabled");
                return out;
            }
            ObjectMapper json = new ObjectMapper();
            ScheduleTemplate[] entries = json.readValue(in, ScheduleTemplate[].class);
            for (ScheduleTemplate t : entries) {
                if (t.id == null || t.id.isBlank()) {
                    System.err.println("[ScheduleTemplateRegistry] skipping entry with no id");
                    continue;
                }
                if (t.yaml == null || t.yaml.isBlank()) {
                    System.err.println("[ScheduleTemplateRegistry] template " + t.id + " has no `yaml` field");
                    continue;
                }
                String body = readTextResource(cl, DIR + t.yaml);
                if (body == null) {
                    System.err.println("[ScheduleTemplateRegistry] " + DIR + t.yaml + " not found — skipping " + t.id);
                    continue;
                }
                t.jobYaml = body;
                out.put(t.id, t);
            }
        } catch (IOException e) {
            System.err.println("[ScheduleTemplateRegistry] load failed: " + e.getMessage());
        }
        return out;
    }

    private static String readTextResource(ClassLoader cl, String path) {
        try (InputStream in = cl.getResourceAsStream(path)) {
            if (in == null) return null;
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }
}
