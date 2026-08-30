/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A named, pre-baked schedule the operator can pick as a starting
 * point in the UI's "New from template..." picker. Loaded from
 * {@code classpath:schedule-templates/manifest.json} at driver start
 * via {@link ScheduleTemplateRegistry}.
 *
 * <p>The registry inlines the YAML on read so REST clients get one
 * self-contained payload per template — no follow-up fetch to hydrate
 * the body.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScheduleTemplate {

    /** Stable id — used as the picker's key. Kebab-case. */
    public String id;

    /** Human title shown in the picker. */
    public String title;

    /** One-paragraph explanation of what the template is for. */
    public String description;

    /** Same values as {@code JobRunner#lifecycleOf} — helps the UI show
     *  the right badge before the user submits. */
    public String lifecycle;

    /** Filename of the YAML file (co-located with manifest.json). Kept
     *  for round-trippability; the registry inlines the actual text
     *  into {@link #jobYaml} on load so clients don't need to fetch it. */
    public String yaml;

    /** Inlined body — set by the registry after reading the YAML file. */
    public String jobYaml;

    /** Suggested field defaults for the create dialog. Any subset of the
     *  {@link Schedule} field names — the UI applies whatever keys are
     *  present without validation. */
    public Map<String, Object> defaults = new LinkedHashMap<>();
}
