/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Render a schedule's job YAML with {@code ${VAR}} substitutions before
 * it hits {@link com.hitorro.mesh.pipelines.parse.JobSpecYaml#parse(String)}.
 *
 * <h3>Available variables</h3>
 * <ul>
 *   <li>{@code ${CHECKPOINT}} — raw checkpoint string; empty when unset.</li>
 *   <li>{@code ${CHECKPOINT_UNIX_SECONDS}} — if the checkpoint parses as an
 *       ISO instant, its epoch seconds. Empty otherwise.</li>
 *   <li>{@code ${CHECKPOINT_UNIX_MILLIS}} — same, in millis.</li>
 *   <li>{@code ${NOW}} — the scheduler's clock at trigger time (ISO instant).</li>
 *   <li>{@code ${NOW_UNIX_SECONDS}} / {@code ${NOW_UNIX_MILLIS}} — derived.</li>
 *   <li>{@code ${SCHEDULE_NAME}} — the name of the schedule that fired.</li>
 * </ul>
 *
 * <p>Unresolved variables are left as-is (with a warning at trigger
 * time), not replaced with empty strings — makes YAML parse errors
 * more diagnosable when a template references a variable the
 * substituter doesn't know.</p>
 *
 * <p>Sibling to {@code com.hitorro.util.core.params.PropertiesUtil}'s
 * {@code resolveJsonVariable} — that one lower-cases variable names
 * and drops unknowns silently. This one keeps case-sensitive uppercase
 * names and preserves unknowns as literal {@code ${VAR}} for
 * diagnostics. Kept scheduler-local for that reason.</p>
 */
public final class TemplateSubstitution {

    private static final Pattern VAR = Pattern.compile("\\$\\{([A-Z][A-Z0-9_]*)\\}");

    private TemplateSubstitution() {}

    public static String render(String template, String scheduleName,
                                String checkpoint, Instant now) {
        Map<String, String> vars = new java.util.LinkedHashMap<>();
        vars.put("SCHEDULE_NAME", scheduleName == null ? "" : scheduleName);
        vars.put("CHECKPOINT", checkpoint == null ? "" : checkpoint);

        // Derived checkpoint tokens — best-effort ISO parse.
        long cpSec = 0, cpMs = 0;
        boolean cpTimeOk = false;
        if (checkpoint != null && !checkpoint.isBlank()) {
            try {
                Instant i = Instant.parse(checkpoint.trim());
                cpSec = i.getEpochSecond();
                cpMs  = i.toEpochMilli();
                cpTimeOk = true;
            } catch (DateTimeParseException ignore) { /* opaque non-instant */ }
        }
        vars.put("CHECKPOINT_UNIX_SECONDS", cpTimeOk ? Long.toString(cpSec) : "");
        vars.put("CHECKPOINT_UNIX_MILLIS",  cpTimeOk ? Long.toString(cpMs)  : "");

        vars.put("NOW", now.toString());
        vars.put("NOW_UNIX_SECONDS", Long.toString(now.getEpochSecond()));
        vars.put("NOW_UNIX_MILLIS",  Long.toString(now.toEpochMilli()));

        Matcher m = VAR.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            String val = vars.get(key);
            // Unknown variable: leave the literal token so YAML parse errors
            // point at exactly the offending name. Escape $ for appendReplacement.
            String replacement = (val != null)
                    ? Matcher.quoteReplacement(val)
                    : Matcher.quoteReplacement("${" + key + "}");
            m.appendReplacement(out, replacement);
        }
        m.appendTail(out);
        return out.toString();
    }
}
