/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.schedule;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class TemplateSubstitutionTest {

    private final Instant NOW = Instant.parse("2026-08-29T12:00:00Z");

    @Test
    void substitutes_known_variables() {
        String tpl = "since=${CHECKPOINT} sched=${SCHEDULE_NAME} now=${NOW}";
        String r = TemplateSubstitution.render(tpl, "mac-mail", "2026-08-28T12:00:00Z", NOW);
        assertThat(r).isEqualTo("since=2026-08-28T12:00:00Z sched=mac-mail now=2026-08-29T12:00:00Z");
    }

    @Test
    void iso_checkpoint_yields_derived_unix_variants() {
        String tpl = "sec=${CHECKPOINT_UNIX_SECONDS} ms=${CHECKPOINT_UNIX_MILLIS}";
        String r = TemplateSubstitution.render(tpl, "s", "2026-08-28T12:00:00Z", NOW);
        // 2026-08-28T12:00:00Z = 1787918400
        assertThat(r).isEqualTo("sec=1787918400 ms=1787918400000");
    }

    @Test
    void opaque_non_instant_checkpoint_leaves_derived_tokens_blank() {
        String tpl = "cp=${CHECKPOINT} sec=${CHECKPOINT_UNIX_SECONDS}";
        String r = TemplateSubstitution.render(tpl, "s", "rowid-42", NOW);
        assertThat(r).isEqualTo("cp=rowid-42 sec=");
    }

    @Test
    void unknown_variable_is_preserved_as_literal_for_diagnostics() {
        String tpl = "known=${NOW} weird=${NOT_A_VAR}";
        String r = TemplateSubstitution.render(tpl, "s", "", NOW);
        assertThat(r).isEqualTo("known=2026-08-29T12:00:00Z weird=${NOT_A_VAR}");
    }

    @Test
    void null_or_blank_checkpoint_renders_as_empty() {
        String r1 = TemplateSubstitution.render("cp=[${CHECKPOINT}]", "s", null, NOW);
        String r2 = TemplateSubstitution.render("cp=[${CHECKPOINT}]", "s", "  ", NOW);
        assertThat(r1).isEqualTo("cp=[]");
        assertThat(r2).isEqualTo("cp=[  ]");   // whitespace preserved verbatim
    }

    @Test
    void now_variants_are_consistent() {
        String tpl = "${NOW_UNIX_SECONDS}|${NOW_UNIX_MILLIS}";
        String r = TemplateSubstitution.render(tpl, "s", "", NOW);
        assertThat(r).isEqualTo("1788004800|1788004800000");
    }

    @Test
    void empty_template_returns_empty() {
        assertThat(TemplateSubstitution.render("", "s", null, NOW)).isEmpty();
    }
}
