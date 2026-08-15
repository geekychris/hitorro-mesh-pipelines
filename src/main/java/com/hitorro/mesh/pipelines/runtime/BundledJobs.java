/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Loads example job specs bundled inside classpath-reachable jars. UI's
 * "Bundled examples" list scans this registry.
 */
public final class BundledJobs {

    /** Order matters — first in the list shows first in the UI. */
    public static final String[] EXAMPLES = {
            "countries-triple-sink",
            "airports-typed-enrich",
            "nats-echo",
            "nats-publisher",
            "airports-groovy",
            "kv-write",
            "kv-read",
    };

    private BundledJobs() { }

    /** Returns id → YAML text for every bundled example that resolves. */
    public static Map<String, String> loadAll() {
        Map<String, String> out = new LinkedHashMap<>();
        for (String id : EXAMPLES) {
            String path = "/examples/jobs/" + id + ".yaml";
            try (InputStream in = BundledJobs.class.getResourceAsStream(path)) {
                if (in == null) continue;
                out.put(id, new String(in.readAllBytes()));
            } catch (IOException ignored) { }
        }
        return out;
    }

    public static String load(String id) throws IOException {
        String path = "/examples/jobs/" + id + ".yaml";
        try (InputStream in = BundledJobs.class.getResourceAsStream(path)) {
            if (in == null) throw new IOException("no bundled job: " + id);
            return new String(in.readAllBytes());
        }
    }
}
