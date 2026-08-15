/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Bridges a completed pipeline sink into a mesh broadcast table by
 * materialising the sink's rows into the mesh's datasets directory and
 * registering the name with the driver.
 *
 * <p>Expected outcome after this runs:</p>
 * <pre>
 *   $HITORRO_DATASETS_HOME/&lt;name&gt;/
 *     manifest.yaml
 *     types/&lt;table_name&gt;.json
 *     data/&lt;table_name&gt;.ndjson
 * </pre>
 * <p>+ a POST to {@code /mesh/broadcast-tables} on the local driver so
 * the SQL planner accepts JOINs against the name. Actual data at the
 * agent side requires a mesh-init-data.sh + mesh-up restart cycle to
 * load — noted in the summary event on the job status.</p>
 *
 * <p>This is a driver-side bridge; agents still need the file to appear
 * in their configured broadcast-tables list before {@code SELECT} works.
 * The full end-to-end (no restart) is Phase 4 work.</p>
 */
public final class MeshTableBridge {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private MeshTableBridge() { }

    /**
     * Materialise rows + register the table.
     *
     * @param tableName      symbolic name (kebab-case OK; snake_case used
     *                       for the SQL name — kebabs converted).
     * @param rows           snapshot of rows to write
     * @param datasetsHome   root of the mesh datasets home
     *                       (usually {@code ~/.hitorro/datasets})
     * @param driverBaseUrl  where the driver's {@code /mesh/broadcast-
     *                       tables} lives, e.g. {@code http://localhost:8085}
     * @return a summary string suitable for logging into a JobStatus event
     */
    public static String materializeAndRegister(String tableName,
                                                List<JsonNode> rows,
                                                Path datasetsHome,
                                                String driverBaseUrl) throws Exception {
        String snakeName = tableName.replace('-', '_');
        Path root  = datasetsHome.resolve(tableName);
        Path types = root.resolve("types");
        Path data  = root.resolve("data");
        Files.createDirectories(types);
        Files.createDirectories(data);

        // Write NDJSON — one row per line, plain (unbz).
        Path ndjson = data.resolve(snakeName + ".ndjson");
        try (BufferedWriter w = Files.newBufferedWriter(ndjson, StandardCharsets.UTF_8)) {
            for (JsonNode r : rows) {
                w.write(JSON.writeValueAsString(r));
                w.write('\n');
            }
        }

        // Emit a lean type definition — best-effort field discovery from
        // the first row. All fields typed core_string; users can hand-edit.
        ObjectNode type = JSON.createObjectNode();
        type.put("name", snakeName);
        var fields = type.putArray("fields");
        if (!rows.isEmpty() && rows.get(0).isObject()) {
            var it = rows.get(0).fields();
            while (it.hasNext()) {
                var e = it.next();
                var f = fields.addObject();
                f.put("name", e.getKey());
                JsonNode v = e.getValue();
                if      (v.isBoolean())         f.put("type", "core_boolean");
                else if (v.isIntegralNumber())  f.put("type", "core_long");
                else if (v.isFloatingPointNumber()) f.put("type", "core_double");
                else                            f.put("type", "core_string");
            }
        }
        Files.writeString(types.resolve(snakeName + ".json"),
                JSON.writerWithDefaultPrettyPrinter().writeValueAsString(type),
                StandardCharsets.UTF_8);

        // Minimal manifest so any auto-registration path (mesh-init-data.sh)
        // treats this as a broadcast table.
        String manifest =
                "id: " + tableName + "\n" +
                "title: pipeline-generated " + tableName + "\n" +
                "version: \"1\"\n" +
                "description: |\n" +
                "  Materialised from a pipelines kvstore sink at\n" +
                "  " + java.time.Instant.now() + ".\n" +
                "license:\n" +
                "  spdx: LicenseRef-Internal\n" +
                "record:\n" +
                "  type: pipeline." + snakeName + "\n" +
                "  fields: []\n" +
                "partitionBy: null\n";
        Files.writeString(root.resolve("manifest.yaml"), manifest, StandardCharsets.UTF_8);

        // POST /mesh/broadcast-tables — driver-side registration.
        String body = JSON.writeValueAsString(java.util.Map.of("name", snakeName));
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(driverBaseUrl + "/mesh/broadcast-tables"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        String registerStatus = (resp.statusCode() == 200) ? "registered" : "http " + resp.statusCode();

        return String.format(
                "materialised %d rows → %s · driver: %s · restart agents to load",
                rows.size(), ndjson.toAbsolutePath(), registerStatus);
    }
}
