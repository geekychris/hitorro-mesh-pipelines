/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nails down the hash-partitioning invariant that the shuffle-reduce
 * path depends on: rows with the same group-by key must always land in
 * the same bucket, otherwise the reducer sees a group split across
 * buckets and the aggregate is wrong.
 *
 * <p>Doesn't touch NATS — pure {@code pickBucket} logic. Testing this
 * separately from the connection-y bits keeps the fastest-feedback
 * suite (which never blocks a build waiting on a running server).</p>
 */
class ShuffleFanoutHashTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void sameKey_alwaysSameBucket() {
        // The invariant that makes shuffle-reduce work — a reducer
        // must see every row of its groups.
        JsonNode a1 = row("region", "NA", "n", 1);
        JsonNode a2 = row("region", "NA", "n", 999);
        JsonNode a3 = row("region", "NA", "extra", "junk");
        int b1 = ShuffleFanoutSink.pickBucket(a1, "region", 8);
        int b2 = ShuffleFanoutSink.pickBucket(a2, "region", 8);
        int b3 = ShuffleFanoutSink.pickBucket(a3, "region", 8);
        assertThat(b1).isEqualTo(b2).isEqualTo(b3);
    }

    @Test
    void nullKey_deterministicNullBucket() {
        // A row where the key path resolves to null or missing goes to
        // the "<null>" sentinel bucket. All null-keyed rows collapse
        // together — the reducer for that bucket sees them as one group.
        JsonNode missing   = row("other", "x");                // no "region"
        JsonNode explicit  = row("region", (String) null);
        int b1 = ShuffleFanoutSink.pickBucket(missing,  "region", 8);
        int b2 = ShuffleFanoutSink.pickBucket(explicit, "region", 8);
        assertThat(b1).isEqualTo(b2);
    }

    @Test
    void allBucketsAreInRange() {
        // Fuzz 1000 random keys and prove no bucket escapes [0, buckets).
        // Guards against negative-hashCode overflow in the bit-mask logic.
        int buckets = 16;
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i + "-" + (i * 31337L);
            int b = ShuffleFanoutSink.pickBucket(row("k", key), "k", buckets);
            assertThat(b).isBetween(0, buckets - 1);
        }
    }

    @Test
    void keyDistributionSpreadsAcrossBuckets() {
        // With 1000 distinct keys and 8 buckets, every bucket should
        // receive at least a few rows — proves hash isn't degenerate.
        // Not a uniformity test (hashCode's not a great hash) — just a
        // smoke test that we're not funnelling everything into bucket 0.
        int buckets = 8;
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            int b = ShuffleFanoutSink.pickBucket(row("k", "u-" + i), "k", buckets);
            counts.merge(b, 1, Integer::sum);
        }
        Set<Integer> nonEmpty = new HashSet<>(counts.keySet());
        assertThat(nonEmpty).hasSizeGreaterThanOrEqualTo(buckets - 1);
    }

    @Test
    void dottedKeyPath_walksNestedFields() {
        // Real jobs group on paths like "user.country" — verify the
        // pluck walks the tree instead of literal-matching "user.country".
        ObjectNode outer = JSON.createObjectNode();
        outer.putObject("user").put("country", "GB");
        int bGB1 = ShuffleFanoutSink.pickBucket(outer, "user.country", 8);
        outer.with("user").put("country", "GB");                    // same value
        int bGB2 = ShuffleFanoutSink.pickBucket(outer, "user.country", 8);
        outer.with("user").put("country", "US");
        int bUS  = ShuffleFanoutSink.pickBucket(outer, "user.country", 8);
        assertThat(bGB1).isEqualTo(bGB2);
        assertThat(bGB1).isNotEqualTo(bUS);
    }

    @Test
    void singleBucket_alwaysReturnsZero() {
        // Edge: reducer count of 1 → every row goes to bucket 0.
        for (int i = 0; i < 50; i++) {
            assertThat(ShuffleFanoutSink.pickBucket(row("k", "u-" + i), "k", 1))
                    .isEqualTo(0);
        }
    }

    // ------------------------------------------------------------ helpers

    /** Alternating key/value; values may be String, Integer, or null. */
    private static ObjectNode row(Object... kv) {
        ObjectNode n = JSON.createObjectNode();
        for (int i = 0; i < kv.length; i += 2) {
            String k = (String) kv[i];
            Object v = kv[i + 1];
            if (v == null) n.putNull(k);
            else if (v instanceof Integer x) n.put(k, x);
            else n.put(k, String.valueOf(v));
        }
        return n;
    }
}
