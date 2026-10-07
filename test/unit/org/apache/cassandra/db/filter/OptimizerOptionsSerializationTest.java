/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.db.filter.OptimizerOptions.HYBRID_SORT_ORDER;
import static org.apache.cassandra.db.filter.OptimizerOptions.INTERSECTION_CLAUSE_LIMIT;
import static org.apache.cassandra.db.filter.OptimizerOptions.QUERY_OPTIMIZATION_LEVEL;
import static org.apache.cassandra.db.filter.OptimizerOptions.USE_TERM_STATISTICS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link OptimizerOptions.Serializer} round-trips correctly at VERSION_DS_20
 * and degrades gracefully (returns NONE) for older messaging versions.
 */
public class OptimizerOptionsSerializationTest
{
    private static final OptimizerOptions.Serializer serializer = OptimizerOptions.serializer;

    // -------------------------------------------------------------------------
    // Round-trip at current version
    // -------------------------------------------------------------------------

    @Test
    public void testNoneRoundTrip() throws IOException
    {
        assertRoundTrip(OptimizerOptions.NONE);
    }

    @Test
    public void testAllOptionsRoundTrip() throws IOException
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(
                QUERY_OPTIMIZATION_LEVEL, "0",
                INTERSECTION_CLAUSE_LIMIT, "7",
                USE_TERM_STATISTICS, "false",
                HYBRID_SORT_ORDER, "filter_then_sort"));
        assertRoundTrip(opts);
    }

    @Test
    public void testOptLevelOnlyRoundTrip() throws IOException
    {
        assertRoundTrip(OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "1")));
    }

    @Test
    public void testIntersectionClauseLimitOnlyRoundTrip() throws IOException
    {
        assertRoundTrip(OptimizerOptions.fromMap(Map.of(INTERSECTION_CLAUSE_LIMIT, "10")));
    }

    @Test
    public void testUseTermStatisticsOnlyRoundTrip() throws IOException
    {
        assertRoundTrip(OptimizerOptions.fromMap(Map.of(USE_TERM_STATISTICS, "true")));
        assertRoundTrip(OptimizerOptions.fromMap(Map.of(USE_TERM_STATISTICS, "false")));
    }

    @Test
    public void testHybridSortOrderRoundTrip() throws IOException
    {
        for (OptimizerOptions.HybridSortOrder order : OptimizerOptions.HybridSortOrder.values())
        {
            OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(HYBRID_SORT_ORDER, order.name()));
            assertRoundTrip(opts);
        }
    }

    // -------------------------------------------------------------------------
    // Older messaging version — NONE should pass through, options silently dropped
    // -------------------------------------------------------------------------

    @Test
    public void testNoneSerializesAtOlderVersion() throws IOException
    {
        // NONE should serialize to zero bytes and deserialize back to NONE at old versions
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(OptimizerOptions.NONE, out, MessagingService.VERSION_DS_12);
            assertThat(serializer.serializedSize(OptimizerOptions.NONE, MessagingService.VERSION_DS_12)).isEqualTo(0);
            assertThat(out.buffer().remaining()).isEqualTo(0);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            OptimizerOptions result = serializer.deserialize(in, MessagingService.VERSION_DS_12);
            assertThat(result).isSameAs(OptimizerOptions.NONE);
        }
    }

    @Test
    public void testOptionsThrowAtOlderVersionWhenNonNone()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "0"));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> serializer.serialize(opts, out, MessagingService.VERSION_DS_12))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(String.valueOf(MessagingService.VERSION_DS_12));
        }
    }

    // -------------------------------------------------------------------------
    // serializedSize matches actual bytes written
    // -------------------------------------------------------------------------

    @Test
    public void testSerializedSizeMatchesBytesWritten() throws IOException
    {
        OptimizerOptions[] cases = {
                OptimizerOptions.NONE,
                OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "0")),
                OptimizerOptions.fromMap(Map.of(INTERSECTION_CLAUSE_LIMIT, "3")),
                OptimizerOptions.fromMap(Map.of(USE_TERM_STATISTICS, "true")),
                OptimizerOptions.fromMap(Map.of(HYBRID_SORT_ORDER, "sort_then_filter")),
                OptimizerOptions.fromMap(Map.of(
                        QUERY_OPTIMIZATION_LEVEL, "1",
                        INTERSECTION_CLAUSE_LIMIT, "5",
                        USE_TERM_STATISTICS, "false",
                        HYBRID_SORT_ORDER, "auto")),
        };

        for (OptimizerOptions opts : cases)
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                serializer.serialize(opts, out, MessagingService.VERSION_DS_20);
                assertThat(out.buffer().remaining())
                        .as("serializedSize mismatch for %s", opts)
                        .isEqualTo((int) serializer.serializedSize(opts, MessagingService.VERSION_DS_20));
            }
        }
    }

    // -------------------------------------------------------------------------
    // helper
    // -------------------------------------------------------------------------

    private static void assertRoundTrip(OptimizerOptions expected) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(expected, out, MessagingService.VERSION_DS_20);
            assertThat(out.buffer().remaining())
                    .isEqualTo((int) serializer.serializedSize(expected, MessagingService.VERSION_DS_20));

            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            OptimizerOptions actual = serializer.deserialize(in, MessagingService.VERSION_DS_20);
            assertThat(actual).isEqualTo(expected);
        }
    }
}
