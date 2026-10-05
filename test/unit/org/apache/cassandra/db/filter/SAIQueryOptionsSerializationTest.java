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

import static org.apache.cassandra.db.filter.SAIQueryOptions.SAI_HYBRID_SORT_ORDER;
import static org.apache.cassandra.db.filter.SAIQueryOptions.SAI_INTERSECTION_CLAUSE_LIMIT;
import static org.apache.cassandra.db.filter.SAIQueryOptions.SAI_QUERY_OPTIMIZATION_LEVEL;
import static org.apache.cassandra.db.filter.SAIQueryOptions.SAI_USE_TERM_STATISTICS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link SAIQueryOptions.Serializer} round-trips correctly at VERSION_DS_20
 * and degrades gracefully (returns NONE) for older messaging versions.
 */
public class SAIQueryOptionsSerializationTest
{
    private static final SAIQueryOptions.Serializer serializer = SAIQueryOptions.serializer;

    // -------------------------------------------------------------------------
    // Round-trip at current version
    // -------------------------------------------------------------------------

    @Test
    public void testNoneRoundTrip() throws IOException
    {
        assertRoundTrip(SAIQueryOptions.NONE);
    }

    @Test
    public void testAllOptionsRoundTrip() throws IOException
    {
        SAIQueryOptions opts = SAIQueryOptions.fromMap(Map.of(
                SAI_QUERY_OPTIMIZATION_LEVEL, "0",
                SAI_INTERSECTION_CLAUSE_LIMIT, "7",
                SAI_USE_TERM_STATISTICS, "false",
                SAI_HYBRID_SORT_ORDER, "filter_then_sort"));
        assertRoundTrip(opts);
    }

    @Test
    public void testOptLevelOnlyRoundTrip() throws IOException
    {
        assertRoundTrip(SAIQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "1")));
    }

    @Test
    public void testIntersectionClauseLimitOnlyRoundTrip() throws IOException
    {
        assertRoundTrip(SAIQueryOptions.fromMap(Map.of(SAI_INTERSECTION_CLAUSE_LIMIT, "10")));
    }

    @Test
    public void testUseTermStatisticsOnlyRoundTrip() throws IOException
    {
        assertRoundTrip(SAIQueryOptions.fromMap(Map.of(SAI_USE_TERM_STATISTICS, "true")));
        assertRoundTrip(SAIQueryOptions.fromMap(Map.of(SAI_USE_TERM_STATISTICS, "false")));
    }

    @Test
    public void testHybridSortOrderRoundTrip() throws IOException
    {
        for (SAIQueryOptions.HybridSortOrder order : SAIQueryOptions.HybridSortOrder.values())
        {
            SAIQueryOptions opts = SAIQueryOptions.fromMap(Map.of(SAI_HYBRID_SORT_ORDER, order.name()));
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
            serializer.serialize(SAIQueryOptions.NONE, out, MessagingService.VERSION_DS_12);
            assertThat(serializer.serializedSize(SAIQueryOptions.NONE, MessagingService.VERSION_DS_12)).isEqualTo(0);
            assertThat(out.buffer().remaining()).isEqualTo(0);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            SAIQueryOptions result = serializer.deserialize(in, MessagingService.VERSION_DS_12);
            assertThat(result).isSameAs(SAIQueryOptions.NONE);
        }
    }

    @Test
    public void testOptionsThrowAtOlderVersionWhenNonNone()
    {
        SAIQueryOptions opts = SAIQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "0"));
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
        SAIQueryOptions[] cases = {
                SAIQueryOptions.NONE,
                SAIQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "0")),
                SAIQueryOptions.fromMap(Map.of(SAI_INTERSECTION_CLAUSE_LIMIT, "3")),
                SAIQueryOptions.fromMap(Map.of(SAI_USE_TERM_STATISTICS, "true")),
                SAIQueryOptions.fromMap(Map.of(SAI_HYBRID_SORT_ORDER, "sort_then_filter")),
                SAIQueryOptions.fromMap(Map.of(
                        SAI_QUERY_OPTIMIZATION_LEVEL, "1",
                        SAI_INTERSECTION_CLAUSE_LIMIT, "5",
                        SAI_USE_TERM_STATISTICS, "false",
                        SAI_HYBRID_SORT_ORDER, "auto")),
        };

        for (SAIQueryOptions opts : cases)
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

    private static void assertRoundTrip(SAIQueryOptions expected) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(expected, out, MessagingService.VERSION_DS_20);
            assertThat(out.buffer().remaining())
                    .isEqualTo((int) serializer.serializedSize(expected, MessagingService.VERSION_DS_20));

            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            SAIQueryOptions actual = serializer.deserialize(in, MessagingService.VERSION_DS_20);
            assertThat(actual).isEqualTo(expected);
        }
    }
}
