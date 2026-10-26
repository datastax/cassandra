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

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.db.filter.OptimizerOptions.HYBRID_SORT_ORDER;
import static org.apache.cassandra.db.filter.OptimizerOptions.INTERSECTION_CLAUSE_LIMIT;
import static org.apache.cassandra.db.filter.OptimizerOptions.QUERY_OPTIMIZATION_LEVEL;
import static org.apache.cassandra.db.filter.OptimizerOptions.USE_TERM_STATISTICS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link OptimizerOptions}: parsing, validation, accessor fallback, toCQLString, and equals/hashCode.
 */
public class OptimizerOptionsTest
{
    // -------------------------------------------------------------------------
    // NONE singleton
    // -------------------------------------------------------------------------

    @Test
    public void testNoneIsReturnedForEmptyMap()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of());
        assertThat(opts).isSameAs(OptimizerOptions.NONE);
    }

    @Test
    public void testCreateWithAllNullsReturnsNone()
    {
        assertThat(OptimizerOptions.create(null, null, null, null)).isSameAs(OptimizerOptions.NONE);
    }

    // -------------------------------------------------------------------------
    // fromMap — valid inputs
    // -------------------------------------------------------------------------

    @Test
    public void testFromMapAllOptions()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(
                QUERY_OPTIMIZATION_LEVEL, "0",
                INTERSECTION_CLAUSE_LIMIT, "5",
                USE_TERM_STATISTICS, "false",
                HYBRID_SORT_ORDER, "sort_then_filter"));

        assertThat(opts.queryOptimizationLevel()).isEqualTo(0);
        assertThat(opts.intersectionClauseLimit()).isEqualTo(5);
        assertThat(opts.useTermStatistics()).isFalse();
        assertThat(opts.hybridSortOrder()).isEqualTo(OptimizerOptions.HybridSortOrder.SORT_THEN_FILTER);
    }

    @Test
    public void testFromMapOptLevelOne()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "1"));
        assertThat(opts.queryOptimizationLevel()).isEqualTo(1);
    }

    @Test
    public void testFromMapUseTermStatisticsTrue()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(USE_TERM_STATISTICS, "true"));
        assertThat(opts.useTermStatistics()).isTrue();
    }

    @Test
    public void testFromMapHybridSortOrderAuto()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(HYBRID_SORT_ORDER, "auto"));
        // AUTO is the default, but if explicitly set the field should be AUTO (not null)
        assertThat(opts.hybridSortOrder()).isEqualTo(OptimizerOptions.HybridSortOrder.AUTO);
    }

    @Test
    public void testFromMapHybridSortOrderFilterThenSort()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(HYBRID_SORT_ORDER, "filter_then_sort"));
        assertThat(opts.hybridSortOrder()).isEqualTo(OptimizerOptions.HybridSortOrder.FILTER_THEN_SORT);
    }

    @Test
    public void testFromMapIsCaseInsensitiveForBooleans()
    {
        assertThat(OptimizerOptions.fromMap(Map.of(USE_TERM_STATISTICS, "TRUE")).useTermStatistics()).isTrue();
        assertThat(OptimizerOptions.fromMap(Map.of(USE_TERM_STATISTICS, "False")).useTermStatistics()).isFalse();
    }

    @Test
    public void testFromMapIsCaseInsensitiveForSortOrder()
    {
        assertThat(OptimizerOptions.fromMap(Map.of(HYBRID_SORT_ORDER, "SORT_THEN_FILTER")).hybridSortOrder())
                .isEqualTo(OptimizerOptions.HybridSortOrder.SORT_THEN_FILTER);
    }

    // -------------------------------------------------------------------------
    // fromMap — validation errors
    // -------------------------------------------------------------------------

    @Test
    public void testUnknownKeyThrows()
    {
        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of("unknown_key", "x")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("Unknown SAI optimizer option: unknown_key");
    }

    @Test
    public void testOptLevelOutOfRangeThrows()
    {
        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "2")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(QUERY_OPTIMIZATION_LEVEL);

        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "-1")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(QUERY_OPTIMIZATION_LEVEL);
    }

    @Test
    public void testOptLevelNotAnIntThrows()
    {
        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "yes")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(QUERY_OPTIMIZATION_LEVEL);
    }

    @Test
    public void testIntersectionClauseLimitZeroThrows()
    {
        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of(INTERSECTION_CLAUSE_LIMIT, "0")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(INTERSECTION_CLAUSE_LIMIT);
    }

    @Test
    public void testIntersectionClauseLimitNegativeThrows()
    {
        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of(INTERSECTION_CLAUSE_LIMIT, "-1")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(INTERSECTION_CLAUSE_LIMIT);
    }

    @Test
    public void testIntersectionClauseLimitNotAnIntThrows()
    {
        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of(INTERSECTION_CLAUSE_LIMIT, "lots")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(INTERSECTION_CLAUSE_LIMIT);
    }

    @Test
    public void testUseTermStatisticsInvalidValueThrows()
    {
        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of(USE_TERM_STATISTICS, "yes")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(USE_TERM_STATISTICS);
    }

    @Test
    public void testHybridSortOrderInvalidValueThrows()
    {
        assertThatThrownBy(() -> OptimizerOptions.fromMap(Map.of(HYBRID_SORT_ORDER, "random")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(HYBRID_SORT_ORDER);
    }

    // -------------------------------------------------------------------------
    // Accessor fallback to globals
    // -------------------------------------------------------------------------

    @Test
    public void testNoneAccessorsFallBackToGlobals()
    {
        OptimizerOptions none = OptimizerOptions.NONE;
        assertThat(none.queryOptimizationLevel()).isEqualTo(CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt());
        assertThat(none.intersectionClauseLimit()).isEqualTo(CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt());
        assertThat(none.useTermStatistics()).isEqualTo(CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean());
        assertThat(none.hybridSortOrder()).isEqualTo(OptimizerOptions.HybridSortOrder.AUTO);
    }

    @Test
    public void testPartialOptionsFallBackToGlobalsForAbsentFields()
    {
        // Only opt-level is set; everything else should fall back to globals.
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "0"));
        assertThat(opts.queryOptimizationLevel()).isEqualTo(0);
        assertThat(opts.intersectionClauseLimit()).isEqualTo(CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt());
        assertThat(opts.useTermStatistics()).isEqualTo(CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean());
        assertThat(opts.hybridSortOrder()).isEqualTo(OptimizerOptions.HybridSortOrder.AUTO);
    }

    // -------------------------------------------------------------------------
    // toCQLString
    // -------------------------------------------------------------------------

    @Test
    public void testToCQLStringNoneIsEmpty()
    {
        assertThat(OptimizerOptions.NONE.toCQLString()).isEmpty();
    }

    @Test
    public void testToCQLStringIncludesSetFields()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(
                QUERY_OPTIMIZATION_LEVEL, "0",
                INTERSECTION_CLAUSE_LIMIT, "3",
                USE_TERM_STATISTICS, "true",
                HYBRID_SORT_ORDER, "filter_then_sort"));

        String cql = opts.toCQLString();
        assertThat(cql).contains("'query_optimization_level': 0");
        assertThat(cql).contains("'intersection_clause_limit': 3");
        assertThat(cql).contains("'use_term_statistics': true");
        assertThat(cql).contains("'hybrid_sort_order': 'FILTER_THEN_SORT'");
    }

    @Test
    public void testToCQLStringOmitsAbsentFields()
    {
        OptimizerOptions opts = OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "1"));
        String cql = opts.toCQLString();
        assertThat(cql).contains(QUERY_OPTIMIZATION_LEVEL);
        assertThat(cql).doesNotContain(INTERSECTION_CLAUSE_LIMIT);
        assertThat(cql).doesNotContain(USE_TERM_STATISTICS);
        assertThat(cql).doesNotContain(HYBRID_SORT_ORDER);
    }

    // -------------------------------------------------------------------------
    // equals / hashCode
    // -------------------------------------------------------------------------

    @Test
    public void testEqualsAndHashCode()
    {
        OptimizerOptions a = OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "0", INTERSECTION_CLAUSE_LIMIT, "4"));
        OptimizerOptions b = OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "0", INTERSECTION_CLAUSE_LIMIT, "4"));
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    public void testNotEqualWhenFieldsDiffer()
    {
        OptimizerOptions a = OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "0"));
        OptimizerOptions b = OptimizerOptions.fromMap(Map.of(QUERY_OPTIMIZATION_LEVEL, "1"));
        assertThat(a).isNotEqualTo(b);
    }
}
