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

import static org.apache.cassandra.db.filter.SaiQueryOptions.SAI_HYBRID_SORT_ORDER;
import static org.apache.cassandra.db.filter.SaiQueryOptions.SAI_INTERSECTION_CLAUSE_LIMIT;
import static org.apache.cassandra.db.filter.SaiQueryOptions.SAI_QUERY_OPTIMIZATION_LEVEL;
import static org.apache.cassandra.db.filter.SaiQueryOptions.SAI_USE_TERM_STATISTICS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SaiQueryOptions}: parsing, validation, accessor fallback, toCQLString, and equals/hashCode.
 */
public class SaiQueryOptionsTest
{
    // -------------------------------------------------------------------------
    // NONE singleton
    // -------------------------------------------------------------------------

    @Test
    public void testNoneIsReturnedForEmptyMap()
    {
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of());
        assertThat(opts).isSameAs(SaiQueryOptions.NONE);
    }

    @Test
    public void testCreateWithAllNullsReturnsNone()
    {
        assertThat(SaiQueryOptions.create(null, null, null, null)).isSameAs(SaiQueryOptions.NONE);
    }

    // -------------------------------------------------------------------------
    // fromMap — valid inputs
    // -------------------------------------------------------------------------

    @Test
    public void testFromMapAllOptions()
    {
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of(
                SAI_QUERY_OPTIMIZATION_LEVEL, "0",
                SAI_INTERSECTION_CLAUSE_LIMIT, "5",
                SAI_USE_TERM_STATISTICS, "false",
                SAI_HYBRID_SORT_ORDER, "sort_then_filter"));

        assertThat(opts.queryOptimizationLevel).isEqualTo(0);
        assertThat(opts.intersectionClauseLimit).isEqualTo(5);
        assertThat(opts.useTermStatistics).isFalse();
        assertThat(opts.hybridSortOrder).isEqualTo(SaiQueryOptions.HybridSortOrder.SORT_THEN_FILTER);
    }

    @Test
    public void testFromMapOptLevelOne()
    {
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "1"));
        assertThat(opts.queryOptimizationLevel).isEqualTo(1);
    }

    @Test
    public void testFromMapUseTermStatisticsTrue()
    {
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of(SAI_USE_TERM_STATISTICS, "true"));
        assertThat(opts.useTermStatistics).isTrue();
    }

    @Test
    public void testFromMapHybridSortOrderAuto()
    {
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of(SAI_HYBRID_SORT_ORDER, "auto"));
        // AUTO is the default, but if explicitly set the field should be AUTO (not null)
        assertThat(opts.hybridSortOrder).isEqualTo(SaiQueryOptions.HybridSortOrder.AUTO);
    }

    @Test
    public void testFromMapHybridSortOrderFilterThenSort()
    {
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of(SAI_HYBRID_SORT_ORDER, "filter_then_sort"));
        assertThat(opts.hybridSortOrder).isEqualTo(SaiQueryOptions.HybridSortOrder.FILTER_THEN_SORT);
    }

    @Test
    public void testFromMapIsCaseInsensitiveForBooleans()
    {
        assertThat(SaiQueryOptions.fromMap(Map.of(SAI_USE_TERM_STATISTICS, "TRUE")).useTermStatistics).isTrue();
        assertThat(SaiQueryOptions.fromMap(Map.of(SAI_USE_TERM_STATISTICS, "False")).useTermStatistics).isFalse();
    }

    @Test
    public void testFromMapIsCaseInsensitiveForSortOrder()
    {
        assertThat(SaiQueryOptions.fromMap(Map.of(SAI_HYBRID_SORT_ORDER, "SORT_THEN_FILTER")).hybridSortOrder)
                .isEqualTo(SaiQueryOptions.HybridSortOrder.SORT_THEN_FILTER);
    }

    // -------------------------------------------------------------------------
    // fromMap — validation errors
    // -------------------------------------------------------------------------

    @Test
    public void testUnknownKeyThrows()
    {
        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of("unknown_key", "x")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("Unknown SAI query option: unknown_key");
    }

    @Test
    public void testOptLevelOutOfRangeThrows()
    {
        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "2")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(SAI_QUERY_OPTIMIZATION_LEVEL);

        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "-1")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(SAI_QUERY_OPTIMIZATION_LEVEL);
    }

    @Test
    public void testOptLevelNotAnIntThrows()
    {
        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "yes")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(SAI_QUERY_OPTIMIZATION_LEVEL);
    }

    @Test
    public void testIntersectionClauseLimitZeroThrows()
    {
        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of(SAI_INTERSECTION_CLAUSE_LIMIT, "0")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(SAI_INTERSECTION_CLAUSE_LIMIT);
    }

    @Test
    public void testIntersectionClauseLimitNegativeThrows()
    {
        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of(SAI_INTERSECTION_CLAUSE_LIMIT, "-1")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(SAI_INTERSECTION_CLAUSE_LIMIT);
    }

    @Test
    public void testIntersectionClauseLimitNotAnIntThrows()
    {
        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of(SAI_INTERSECTION_CLAUSE_LIMIT, "lots")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(SAI_INTERSECTION_CLAUSE_LIMIT);
    }

    @Test
    public void testUseTermStatisticsInvalidValueThrows()
    {
        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of(SAI_USE_TERM_STATISTICS, "yes")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(SAI_USE_TERM_STATISTICS);
    }

    @Test
    public void testHybridSortOrderInvalidValueThrows()
    {
        assertThatThrownBy(() -> SaiQueryOptions.fromMap(Map.of(SAI_HYBRID_SORT_ORDER, "random")))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(SAI_HYBRID_SORT_ORDER);
    }

    // -------------------------------------------------------------------------
    // Accessor fallback to globals
    // -------------------------------------------------------------------------

    @Test
    public void testNoneAccessorsFallBackToGlobals()
    {
        SaiQueryOptions none = SaiQueryOptions.NONE;
        assertThat(none.queryOptimizationLevel()).isEqualTo(CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt());
        assertThat(none.intersectionClauseLimit()).isEqualTo(CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt());
        assertThat(none.useTermStatistics()).isEqualTo(CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean());
        assertThat(none.hybridSortOrder()).isEqualTo(SaiQueryOptions.HybridSortOrder.AUTO);
    }

    @Test
    public void testPartialOptionsFallBackToGlobalsForAbsentFields()
    {
        // Only opt-level is set; everything else should fall back to globals.
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "0"));
        assertThat(opts.queryOptimizationLevel()).isEqualTo(0);
        assertThat(opts.intersectionClauseLimit()).isEqualTo(CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt());
        assertThat(opts.useTermStatistics()).isEqualTo(CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean());
        assertThat(opts.hybridSortOrder()).isEqualTo(SaiQueryOptions.HybridSortOrder.AUTO);
    }

    // -------------------------------------------------------------------------
    // toCQLString
    // -------------------------------------------------------------------------

    @Test
    public void testToCQLStringNoneIsEmpty()
    {
        assertThat(SaiQueryOptions.NONE.toCQLString()).isEmpty();
    }

    @Test
    public void testToCQLStringIncludesSetFields()
    {
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of(
                SAI_QUERY_OPTIMIZATION_LEVEL, "0",
                SAI_INTERSECTION_CLAUSE_LIMIT, "3",
                SAI_USE_TERM_STATISTICS, "true",
                SAI_HYBRID_SORT_ORDER, "filter_then_sort"));

        String cql = opts.toCQLString();
        assertThat(cql).contains("'sai_query_optimization_level': 0");
        assertThat(cql).contains("'sai_intersection_clause_limit': 3");
        assertThat(cql).contains("'sai_use_term_statistics': true");
        assertThat(cql).contains("'sai_hybrid_sort_order': 'FILTER_THEN_SORT'");
    }

    @Test
    public void testToCQLStringOmitsAbsentFields()
    {
        SaiQueryOptions opts = SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "1"));
        String cql = opts.toCQLString();
        assertThat(cql).contains(SAI_QUERY_OPTIMIZATION_LEVEL);
        assertThat(cql).doesNotContain(SAI_INTERSECTION_CLAUSE_LIMIT);
        assertThat(cql).doesNotContain(SAI_USE_TERM_STATISTICS);
        assertThat(cql).doesNotContain(SAI_HYBRID_SORT_ORDER);
    }

    // -------------------------------------------------------------------------
    // equals / hashCode
    // -------------------------------------------------------------------------

    @Test
    public void testEqualsAndHashCode()
    {
        SaiQueryOptions a = SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "0", SAI_INTERSECTION_CLAUSE_LIMIT, "4"));
        SaiQueryOptions b = SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "0", SAI_INTERSECTION_CLAUSE_LIMIT, "4"));
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    public void testNotEqualWhenFieldsDiffer()
    {
        SaiQueryOptions a = SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "0"));
        SaiQueryOptions b = SaiQueryOptions.fromMap(Map.of(SAI_QUERY_OPTIMIZATION_LEVEL, "1"));
        assertThat(a).isNotEqualTo(b);
    }
}
