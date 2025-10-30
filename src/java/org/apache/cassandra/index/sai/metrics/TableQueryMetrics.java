/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.metrics;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Table query metrics for different kinds of query. The metrics for each type of query are divided into two groups:
 * <ul>
 *    <li>Per table counters ({@link PerTable}).</li>
 *    <li>Per query timers and histograms ({@link PerQuery}).</li>
 * </ul>
 * The following kinds of query are tracked:
 * <ul>
 *    <li>All SAI queries.</li>
 *    <li>Single-partition filter queries (filtering only, no top-k).</li>
 *    <li>Multi-partition filter queries (filtering only, no top-k).</li>
 *    <li>Single-partition top-k queries (top-k only, no filtering).</li>
 *    <li>Multi-partition top-k queries (top-k only, no filtering).</li>
 *    <li>Single-partition hybrid queries (both filtering and top-k).</li>
 *    <li>Multi-partition hybrid queries (both filtering and top-k).</li>
 * </ul>
 * The general metrics for all SAI queries are always recorded. The other kinds of queries are recorded only if they are
 * enabled via the {@link CassandraRelevantProperties#SAI_QUERY_KIND_PER_TABLE_METRICS_ENABLED} and
 * {@link CassandraRelevantProperties#SAI_QUERY_KIND_PER_QUERY_METRICS_ENABLED} system properties.
 */
public class TableQueryMetrics
{
    /** Per table metrics for all kinds of queries (counters). */
    public final EnumMap<QueryKind, PerTable> perTableMetrics = new EnumMap<>(QueryKind.class);

    /** Per query metrics for all kinds of queries (timers and histograms). */
    public final EnumMap<QueryKind, PerQuery> perQueryMetrics = new EnumMap<>(QueryKind.class);

    public TableQueryMetrics(TableMetadata table)
    {
        addMetrics(table, QueryKind.ALL, cmd -> true);
        addMetrics(table, QueryKind.SP_FILTER_ONLY, cmd -> !cmd.isTopK() && cmd.usesIndexFiltering() && cmd.isSinglePartition()); // single-partition-queries that are filtering only
        addMetrics(table, QueryKind.MP_FILTER_ONLY, cmd -> !cmd.isTopK() && cmd.usesIndexFiltering() && !cmd.isSinglePartition()); // multi-partition queries that are filtering only
        addMetrics(table, QueryKind.SP_TOPK_ONLY, cmd -> cmd.isTopK() && !cmd.usesIndexFiltering() && cmd.isSinglePartition()); // single-partition queries that are top-k only
        addMetrics(table, QueryKind.MP_TOPK_ONLY, cmd -> cmd.isTopK() && !cmd.usesIndexFiltering() && !cmd.isSinglePartition()); // multi-partition queries that are top-k only
        addMetrics(table, QueryKind.SP_HYBRID, cmd -> cmd.isTopK() && cmd.usesIndexFiltering() && cmd.isSinglePartition()); // single-partition queries that are both filtering and top-k
        addMetrics(table, QueryKind.MP_HYBRID, cmd -> cmd.isTopK() && cmd.usesIndexFiltering() && !cmd.isSinglePartition()); // multi-partition queries that are both filtering and top-k
    }

    public enum QueryKind
    {
        ALL(""),
        SP_FILTER_ONLY("SinglePartitionFilterOnly"),
        MP_FILTER_ONLY("MultiPartitionFilterOnly"),
        SP_TOPK_ONLY("SinglePartitionTopKOnly"),
        MP_TOPK_ONLY("MultiPartitionTopKOnly"),
        SP_HYBRID("SinglePartitionHybrid"),
        MP_HYBRID("MultiPartitionHybrid");

        private final String name;

        QueryKind(String name)
        {
            this.name = name;
        }
    }

    private void addMetrics(TableMetadata table, QueryKind queryKind, Predicate<ReadCommand> filter)
    {
        if (queryKind == QueryKind.ALL || CassandraRelevantProperties.SAI_QUERY_KIND_PER_TABLE_METRICS_ENABLED.getBoolean())
            perTableMetrics.put(queryKind, new PerTable(table, queryKind, filter));

        if (queryKind == QueryKind.ALL || CassandraRelevantProperties.SAI_QUERY_KIND_PER_QUERY_METRICS_ENABLED.getBoolean())
            perQueryMetrics.put(queryKind, new PerQuery(table, queryKind, filter));
    }

    /**
     * Records metrics for a single query.
     *
     * @param context the stats relevant to the execution of a single query
     * @param command the query command
     */
    public void record(QueryContext context, ReadCommand command)
    {
        QueryContext.Snapshot snapshot = new QueryContext.Snapshot(context);
        perTableMetrics.values().forEach(m -> m.record(snapshot, command));
        perQueryMetrics.values().forEach(m -> m.record(snapshot, command));

        if (Tracing.isTracing())
        {
            final long queryLatencyMicros = TimeUnit.NANOSECONDS.toMicros(snapshot.totalQueryTimeNs);

            if (snapshot.filterSortOrder == QueryContext.FilterSortOrder.SEARCH_THEN_ORDER)
            {
                Tracing.trace("Index query accessed memtable indexes, {}, and {}, selected {} before ranking, " +
                              "post-filtered {} in {}, and took {} microseconds.",
                              pluralize(snapshot.sstablesHit, "SSTable index", "es"),
                              pluralize(snapshot.segmentsHit, "segment", "s"),
                              pluralize(snapshot.rowsPreFiltered, "row", "s"),
                              pluralize(snapshot.rowsFiltered, "row", "s"),
                              pluralize(snapshot.partitionsRead, "partition", "s"),
                              queryLatencyMicros);
            }
            else
            {
                Tracing.trace("Index query accessed memtable indexes, {}, and {}, post-filtered {} in {}, " +
                              "and took {} microseconds.",
                              pluralize(snapshot.sstablesHit, "SSTable index", "es"),
                              pluralize(snapshot.segmentsHit, "segment", "s"),
                              pluralize(snapshot.rowsFiltered, "row", "s"),
                              pluralize(snapshot.partitionsRead, "partition", "s"),
                              queryLatencyMicros);
            }
        }
    }

    /**
     * Releases all the resources used by these metrics.
     */
    public void release()
    {
        perTableMetrics.values().forEach(PerTable::release);
        perQueryMetrics.values().forEach(PerQuery::release);
    }

    private static String pluralize(long count, String root, String plural)
    {
        return count == 1 ? String.format("1 %s", root) : String.format("%d %s%s", count, root, plural);
    }

    /**
     * Family of metrics for a specific kind of query.
     */
    public abstract static class AbstractQueryMetrics extends AbstractMetrics
    {
        private static final Pattern PATTERN = Pattern.compile("Query");

        private final Predicate<ReadCommand> filter;

        private AbstractQueryMetrics(String keyspace, String table, String scope, QueryKind queryKind, Predicate<ReadCommand> filter)
        {
            super(keyspace, table, makeName(scope, queryKind));
            this.filter = filter;
        }

        public final void record(QueryContext.Snapshot snapshot, ReadCommand command)
        {
            if (filter.test(command))
                record(snapshot);
        }

        protected abstract void record(QueryContext.Snapshot snapshot);

        public static String makeName(String scope, QueryKind queryKind)
        {
            return PATTERN.matcher(scope).replaceFirst(queryKind.name + "Query");
        }
    }

    /**
     * Per table metrics for a specific kind of query. These metrics are always counters.
     */
    public static class PerTable extends AbstractQueryMetrics
    {
        public static final String METRIC_TYPE = "TableQueryMetrics";

        public final Counter totalQueryTimeouts;
        public final Counter totalPartitionReads;
        public final Counter totalRowsFiltered;
        public final Counter totalQueriesCompleted;

        public final Counter sortThenFilterQueriesCompleted;
        public final Counter filterThenSortQueriesCompleted;

        /**
         * @param table the table to measure metrics for
         * @param queryKind an identifier for the kind of query which metrics are being recorded for
         * @param filter a predicate that determines whether a given query should be recorded
         */
        public PerTable(TableMetadata table, QueryKind queryKind, Predicate<ReadCommand> filter)
        {
            super(table.keyspace, table.name, METRIC_TYPE, queryKind, filter);

            totalPartitionReads = Metrics.counter(createMetricName("TotalPartitionReads"));
            totalRowsFiltered = Metrics.counter(createMetricName("TotalRowsFiltered"));
            totalQueriesCompleted = Metrics.counter(createMetricName("TotalQueriesCompleted"));
            totalQueryTimeouts = Metrics.counter(createMetricName("TotalQueryTimeouts"));

            sortThenFilterQueriesCompleted = Metrics.counter(createMetricName("SortThenFilterQueriesCompleted"));
            filterThenSortQueriesCompleted = Metrics.counter(createMetricName("FilterThenSortQueriesCompleted"));
        }

        @Override
        public void record(QueryContext.Snapshot snapshot)
        {
            if (snapshot.queryTimeouts > 0)
            {
                assert snapshot.queryTimeouts == 1;
                totalQueryTimeouts.inc();
            }

            totalQueriesCompleted.inc();
            totalPartitionReads.inc(snapshot.partitionsRead);
            totalRowsFiltered.inc(snapshot.rowsFiltered);

            if (snapshot.filterSortOrder == QueryContext.FilterSortOrder.SCAN_THEN_FILTER)
                sortThenFilterQueriesCompleted.inc();
            else if (snapshot.filterSortOrder == QueryContext.FilterSortOrder.SEARCH_THEN_ORDER)
                filterThenSortQueriesCompleted.inc();
        }
    }

    /**
     * Per query metrics for a specific kind of query. These metrics are always timers and histograms.
     */
    public static class PerQuery extends AbstractQueryMetrics
    {
        public static final String METRIC_TYPE = "PerQuery";

        public final Timer queryLatency;

        /**
         * Global metrics for all indices hit during the query.
         */
        public final Histogram sstablesHit;
        public final Histogram segmentsHit;
        public final Histogram partitionReads;
        public final Histogram rowsFiltered;

        /**
         * BKD index metrics.
         */
        public final Histogram kdTreePostingsNumPostings;
        /**
         * BKD index posting lists metrics.
         */
        public final Histogram kdTreePostingsSkips;
        public final Histogram kdTreePostingsDecodes;

        /** Shadowed keys scan metrics **/
        public final Histogram shadowedKeysScannedHistogram;

        /**
         * Trie index posting lists metrics.
         */
        public final Histogram postingsSkips;
        public final Histogram postingsDecodes;

        /**
         * Cumulative time spent searching ANN graph.
         */
        public final Timer annGraphSearchLatency;

        /**
         * @param table the table to measure metrics for
         * @param queryKind an identifier for the kind of query which metrics are being recorded for
         * @param filter a predicate that determines whether a given query should be recorded
         */
        public PerQuery(TableMetadata table, QueryKind queryKind, Predicate<ReadCommand> filter)
        {
            super(table.keyspace, table.name, METRIC_TYPE, queryKind, filter);

            queryLatency = Metrics.timer(createMetricName("QueryLatency"));

            sstablesHit = Metrics.histogram(createMetricName("SSTableIndexesHit"), false);
            segmentsHit = Metrics.histogram(createMetricName("IndexSegmentsHit"), false);

            kdTreePostingsSkips = Metrics.histogram(createMetricName("KDTreePostingsSkips"), true);
            kdTreePostingsNumPostings = Metrics.histogram(createMetricName("KDTreePostingsNumPostings"), false);
            kdTreePostingsDecodes = Metrics.histogram(createMetricName("KDTreePostingsDecodes"), false);

            postingsSkips = Metrics.histogram(createMetricName("PostingsSkips"), true);
            postingsDecodes = Metrics.histogram(createMetricName("PostingsDecodes"), false);

            partitionReads = Metrics.histogram(createMetricName("PartitionReads"), false);
            rowsFiltered = Metrics.histogram(createMetricName("RowsFiltered"), false);

            shadowedKeysScannedHistogram = Metrics.histogram(createMetricName("ShadowedKeysScannedHistogram"), false);

            // Key vector metrics that translate to performance
            annGraphSearchLatency = Metrics.timer(createMetricName("ANNGraphSearchLatency"));
        }

        @Override
        public void record(QueryContext.Snapshot snapshot)
        {
            queryLatency.update(snapshot.totalQueryTimeNs, TimeUnit.NANOSECONDS);
            sstablesHit.update(snapshot.sstablesHit);
            segmentsHit.update(snapshot.segmentsHit);
            partitionReads.update(snapshot.partitionsRead);
            rowsFiltered.update(snapshot.rowsFiltered);

            // Record string index cache metrics.
            if (snapshot.trieSegmentsHit > 0)
            {
                postingsSkips.update(snapshot.triePostingsSkips);
                postingsDecodes.update(snapshot.triePostingsDecodes);
            }

            // Record numeric index cache metrics.
            if (snapshot.bkdSegmentsHit > 0)
            {
                kdTreePostingsNumPostings.update(snapshot.bkdPostingListsHit);
                kdTreePostingsSkips.update(snapshot.bkdPostingsSkips);
                kdTreePostingsDecodes.update(snapshot.bkdPostingsDecodes);
            }

            // Record vector index metrics.
            // If ann brute forced the whole search, this is 0. We don't measure brute force latency. Maybe we should?
            // At the very least, we collect brute force comparison metrics, which should give a reasonable indicator
            // of work done.
            if (snapshot.annGraphSearchLatency > 0)
            {
                annGraphSearchLatency.update(snapshot.annGraphSearchLatency, TimeUnit.NANOSECONDS);
            }

            shadowedKeysScannedHistogram.update(snapshot.shadowedPrimaryKeyCount);
        }
    }
}
