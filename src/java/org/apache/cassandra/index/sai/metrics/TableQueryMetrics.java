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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TableQueryMetrics extends AbstractMetrics
{
    public static final String TABLE_QUERY_METRIC_TYPE = "TableQueryMetrics";

    public final Timer postFilteringReadLatency;

    public final PerQueryMetrics perQueryMetrics;

    public final Counter totalQueryTimeouts;
    public final Counter totalPartitionReads;
    public final Counter totalRowsFiltered;
    public final Counter totalQueriesCompleted;

    public final Counter sortThenFilterQueriesCompleted;
    public final Counter filterThenSortQueriesCompleted;

    public TableQueryMetrics(TableMetadata table)
    {
        super(table.keyspace, table.name, TABLE_QUERY_METRIC_TYPE);

        perQueryMetrics = new PerQueryMetrics(table);

        postFilteringReadLatency = Metrics.timer(createMetricName("PostFilteringReadLatency"));

        totalPartitionReads = Metrics.counter(createMetricName("TotalPartitionReads"));
        totalRowsFiltered = Metrics.counter(createMetricName("TotalRowsFiltered"));
        totalQueriesCompleted = Metrics.counter(createMetricName("TotalQueriesCompleted"));
        totalQueryTimeouts = Metrics.counter(createMetricName("TotalQueryTimeouts"));

        sortThenFilterQueriesCompleted = Metrics.counter(createMetricName("SortThenFilterQueriesCompleted"));
        filterThenSortQueriesCompleted = Metrics.counter(createMetricName("FilterThenSortQueriesCompleted"));
    }

    public void record(QueryContext queryContext)
    {
        if (queryContext.queryTimeouts() > 0)
        {
            assert queryContext.queryTimeouts() == 1;

            totalQueryTimeouts.inc();
        }

        perQueryMetrics.record(queryContext);
    }

    public void release()
    {
        super.release();
        perQueryMetrics.release();
    }

    public class PerQueryMetrics extends AbstractMetrics
    {
        public static final String PER_QUERY_METRICS_TYPE = "PerQuery";

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

        public PerQueryMetrics(TableMetadata table)
        {
            super(table.keyspace, table.name, PER_QUERY_METRICS_TYPE);

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

        private void recordStringIndexCacheMetrics(QueryContext events)
        {
            postingsSkips.update(events.triePostingsSkips());
            postingsDecodes.update(events.triePostingsDecodes());
        }

        private void recordNumericIndexCacheMetrics(QueryContext events)
        {
            kdTreePostingsNumPostings.update(events.bkdPostingListsHit());

            kdTreePostingsSkips.update(events.bkdPostingsSkips());
            kdTreePostingsDecodes.update(events.bkdPostingsDecodes());
        }

        private void recordVectorIndexMetrics(QueryContext queryContext)
        {
            annGraphSearchLatency.update(queryContext.annGraphSearchLatency(), TimeUnit.NANOSECONDS);
        }

        public void record(QueryContext queryContext)
        {
            final long totalQueryTimeNs = queryContext.totalQueryTimeNs();
            queryLatency.update(totalQueryTimeNs, TimeUnit.NANOSECONDS);
            final long queryLatencyMicros = TimeUnit.NANOSECONDS.toMicros(totalQueryTimeNs);

            final long ssTablesHit = queryContext.sstablesHit();
            final long segmentsHit = queryContext.segmentsHit();
            final long partitionsRead = queryContext.partitionsRead();
            final long rowsFiltered = queryContext.rowsFiltered();
            final long rowsPreFiltered = queryContext.rowsFiltered();

            sstablesHit.update(ssTablesHit);
            this.segmentsHit.update(segmentsHit);

            partitionReads.update(partitionsRead);
            totalPartitionReads.inc(partitionsRead);

            this.rowsFiltered.update(rowsFiltered);
            totalRowsFiltered.inc(rowsFiltered);

            if (queryContext.filterSortOrder() == QueryContext.FilterSortOrder.SCAN_THEN_FILTER)
                sortThenFilterQueriesCompleted.inc();
            else if (queryContext.filterSortOrder() == QueryContext.FilterSortOrder.SEARCH_THEN_ORDER)
                filterThenSortQueriesCompleted.inc();

            if (Tracing.isTracing())
            {
                if (queryContext.filterSortOrder() == QueryContext.FilterSortOrder.SEARCH_THEN_ORDER)
                {
                    Tracing.trace("Index query accessed memtable indexes, {}, and {}, selected {} before ranking, post-filtered {} in {}, and took {} microseconds.",
                                  pluralize(ssTablesHit, "SSTable index", "es"),
                                  pluralize(segmentsHit, "segment", "s"),
                                  pluralize(rowsPreFiltered, "row", "s"),
                                  pluralize(rowsFiltered, "row", "s"),
                                  pluralize(partitionsRead, "partition", "s"),
                                  queryLatencyMicros);
                }
                else
                {
                    Tracing.trace("Index query accessed memtable indexes, {}, and {}, post-filtered {} in {}, and took {} microseconds.",
                                  pluralize(ssTablesHit, "SSTable index", "es"),
                                  pluralize(segmentsHit, "segment", "s"),
                                  pluralize(rowsFiltered, "row", "s"),
                                  pluralize(partitionsRead, "partition", "s"),
                                  queryLatencyMicros);
                }
            }

            if (queryContext.trieSegmentsHit() > 0)
                recordStringIndexCacheMetrics(queryContext);
            if (queryContext.bkdSegmentsHit() > 0)
                recordNumericIndexCacheMetrics(queryContext);
            // If ann brute forced the whole search, this is 0. We don't measure brute force latency. Maybe we should?
            // At the very least, we collect brute force comparison metrics, which should give a reasonable indicator
            // of work done.
            if (queryContext.annGraphSearchLatency() > 0)
                recordVectorIndexMetrics(queryContext);

            shadowedKeysScannedHistogram.update(queryContext.getShadowedPrimaryKeyCount());

            totalQueriesCompleted.inc();
        }
    }

    private String pluralize(long count, String root, String plural)
    {
        return count == 1 ? String.format("1 %s", root) : String.format("%d %s%s", count, root, plural);
    }
}
