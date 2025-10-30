/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.plan;

import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.index.sai.QueryContext;

/**
 * {@link Monitorable.ExecutionInfo} implementation for SAI queries.
 * It holds and prints the metrics from the {@link QueryContext} of the monitorized queries, and its {@link Plan}.
 */
public class QueryMonitorableExecutionInfo implements Monitorable.ExecutionInfo
{
    private static final String INDENT = "  ";
    private static final String DOUBLE_INDENT = INDENT + INDENT;

    private final QueryContext.Snapshot metrics;
    private final String plan;

    /**
     * Builds a new execution info object for a query.
     *
     * @param context the query context
     * @param plan the query plan
     */
    public QueryMonitorableExecutionInfo(QueryContext context, Plan plan)
    {
        this.metrics = context.snapshot();
        this.plan = toLogString(plan);
    }

    @Override
    public String toLogString(boolean unique)
    {
        StringBuilder sb = new StringBuilder("\n");
        String sectionNamePrefix = INDENT + (unique ? "SAI slow query " : "SAI slowest query ");

        // append the index context metrics
        sb.append(sectionNamePrefix).append("metrics:\n");
        appendMetric(sb, "sstablesHit", metrics.sstablesHit);
        appendMetric(sb, "segmentsHit", metrics.segmentsHit);
        appendMetric(sb, "partitionsRead", metrics.partitionsRead);
        appendMetric(sb, "rowsFiltered", metrics.rowsFiltered);
        appendMetric(sb, "rowsPreFiltered", metrics.rowsPreFiltered);
        appendMetric(sb, "trieSegmentsHit", metrics.trieSegmentsHit);
        appendMetric(sb, "bkdPostingListsHit", metrics.bkdPostingListsHit);
        appendMetric(sb, "bkdSegmentsHit", metrics.bkdSegmentsHit);
        appendMetric(sb, "bkdPostingsSkips", metrics.bkdPostingsSkips);
        appendMetric(sb, "bkdPostingsDecodes", metrics.bkdPostingsDecodes);
        appendMetric(sb, "triePostingsSkips", metrics.triePostingsSkips);
        appendMetric(sb, "triePostingsDecodes", metrics.triePostingsDecodes);
        appendMetric(sb, "annGraphSearchLatencyNanos", metrics.annGraphSearchLatency);
        appendMetric(sb, "shadowedPrimaryKeyCount", metrics.shadowedPrimaryKeyCount);

        // append the plan
        sb.append(sectionNamePrefix).append("plan:\n").append(plan);

        return sb.toString();
    }

    private static String toLogString(Plan plan)
    {
        String s = plan.toStringRecursive(DOUBLE_INDENT);
        return s.endsWith("\n") ? s.substring(0, s.length() - 1) : s;
    }

    private static void appendMetric(StringBuilder sb, String name, Object value)
    {
        sb.append(DOUBLE_INDENT).append(name).append(": ").append(value).append('\n');
    }
}
