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

import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.index.sai.QueryContext;

import javax.annotation.Nullable;

/**
 * {@link Monitorable.ExecutionInfo} implementation for SAI queries.
 * It holds and prints the metrics from the {@link QueryContext} of the monitorized queries, and its {@link Plan}.
 */
public class QueryMonitorableExecutionInfo implements Monitorable.ExecutionInfo
{
    public static final String UNKNOWN_PLAN = "not yet created";

    private final QueryContext.Snapshot metrics;
    private final String plan;

    /**
     * Builds a new execution info object for a query.
     *
     * @param metrics a snapshot of the query context metrics
     * @param plan the query plan
     */
    private QueryMonitorableExecutionInfo(QueryContext.Snapshot metrics, String plan)
    {
        this.metrics = metrics;
        this.plan = plan;
    }

    /**
     * Returns a supplier of {@link Monitorable.ExecutionInfo} for a query, to be used when logging slow queries.
     *
     * @param context the query context
     * @param plan the query plan, which might be null if monitoring happens before it has been created
     * @return a supplier of {@link Monitorable.ExecutionInfo} for a query
     */
    public static Monitorable.ExecutionInfo create(QueryContext context, @Nullable Plan plan)
    {
        String planAsString = toLogString(plan);
        return new QueryMonitorableExecutionInfo(context.snapshot(), planAsString);
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
        appendMetric(sb, "keysFetched", metrics.keysFetched);
        appendMetric(sb, "partitionsFetched", metrics.partitionsFetched);
        appendMetric(sb, "partitionsReturned", metrics.partitionsReturned);
        appendMetric(sb, "partitionTombstonesFetched", metrics.partitionTombstonesFetched);
        appendMetric(sb, "rowsFetched", metrics.rowsFetched);
        appendMetric(sb, "rowsReturned", metrics.rowsReturned);
        appendMetric(sb, "rowTombstonesFetched", metrics.rowTombstonesFetched);
        appendMetric(sb, "cellsFetched", metrics.cellsFetched);
        appendMetric(sb, "cellsReturned", metrics.cellsReturned);
        appendMetric(sb, "trieSegmentsHit", metrics.trieSegmentsHit);
        appendMetric(sb, "triePostingsSkips", metrics.triePostingsSkips);
        appendMetric(sb, "triePostingsDecodes", metrics.triePostingsDecodes);
        appendMetric(sb, "bkdSegmentsHit", metrics.bkdSegmentsHit);
        appendMetric(sb, "bkdPostingListsHit", metrics.bkdPostingListsHit);
        appendMetric(sb, "bkdPostingsSkips", metrics.bkdPostingsSkips);
        appendMetric(sb, "bkdPostingsDecodes", metrics.bkdPostingsDecodes);
        appendMetric(sb, "annGraphSearchLatencyNanos", metrics.annGraphSearchLatency);

        // append the plan
        sb.append(sectionNamePrefix).append("plan:\n").append(plan);

        return sb.toString();
    }

    private static String toLogString(@Nullable Plan plan)
    {
        if (plan == null)
            return DOUBLE_INDENT + UNKNOWN_PLAN;

        String s = plan.toStringRecursive(Redaction.REDACT, DOUBLE_INDENT);
        return s.endsWith("\n") ? s.substring(0, s.length() - 1) : s;
    }

    private static void appendMetric(StringBuilder sb, String name, Object value)
    {
        sb.append(DOUBLE_INDENT).append(name).append(": ").append(value).append('\n');
    }
}
