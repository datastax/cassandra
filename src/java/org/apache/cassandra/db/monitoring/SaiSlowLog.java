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

package org.apache.cassandra.db.monitoring;

import java.beans.ConstructorProperties;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.tracing.Tracing;

public class SaiSlowLog
{
    private static final Logger logger = LoggerFactory.getLogger(SaiSlowLog.class);

    private final AtomicInteger numSlowestQueries = new AtomicInteger();
    // a structure that keeps the N slowest queries - don't use getter/setter so that it's not exposed over JMX
    private final SaiSlowestQueriesQueue slowestQueries;
    // KATE: make this false by default and decide on yaml property, -D flag, or JMX to enable; hook setEnabled() to change
    private final AtomicBoolean enabled = new AtomicBoolean(true);

    static final Comparator<SlowSaiQuery> SLOW_SAI_QUERY_COMPARATOR = (SlowSaiQuery o1, SlowSaiQuery o2) -> Long.compare(o2.getDuration(), o1.getDuration());

    @VisibleForTesting
    static SaiSlowLog instance = new SaiSlowLog();

    public SaiSlowLog()
    {
        numSlowestQueries.set(DatabaseDescriptor.getSaiSlowLogNumSlowestQueries());
        slowestQueries = new SaiSlowestQueriesQueue(numSlowestQueries.get());
    }

    public boolean isEnabled()
    {
        return enabled.get();
    }

    public synchronized void setEnabled(boolean newValue)
    {
        boolean oldValue = this.enabled.get();

        if (oldValue != newValue)
        {
            this.enabled.set(newValue);
            logger.info("SAI Slow Log is now {}", newValue ? "enabled" : "disabled");
        }
    }

    // KATE below two will be used to set and get the number of slowest queries; make it configurable through property/nodetool?
    public int getNumSlowestQueries()
    {
        return numSlowestQueries.get();
    }

    public void setNumSlowestQueries(int numSlowestQueries)
    {
        int oldValue = this.numSlowestQueries.get();
        if (oldValue != numSlowestQueries)
        {
            if (numSlowestQueries < 1)
            {
                throw new IllegalArgumentException("sai_slow_log_num_slowest_queries must be > 0");
            }
            this.numSlowestQueries.set(numSlowestQueries);
            resizeMinMaxHeap();
            logger.info("sai_slow_log_num_slowest_queries set to {} (was {}).", numSlowestQueries, oldValue);
        }
    }

    private void resizeMinMaxHeap()
    {
        slowestQueries.resize(numSlowestQueries.get());
    }

    public List<SlowSaiQuery> retrieveRecentSlowestCqlQueries()
    {
        List<SlowSaiQuery> queries = slowestQueries.getAndReset();
        // there is no ordering guarantee and we want the slowest queries first
        queries.sort(SLOW_SAI_QUERY_COMPARATOR);
        return queries;
    }

    /**
     * Check if we should record a given operation in the SAI slow log. If so, extract
     * the necessary detail from the query context and log it.
     * @param queryContext operation to log
     */
    public static void maybeRecord(QueryContext queryContext)
    {
        if (!instance.isEnabled())
            return;

        // KATE: Remove below line to avoid logging every query; it is used just during development/testing
        logger.debug("Recording slow SAI query");
        UUID tracingSessionId = Tracing.isTracing() ? Tracing.instance.getSessionId() : null;

        instance.slowestQueries.addAsync(new SaiSlowLog.SlowSaiQuery(queryContext.totalQueryTimeNs(),
                                                                 queryContext.optimizedPlan(),
                                                                 null == tracingSessionId ? "" : tracingSessionId.toString()));
    }

    public static final class SlowSaiQuery implements Comparable<SlowSaiQuery>
    {
        private final long duration;
        private final String optimizedPlan;
        private final String tracingSessionId;

        @ConstructorProperties({ "duration", "optimizedPlan", "tracingSessionId" })
        public SlowSaiQuery(long duration, String optimizedPlan, String tracingSessionId)
        {
            this.duration = duration;
            this.optimizedPlan = optimizedPlan;
            this.tracingSessionId = tracingSessionId;
        }

        public long getDuration()
        {
            return duration;
        }

        // KATE: probably will be needed for testing?
        public String getOptimizedPlan()
        {
            return optimizedPlan;
        }

        // KATE: maybe needed for testing?
        public String getTracingSessionId()
        {
            return tracingSessionId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SlowSaiQuery that = (SlowSaiQuery) o;
            return duration == that.duration &&
                   Objects.equal(optimizedPlan, that.optimizedPlan) &&
                   Objects.equal(tracingSessionId, that.tracingSessionId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(duration, optimizedPlan, tracingSessionId);
        }

        @Override
        public int compareTo(SlowSaiQuery other)
        {
            return Long.compare(this.duration, other.duration);
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder("SlowSaiQuery{\n");
            sb.append("duration=").append(duration).append("ns\n");
            sb.append("\noptimizedPlan:\n").append(optimizedPlan);
            sb.append("\ntracingSessionId=").append(tracingSessionId);
            sb.append('}');
            return sb.toString();
        }
    }
}
