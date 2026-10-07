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

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.util.work.ProgressTracker;
import io.github.jbellis.jvector.util.work.WorkStage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.metrics.MicrometerMetrics;

/**
 * Micrometer instrumentation for the Cassandra/jvector vector-compaction pipeline.
 * Each phase is timed with a {@link Timer} tagged with owner, phase, keyspace, table, and index.
 */
public final class VectorCompactionMetrics extends MicrometerMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(VectorCompactionMetrics.class);

    public static final String METRIC_NAME = "sai_vector_compaction_phase";

    public static final VectorCompactionMetrics INSTANCE = new VectorCompactionMetrics();

    /** Cassandra-owned phases surrounding the embedded jvector compactor. */
    public enum Phase implements WorkStage
    {
        SOURCE_SCAN,
        POSTINGS_MAP_SETUP,
        ROW_INGEST,
        INGEST_DRAIN,
        TOTAL_MERGE,
        ORDINAL_REMAP,
        JVECTOR_COMPACT,
        LOAD_RETRAINED_PQ,
        FOOTER_CRC,
        POSTINGS_PQ_WRITE,
        THROTTLE_WAIT,
        CLEANUP
    }

    private VectorCompactionMetrics()
    {
    }

    VectorCompactionMetrics(MeterRegistry registry, Tags tags)
    {
        register(registry, tags);
    }

    public ProgressTracker.PhaseScope start(Phase phase, IndexContext context)
    {
        return start("cassandra", phase, context);
    }

    public ProgressTracker.PhaseScope start(String owner, WorkStage phase, IndexContext context)
    {
        try
        {
            Tags phaseTags = Tags.of("owner", owner,
                                     "phase", phase.name().toLowerCase(Locale.ROOT),
                                     "keyspace", tag(context == null ? null : context.getKeyspace()),
                                     "table", tag(context == null ? null : context.getTable()),
                                     "index", tag(context == null ? null : context.getIndexName()));
            Timer.Sample sample = Timer.start();
            Timer timer = timer(METRIC_NAME, false, phaseTags);
            return new ProgressTracker.PhaseScope()
            {
                @Override
                public void onProgress(long completed, long total)
                {
                }

                @Override
                public void close()
                {
                    try
                    {
                        sample.stop(timer);
                    }
                    catch (Throwable t)
                    {
                        logger.warn("Unable to stop vector compaction phase timer {}:{}", owner, phase.name(), t);
                    }
                }
            };
        }
        catch (Throwable t)
        {
            // Metrics are observational and must never fail a compaction.
            logger.warn("Unable to start vector compaction phase timer {}:{}", owner, phase.name(), t);
            return ProgressTracker.PhaseScope.NOOP;
        }
    }

    private static String tag(String value)
    {
        return value == null || value.isEmpty() ? "unknown" : value;
    }
}
