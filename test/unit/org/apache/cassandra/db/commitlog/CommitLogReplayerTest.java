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

package org.apache.cassandra.db.commitlog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(BMUnitRunner.class)
public class CommitLogReplayerTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    @Test
    @BMRules(rules = { @BMRule(name = "Fail applying mutation",
            targetClass = "org.apache.cassandra.concurrent.Stage",
            targetMethod = "submit",
            action = "return CompletableFuture.failedFuture(new RuntimeException(\"mutation failed\"));") } )
    public void testTrackingSegmentsWhenMutationFails()
    {
        CommitLogReplayer.MutationInitiator mutationInitiator = new CommitLogReplayer.MutationInitiator();
        CommitLogReplayer replayer = new CommitLogReplayer(CommitLog.instance, CommitLogPosition.NONE, null, CommitLogReplayer.ReplayFilter.create());
        CommitLogDescriptor descriptor = mock(CommitLogDescriptor.class);
        String failedSegment = "failedSegment";
        when(descriptor.fileName()).thenReturn(failedSegment);
        CompletableFuture<Integer> mutationFuture = mutationInitiator.initiateMutation(mock(Mutation.class), descriptor, 0, 0, replayer);
        Assert.assertThrows(ExecutionException.class, () -> mutationFuture.get());
        Assert.assertTrue(!replayer.getSegmentWithInvalidOrFailedMutations().isEmpty());
        Assert.assertTrue(replayer.getSegmentWithInvalidOrFailedMutations().contains(failedSegment));
    }

    /**
     * Test that when there are few skipped SSTables (<= 100), all are logged in debug mode.
     * This tests the fix for CNDB-15157.
     */
    @Test
    public void testPersistedIntervalsLogsAllSSTablesWhenFew()
    {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CommitLogReplayer.class);
        InMemoryAppender appender = new InMemoryAppender();
        logger.addAppender(appender);
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);

        try
        {
            // Create 50 mock SSTables with different host IDs (so they'll be skipped)
            List<SSTableReader> sstables = new ArrayList<>();
            UUID differentHostId = UUID.randomUUID();
            for (int i = 0; i < 50; i++)
            {
                SSTableReader reader = mock(SSTableReader.class);
                StatsMetadata metadata = createMinimalStatsMetadata(differentHostId);
                when(reader.getSSTableMetadata()).thenReturn(metadata);
                when(reader.getFilename()).thenReturn("sstable-" + i + ".db");
                sstables.add(reader);
            }

            UUID localhostId = UUID.randomUUID();
            CommitLogReplayer.persistedIntervals(sstables, null, localhostId);

            // Verify that debug log contains all sstables (not truncated)
            List<ILoggingEvent> debugEvents = appender.getEventsForLevel(Level.DEBUG);
            boolean foundFullList = false;
            for (ILoggingEvent event : debugEvents)
            {
                String message = event.getFormattedMessage();
                if (message.contains("Ignored commitLogIntervals from the following sstables:"))
                {
                    foundFullList = true;
                    // Should NOT contain the "showing first" message when count is <= 100
                    Assert.assertFalse("Should not limit logging when <= 100 SSTables",
                                     message.contains("showing first"));
                    break;
                }
            }
            assertTrue("Should have logged the full sstables list", foundFullList);
        }
        finally
        {
            logger.detachAppender(appender);
            logger.setLevel(originalLevel);
        }
    }

    /**
     * Test that when there are many skipped SSTables (> 100), only the first 100 are logged.
     * This tests the fix for CNDB-15157.
     */
    @Test
    public void testPersistedIntervalsLimitsLoggingWhenMany()
    {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CommitLogReplayer.class);
        InMemoryAppender appender = new InMemoryAppender();
        logger.addAppender(appender);
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.TRACE); // Need TRACE level to capture the full list message

        try
        {
            // Create 150 mock SSTables with different host IDs (so they'll be skipped)
            List<SSTableReader> sstables = new ArrayList<>();
            UUID differentHostId = UUID.randomUUID();
            for (int i = 0; i < 150; i++)
            {
                SSTableReader reader = mock(SSTableReader.class);
                StatsMetadata metadata = createMinimalStatsMetadata(differentHostId);
                when(reader.getSSTableMetadata()).thenReturn(metadata);
                when(reader.getFilename()).thenReturn("sstable-" + i + ".db");
                sstables.add(reader);
            }

            UUID localhostId = UUID.randomUUID();
            CommitLogReplayer.persistedIntervals(sstables, null, localhostId);

            // Verify that debug log is limited
            List<ILoggingEvent> debugEvents = appender.getEventsForLevel(Level.DEBUG);
            boolean foundLimitedList = false;
            boolean foundTraceMessage = false;
            for (ILoggingEvent event : debugEvents)
            {
                String message = event.getFormattedMessage();
                if (message.contains("Ignored commitLogIntervals from 150 sstables (showing first 100)"))
                {
                    foundLimitedList = true;
                }
                if (message.contains("Use TRACE level to see all 150 skipped sstables"))
                {
                    foundTraceMessage = true;
                }
            }
            assertTrue("Should have logged limited sstables list", foundLimitedList);
            assertTrue("Should have logged message about TRACE level", foundTraceMessage);

            // Verify trace log has full list
            List<ILoggingEvent> traceEvents = appender.getEventsForLevel(Level.TRACE);
            boolean foundFullListInTrace = false;
            for (ILoggingEvent event : traceEvents)
            {
                String message = event.getFormattedMessage();
                if (message.contains("Full list of ignored sstables:"))
                {
                    foundFullListInTrace = true;
                    break;
                }
            }
            assertTrue("Should have logged full list at TRACE level", foundFullListInTrace);
        }
        finally
        {
            logger.detachAppender(appender);
            logger.setLevel(originalLevel);
        }
    }

    /**
     * Creates a minimal StatsMetadata with only the fields needed for testing persistedIntervals.
     */
    private static StatsMetadata createMinimalStatsMetadata(UUID originatingHostId)
    {
        return new StatsMetadata(new EstimatedHistogram(150, true),
                                new EstimatedHistogram(150, true),
                                IntervalSet.empty(),
                                0L, // minTimestamp
                                0L, // maxTimestamp
                                Integer.MAX_VALUE, // minLocalDeletionTime
                                Integer.MAX_VALUE, // maxLocalDeletionTime
                                Integer.MAX_VALUE, // minTTL
                                Integer.MAX_VALUE, // maxTTL
                                0.0, // compressionRatio
                                TombstoneHistogram.createDefault(), // estimatedTombstoneDropTime
                                0, // sstableLevel
                                null, // clusteringTypes
                                null, // coveredClustering
                                false, // hasLegacyCounterShards
                                false, // hasPartitionLevelDeletions
                                0L, // repairedAt
                                0L, // totalColumnsSet
                                0L, // totalRows
                                0.0, // tokenSpaceCoverage
                                originatingHostId,
                                null, // pendingRepair
                                false, // isTransient
                                Collections.emptyMap(), // maxColumnValueLengths
                                null); // zeroCopyMetadata
    }

    private static class InMemoryAppender extends AppenderBase<ILoggingEvent>
    {
        private final List<ILoggingEvent> events = new ArrayList<>();

        private InMemoryAppender()
        {
            start();
        }

        @Override
        protected synchronized void append(ILoggingEvent event)
        {
            events.add(event);
        }

        public synchronized List<ILoggingEvent> getEventsForLevel(Level level)
        {
            List<ILoggingEvent> result = new ArrayList<>();
            for (ILoggingEvent event : events)
            {
                if (event.getLevel() == level)
                {
                    result.add(event);
                }
            }
            return result;
        }
    }
}
