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
package org.apache.cassandra.index.sai.cql;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VectorCompactionConcurrencyTest extends VectorTester
{
    private static final int DIMENSION = 100;
    private static final int ROWS_PER_SSTABLE = 1250;
    private static final int NUM_SSTABLES = 4;
    private static final int TOTAL_ROWS = ROWS_PER_SSTABLE * NUM_SSTABLES;

    private int originalThreads;
    private ThreadPoolMonitor monitor;

    @Parameterized.Parameter(0)
    public int concurrency;

    @Parameterized.Parameters(name = "concurrency={0}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[][]{
        { 0 },  // Synchronous mode - no thread pool
        { 1 },
        { 2 },
        { 4 },
        { FBUtilities.getAvailableProcessors() }
        });
    }

    @Before
    public void setup()
    {
        originalThreads = SegmentBuilder.VECTOR_INDEX_BUILD_THREADS;
        SegmentBuilder.VECTOR_INDEX_BUILD_THREADS = concurrency;

        if (SegmentBuilder.compactionExecutor != null)
        {
            SegmentBuilder.compactionExecutor.shutdown();
        }
        SegmentBuilder.compactionExecutor = SegmentBuilder.createCompactionExecutor();
        SegmentBuilder.submissionQuota = SegmentBuilder.createSubmissionQuota();
        
        monitor = new ThreadPoolMonitor();
    }

    @After
    public void teardown()
    {
        if (monitor != null)
            monitor.stop();
        
        SegmentBuilder.VECTOR_INDEX_BUILD_THREADS = originalThreads;
        
        // Recreate executor and submission quota with original settings
        if (SegmentBuilder.compactionExecutor != null)
        {
            SegmentBuilder.compactionExecutor.shutdown();
        }
        SegmentBuilder.compactionExecutor = SegmentBuilder.createCompactionExecutor();
        SegmentBuilder.submissionQuota = SegmentBuilder.createSubmissionQuota();
    }

    @Test
    public void testCompactionConcurrency() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, " + DIMENSION + ">, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        disableCompaction();

        for (int sstable = 0; sstable < NUM_SSTABLES; sstable++)
        {
            int startPk = sstable * ROWS_PER_SSTABLE;
            int endPk = startPk + ROWS_PER_SSTABLE;

            for (int pk = startPk; pk < endPk; pk++)
            {
                float[] vec = createVector(pk);
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, vector(vec));
            }
            flush();
        }

        monitor.startMonitoring(SegmentBuilder.compactionExecutor);
        compact();
        monitor.stop();

        int maxActive = monitor.getMaxActiveThreads();
        
        if (concurrency == 0)
        {
            assertEquals("Synchronous mode should not use any threads from pool", 0, maxActive);
        }
        else
        {
            assertTrue(String.format("Max active threads (%d) exceeded configured limit (%d)",
                                     maxActive, concurrency),
                       maxActive <= concurrency);

            if (concurrency > 1 && FBUtilities.getAvailableProcessors() >= concurrency)
            {
                // Note: This assertion may occasionally fail on heavily loaded systems
                // where the compaction completes too quickly to observe parallel execution
                assertTrue(String.format("Expected concurrent execution with concurrency=%d but max active threads was %d. " +
                                         "This may indicate compaction completed too quickly or insufficient workload.",
                                         concurrency, maxActive),
                           maxActive > 1);
            }
        }

        assertEquals("Row count mismatch after compaction",
                     TOTAL_ROWS,
                     execute("SELECT * FROM %s").size());

        float[] queryVec = createVector(100);
        var results = execute("SELECT pk FROM %s ORDER BY v ANN OF ? LIMIT 10", vector(queryVec));

        assertTrue("ANN query should return results after compaction", results.size() > 0);
        assertEquals("ANN query should respect LIMIT", 10, results.size());

        for (var row : results)
        {
            int pk = row.getInt("pk");
            assertTrue(String.format("Returned pk %d is outside valid range [0, %d)", pk, TOTAL_ROWS),
                       pk >= 0 && pk < TOTAL_ROWS);
        }
    }

    /**
     * Creates a random vector for the given primary key value.
     * Uses pk as seed for reproducibility.
     */
    private float[] createVector(int pk)
    {
        Random r = new Random(pk);
        float[] vec = new float[DIMENSION];
        for (int i = 0; i < DIMENSION; i++)
        {
            vec[i] = r.nextFloat();
        }
        return vec;
    }

    /**
     * Monitors thread pool activity during compaction.
     * Uses polling with short intervals to track peak concurrency.
     */
    private static class ThreadPoolMonitor
    {
        private final AtomicInteger maxActiveThreads = new AtomicInteger(0);
        private volatile boolean monitoring = false;
        private Thread monitorThread;

        public void startMonitoring(java.util.concurrent.ExecutorService executor)
        {
            // Handle synchronous mode (executor is null when VECTOR_INDEX_BUILD_THREADS = 0)
            if (executor == null || !(executor instanceof java.util.concurrent.ThreadPoolExecutor))
                return;

            monitoring = true;
            java.util.concurrent.ThreadPoolExecutor tpe = (java.util.concurrent.ThreadPoolExecutor) executor;

            monitorThread = new Thread(() -> {
                while (monitoring)
                {
                    int active = tpe.getActiveCount();

                    // Update max if we see higher concurrency
                    int currentMax = maxActiveThreads.get();
                    while (active > currentMax)
                    {
                        if (maxActiveThreads.compareAndSet(currentMax, active))
                            break;
                        currentMax = maxActiveThreads.get();
                    }

                    // Poll frequently to catch peak concurrency
                    FBUtilities.sleepQuietly(5);
                }
            });

            monitorThread.setDaemon(true);
            monitorThread.start();
        }

        public void stop()
        {
            monitoring = false;
            if (monitorThread != null)
            {
                try
                {
                    monitorThread.join(1000);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public int getMaxActiveThreads()
        {
            return maxActiveThreads.get();
        }
    }
}