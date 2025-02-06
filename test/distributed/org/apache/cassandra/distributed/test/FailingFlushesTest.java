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

package org.apache.cassandra.distributed.test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.memtable.Flushing;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.Byteman;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class FailingFlushesTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(FailingFlushesTest.class);

    private static final String INJECTION_SCRIPT = "RULE fail flush\n" +
                                                   "CLASS %s\n" +
                                                   "METHOD %s\n" +
                                                   "AT ENTRY\n" +
                                                   "IF org.apache.cassandra.distributed.test.FailingFlushesTest.shouldFlushFail(%s)\n" +
                                                   "DO\n" +
                                                   "   throw new java.lang.RuntimeException(\"Injected flush failure\")\n" +
                                                   "ENDRULE\n";

    public static final AtomicInteger throwingOperationCounter = new AtomicInteger();
    public static int failedOperationIdx;

    @Parameterized.Parameters(name = "{0}::{1}")
    public static Object[][] data()
    {
        return new Object[][] {
        { Flushing.class.getName(), "flushRunnable", "$1.keyspace.getName()", 25 },
        };
    }

    @Rule
    public TestName testName = new TestName();

    private final String bytemanScript;
    private final String tableName;

    public FailingFlushesTest(String clazz, String method, String keyspaceName, Integer failedOperationIdx)
    {
        this.bytemanScript = String.format(INJECTION_SCRIPT, clazz, method, keyspaceName);
        this.tableName = clazz.replace('.', '_') + "_" + method;
        FailingFlushesTest.failedOperationIdx = failedOperationIdx;
    }

    public static boolean shouldFlushFail(String keyspace)
    {
        logger.info("Checking if the flush should fail for keyspace {}; current op counter: {}/{}", keyspace, throwingOperationCounter.get(), failedOperationIdx);
        return (keyspace.equals(KEYSPACE)) && throwingOperationCounter.incrementAndGet() == failedOperationIdx;
    }

    @Test
    public void testFailingFlushes() throws Throwable
    {
        int NUM_ROWS = 100;

        int flushFailures = 0;
        logger.info("Running test with table name {}, testname {}", tableName, testName.getMethodName());

        try (Cluster cluster = init(Cluster.build(1)
                                           .withInstanceInitializer(this::installBytemanScript)
                                           .start(),
                                    1))
        {
            cluster.setUncaughtExceptionsFilter((t) -> t.getMessage() != null && t.getMessage().contains("Injected flush failure"));

            // set logging level to TRACE on the node
            cluster.get(1).nodetoolResult("setlogginglevel", "org.apache.cassandra", "TRACE").asserts().success();

            cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v int) ", KEYSPACE, tableName));
            int failureIdx = failedOperationIdx;
            cluster.get(1).runOnInstance(() -> FailingFlushesTest.failedOperationIdx = failureIdx);
            // write rows
            for (int i = 0; i < NUM_ROWS; i++)
            {
                logger.info("Writing row {}", i);
                logger.info("Commit log size: {}", cluster.get(1).callOnInstance(() -> CommitLog.instance.getActiveOnDiskSize()));
                cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", KEYSPACE, tableName), ConsistencyLevel.ALL, i, i);
                try
                {
                    logger.info("Flushing {}", i);
                    cluster.get(1).flush(KEYSPACE);
                }
                catch (Exception e)
                {
                    flushFailures++;
                    if (flushFailures == 1)
                        logger.info("flush failed; this is expected");
                    else
                    {
                        logger.error("Unexpexted flush failure");
                        throw new RuntimeException("Unexpected flush failure", e);
                    }
                }
            }

            // check if the commit log didn't back up
            List<String> activeSegments = cluster.get(1).callOnInstance(() -> CommitLog.instance.getActiveSegmentNames());
            logger.info("active segments: {}", activeSegments);
            assertEquals("expected one active segment, got " + activeSegments.size() + " instead;", 1, activeSegments.size());

            // restart the node
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();

            // check all rows are readable
            for (int i = 0; i < NUM_ROWS; i++)
            {
                logger.info("Reading row {}", i);
                Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT * FROM %s.%s WHERE k = ?", KEYSPACE, tableName), ConsistencyLevel.ALL, i);
                logger.info("row: {}", rows);
                assertRows(rows, row(i, i));
            }
        }
    }

    private void installBytemanScript(ClassLoader cl, int nodeNumber)
    {
        System.setProperty("cassandra.trie.memtable.shard.count", "1");
        Byteman.createFromText(bytemanScript).install(cl);
    }
}
