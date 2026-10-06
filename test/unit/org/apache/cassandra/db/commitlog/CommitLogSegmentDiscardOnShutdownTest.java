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

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertFalse;

/**
 * Regression test for CNDB-18499: FSWriteError (ClosedByInterruptException) thrown on the
 * COMMIT-LOG-ALLOCATOR thread when discarding a pre-allocated empty segment during shutdown.
 *
 * Root cause: NIO FileChannel.force() and FileChannel.write() throw ClosedByInterruptException
 * when the calling thread has the interrupt flag set. During shutdown, InfiniteLoopExecutor calls
 * thread.interrupt() on the COMMIT-LOG-ALLOCATOR thread. If the thread was in the middle of
 * discardAvailableSegment() → discard() → close() → sync() → flush() at that moment (a narrow
 * race window), the interrupt causes the IO to fail with FSWriteError.
 *
 * Fix: wrap all calls to discardAvailableSegment() inside AllocatorRunnable.run() in
 * synchronized(this) and clear the interrupt flag with Thread.interrupted() first. The
 * InfiniteLoopExecutor uses SYNCHRONIZED interrupts, meaning thread.interrupt() is delivered
 * while holding the same AllocatorRunnable monitor. Holding the lock during the IO prevents
 * any new interrupt from arriving mid-IO. This mirrors the same protection used for
 * createSegment() on the NORMAL path.
 *
 * NOTE on testability: The bug window is narrow (a few microseconds between interrupt delivery
 * and channel.force()) and only manifests with real fsyncs (SKIP_SYNC=true in unit tests). The
 * test below uses Byteman to inject the failure at close() entry when the interrupt flag is set,
 * providing a best-effort regression check. The definitive validation is in CNDB integration tests
 * which run with real I/O and observed the original failure consistently across all test classes.
 */
@RunWith(BMUnitRunner.class)
public class CommitLogSegmentDiscardOnShutdownTest
{
    @BeforeClass
    public static void beforeClass() throws IOException
    {
        File commitLogDir = new File(Files.createTempDirectory("commitLogSegmentDiscardTest"));
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.commitlog_directory = commitLogDir.toString();
            config.commitlog_sync = Config.CommitLogSync.periodic;
            config.commitlog_sync_period = new DurationSpec.IntMillisecondsBound("10000ms");
            return config;
        });
        DatabaseDescriptor.initializeCommitLogDiskAccessMode();
    }

    @Before
    public void beforeTest()
    {
        CommitLog.instance.stopUnsafe(true);
        CommitLog.instance.start();
    }

    @After
    public void afterTest()
    {
        CommitLog.instance.stopUnsafe(true);
    }

    /**
     * Verifies that no FSWriteError escapes when the COMMIT-LOG-ALLOCATOR thread is interrupted
     * while discarding the pre-allocated empty available segment during shutdown.
     *
     * Byteman intercepts CommitLogSegment.close() on the allocator thread and throws
     * FSWriteError(ClosedByInterruptException) if the interrupt flag is set — exactly what
     * NIO channel.force() does in production. The fix ensures the interrupt flag is cleared
     * inside synchronized(AllocatorRunnable) before the IO chain is entered, so this rule
     * should not fire.
     *
     * Because unit tests run with SKIP_SYNC=true and the race window is narrow, this test may
     * not always reproduce the failure without the fix. For deterministic validation see the
     * CNDB integration test suite (CNDB-18499).
     */
    @Test
    @BMRule(name = "Simulate ClosedByInterruptException in close() when allocator thread is interrupted",
            targetClass = "CommitLogSegment",
            targetMethod = "close",
            targetLocation = "AT ENTRY",
            condition = "Thread.currentThread().getName().equals(\"COMMIT-LOG-ALLOCATOR\") && Thread.currentThread().isInterrupted()",
            action = "org.apache.cassandra.db.commitlog.CommitLogSegmentDiscardOnShutdownTest.notifyAndThrow()")
    public void testNoFSWriteErrorWhenAllocatorThreadInterruptedDuringShutdown() throws Exception
    {
        CommitLog.instance.getSegmentManager().awaitManagementTasksCompletion();

        fsWriteErrorObserved.set(false);

        CommitLog.instance.getSegmentManager().shutdown();
        CommitLog.instance.getSegmentManager().awaitTermination(30, TimeUnit.SECONDS);

        assertFalse("FSWriteError was thrown on the COMMIT-LOG-ALLOCATOR thread: the interrupt flag " +
                    "was set when CommitLogSegment.close() was entered. " +
                    "Fix: wrap discardAvailableSegment() in synchronized(AllocatorRunnable) and call " +
                    "Thread.interrupted() before the IO chain so interrupts cannot arrive mid-IO.",
                    fsWriteErrorObserved.get());
    }

    public static final AtomicBoolean fsWriteErrorObserved = new AtomicBoolean(false);

    public static void notifyAndThrow()
    {
        fsWriteErrorObserved.set(true);
        throw new FSWriteError(new java.nio.channels.ClosedByInterruptException(), "test-segment");
    }
}
