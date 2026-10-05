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
import java.util.concurrent.atomic.AtomicReference;

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for the FSWriteError thrown when the COMMIT-LOG-ALLOCATOR thread is interrupted
 * by InfiniteLoopExecutor shutdown while discarding a pre-allocated empty available segment.
 *
 * The bug: discardAvailableSegment() → discard() → close() → sync() → flush() → channel.force()
 * throws ClosedByInterruptException (wrapped as FSWriteError) when the thread interrupt flag is set,
 * because NIO FileChannel.force() closes the channel and throws on interrupt.
 *
 * The fix: clear the interrupt flag in discardAvailableSegment() before calling discard(), then
 * restore it afterwards, mirroring the same protection used in the NORMAL segment creation path.
 *
 * Because unit tests run with SKIP_SYNC=true (SyncUtil skips real fsyncs), ClosedByInterruptException
 * from channel.force() cannot be reproduced directly. Instead, the two tests here cover the fix from
 * both sides:
 * 1. Simulate the FSWriteError directly inside discard() via Byteman and verify it does not escape.
 * 2. Verify the thread interrupt flag is cleared during discard() and restored after, so the
 *    fix works correctly without side-effects.
 *
 * Both tests invoke discardAvailableSegment() directly via segmentManager.shutdown() on a thread
 * that has its interrupt flag set, matching the COMMIT-LOG-ALLOCATOR state at Cassandra shutdown time.
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
     * Simulates the exact FSWriteError that manifests in CNDB integration tests during container shutdown.
     *
     * The Byteman rule intercepts CommitLogSegment.discard() and throws an FSWriteError wrapping a
     * ClosedByInterruptException — exactly what channel.force() produces when the thread is interrupted.
     *
     * Without the fix, the interrupt flag is set when discard() is entered, the Byteman rule fires,
     * and the FSWriteError escapes. With the fix, the interrupt flag is cleared in
     * discardAvailableSegment() before discard() is called, so the condition is false and no
     * exception is thrown.
     */
    @Test
    @BMRule(name = "Simulate ClosedByInterruptException in discard when thread is interrupted",
            targetClass = "CommitLogSegment",
            targetMethod = "discard",
            targetLocation = "AT ENTRY",
            condition = "Thread.currentThread().isInterrupted()",
            action = "org.apache.cassandra.db.commitlog.CommitLogSegmentDiscardOnShutdownTest.throwSimulatedFSWriteError($0.getPath())")
    public void testFSWriteErrorDoesNotEscapeWhenThreadInterruptedDuringShutdown() throws Exception
    {
        CommitLog.instance.getSegmentManager().awaitManagementTasksCompletion();

        // Invoke shutdown directly on a thread with the interrupt flag set, reproducing the
        // COMMIT-LOG-ALLOCATOR state when InfiniteLoopExecutor calls thread.interrupt().
        // We call segmentManager.shutdown() + awaitTermination() directly rather than
        // shutdownBlocking() to avoid InterruptedException from unrelated synchronized blocks.
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Thread t = new Thread(() -> {
            Thread.currentThread().interrupt(); // set interrupt flag as InfiniteLoopExecutor does
            try
            {
                // shutdown() calls discardAvailableSegment() on this thread.
                // With the fix: interrupt cleared before discard() → Byteman condition false → no throw.
                // Without the fix: interrupt still set → Byteman condition true → FSWriteError thrown.
                CommitLog.instance.getSegmentManager().shutdown();
                CommitLog.instance.getSegmentManager().awaitTermination(30, TimeUnit.SECONDS);
            }
            catch (Throwable e)
            {
                caught.set(e);
            }
            finally
            {
                Thread.interrupted(); // clean up for thread reuse
            }
        });
        t.start();
        t.join(30_000);

        assertNull("FSWriteError (simulating ClosedByInterruptException from channel.force()) " +
                   "escaped discardAvailableSegment() — interrupt flag was not cleared before discard(). " +
                   "Exception: " + caught.get(), caught.get());
    }

    /**
     * Verifies that the interrupt flag is:
     * - cleared during discard() so IO (flush/fsync) is not affected by the interrupt
     * - restored after discardAvailableSegment() returns so the caller still observes the interrupt
     *
     * This confirms the fix correctly preserves interrupt semantics without losing the signal.
     */
    @Test
    @BMRule(name = "Record interrupt state inside discard",
            targetClass = "CommitLogSegment",
            targetMethod = "discard",
            targetLocation = "AT ENTRY",
            action = "org.apache.cassandra.db.commitlog.CommitLogSegmentDiscardOnShutdownTest.recordInterruptDuringDiscard(Thread.currentThread())")
    public void testInterruptFlagClearedDuringDiscardAndRestoredAfter() throws Exception
    {
        CommitLog.instance.getSegmentManager().awaitManagementTasksCompletion();

        AtomicBoolean interruptInsideDiscard = new AtomicBoolean(true); // stays true if discard() not called or flag still set
        interruptInsideDiscardRef = interruptInsideDiscard;

        AtomicBoolean interruptAfterShutdown = new AtomicBoolean();

        Thread t = new Thread(() -> {
            Thread.currentThread().interrupt(); // set flag as InfiniteLoopExecutor would
            try
            {
                CommitLog.instance.getSegmentManager().shutdown();
                CommitLog.instance.getSegmentManager().awaitTermination(30, TimeUnit.SECONDS);
            }
            catch (Throwable ignored) {}
            finally
            {
                interruptAfterShutdown.set(Thread.currentThread().isInterrupted());
                Thread.interrupted(); // clean up
            }
        });
        t.start();
        t.join(30_000);

        assertFalse("Thread interrupt flag was still set inside discard() — fix should clear it before calling discard()",
                    interruptInsideDiscard.get());
        assertTrue("Thread interrupt flag was not restored after discardAvailableSegment() — fix should restore it in finally block",
                   interruptAfterShutdown.get());
    }

    // Written to by the Byteman rule in testInterruptFlagClearedDuringDiscardAndRestoredAfter.
    // Volatile so the Byteman-injected write from the test thread is visible in the main thread.
    public static volatile AtomicBoolean interruptInsideDiscardRef;

    public static void recordInterruptDuringDiscard(Thread t)
    {
        AtomicBoolean ref = interruptInsideDiscardRef;
        if (ref != null)
            ref.set(t.isInterrupted());
    }

    /**
     * Called by the Byteman rule in testFSWriteErrorDoesNotEscapeWhenThreadInterruptedDuringShutdown.
     * Throws the FSWriteError that channel.force() produces when the thread is interrupted.
     * Using a helper method avoids Byteman's parser limitation with `throw factoryMethod()` expressions.
     */
    public static void throwSimulatedFSWriteError(String path)
    {
        throw new FSWriteError(new java.nio.channels.ClosedByInterruptException(), path);
    }
}
