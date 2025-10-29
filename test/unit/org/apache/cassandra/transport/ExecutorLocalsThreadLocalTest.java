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

package org.apache.cassandra.transport;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.*;

/**
 * Tests to validate that ExecutorLocals (thread-local storage) is properly preserved
 * across async boundaries in Message.execute() and Dispatcher.processRequest/processInit.
 * 
 * This test validates the fix for CNDB-15843 where tracing sessions were not being
 * properly stopped because TraceState was lost when callbacks executed on different threads.
 */
public class ExecutorLocalsThreadLocalTest extends CQLTester
{
    private ExecutorService asyncExecutor;

    @BeforeClass
    public static void setupDD()
    {
        prepareServer();
        requireNetwork();
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setupTest()
    {
        asyncExecutor = Executors.newFixedThreadPool(2);
    }

    @After
    public void cleanupTest()
    {
        if (asyncExecutor != null)
        {
            asyncExecutor.shutdown();
            try
            {
                asyncExecutor.awaitTermination(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
        
        // Clean up any lingering thread-local state
        Tracing.instance.stopSession();
        ClientWarn.instance.resetWarnings();
    }

    /**
     * Test that TraceState is preserved when using ExecutorLocals pattern.
     * This simulates what happens in Message.execute() when maybeExecuteAsync
     * completes on a different thread.
     */
    @Test
    public void testTraceStatePreservedAcrossThreads() throws Exception
    {
        // Start a tracing session on the main thread
        TimeUUID sessionId = Tracing.instance.newSession(ClientState.forInternalCalls(), Tracing.TraceType.QUERY);
        Tracing.instance.begin("test-request", Collections.emptyMap());
        
        assertNotNull("TraceState should be set on main thread", Tracing.instance.get());
        assertEquals("Session ID should match", sessionId, Tracing.instance.get().sessionId);

        // Capture ExecutorLocals before async execution (like Message.execute does)
        ExecutorLocals executorLocals = ExecutorLocals.current();
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TimeUUID> callbackSessionId = new AtomicReference<>();
        AtomicReference<Boolean> callbackHadTraceState = new AtomicReference<>(false);

        // Simulate async execution on a different thread
        asyncExecutor.submit(() -> {
            try
            {
                // Without ExecutorLocals restoration, TraceState would be null here
                assertNull("TraceState should be null on different thread before restoration", 
                          Tracing.instance.get());

                // Restore ExecutorLocals (like the fix does)
                try (org.apache.cassandra.utils.Closeable close = executorLocals.get())
                {
                    // Now TraceState should be available
                    callbackHadTraceState.set(Tracing.instance.get() != null);
                    if (Tracing.instance.get() != null)
                    {
                        callbackSessionId.set(Tracing.instance.get().sessionId);
                    }
                }
            }
            finally
            {
                latch.countDown();
            }
        });

        assertTrue("Callback should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
        assertTrue("TraceState should be available in callback after ExecutorLocals restoration", 
                  callbackHadTraceState.get());
        assertEquals("Session ID should match in callback", sessionId, callbackSessionId.get());

        // Clean up
        Tracing.instance.stopSession();
        assertNull("TraceState should be null after stopSession", Tracing.instance.get());
    }

    /**
     * Test that ClientWarn state is preserved across threads.
     * This validates that all ExecutorLocals (not just TraceState) are preserved.
     */
    @Test
    public void testClientWarnPreservedAcrossThreads() throws Exception
    {
        // Initialize ClientWarn state by calling captureWarnings()
        ClientWarn.instance.captureWarnings();
        
        // Set a warning on the main thread
        String expectedWarning = "Test warning message";
        ClientWarn.instance.warn(expectedWarning);
        
        List<String> warnings = ClientWarn.instance.getWarnings();
        assertNotNull("Warnings should not be null after captureWarnings()", warnings);
        assertEquals("Warning should be set on main thread", 1, warnings.size());
        assertEquals("Warning message should match", expectedWarning, warnings.get(0));

        // Capture ExecutorLocals before async execution
        ExecutorLocals executorLocals = ExecutorLocals.current();
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> callbackWarning = new AtomicReference<>();
        AtomicReference<Boolean> warningsNullBeforeRestore = new AtomicReference<>(false);

        // Simulate async execution on a different thread
        asyncExecutor.submit(() -> {
            try
            {
                // Without ExecutorLocals restoration, warnings would be null/empty
                List<String> asyncWarnings = ClientWarn.instance.getWarnings();
                warningsNullBeforeRestore.set(asyncWarnings == null || asyncWarnings.isEmpty());

                // Restore ExecutorLocals
                try (org.apache.cassandra.utils.Closeable close = executorLocals.get())
                {
                    // Now warnings should be available
                    List<String> restoredWarnings = ClientWarn.instance.getWarnings();
                    if (restoredWarnings != null && !restoredWarnings.isEmpty())
                    {
                        callbackWarning.set(restoredWarnings.get(0));
                    }
                }
            }
            finally
            {
                latch.countDown();
            }
        });

        assertTrue("Callback should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
        assertTrue("Warnings should be null/empty on different thread before restoration", 
                   warningsNullBeforeRestore.get());
        assertEquals("Warning should be preserved in callback", expectedWarning, callbackWarning.get());

        // Clean up
        ClientWarn.instance.resetWarnings();
    }

    /**
     * Test that stopSession() is called correctly when ExecutorLocals is restored.
     * This is the core issue that was fixed in CNDB-15843.
     */
    @Test
    public void testStopSessionCalledWithExecutorLocals() throws Exception
    {
        // Start a tracing session
        TimeUUID sessionId = Tracing.instance.newSession(ClientState.forInternalCalls(), Tracing.TraceType.QUERY);
        Tracing.instance.begin("test-request", Collections.emptyMap());
        
        assertNotNull("TraceState should be set", Tracing.instance.get());

        // Capture ExecutorLocals
        ExecutorLocals executorLocals = ExecutorLocals.current();
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Boolean> stopSessionSucceeded = new AtomicReference<>(false);
        AtomicReference<Boolean> traceStateNullAfterStop = new AtomicReference<>(false);

        // Simulate async execution
        asyncExecutor.submit(() -> {
            try
            {
                // Restore ExecutorLocals and call stopSession (like Message.execute does)
                try (org.apache.cassandra.utils.Closeable close = executorLocals.get())
                {
                    if (Tracing.instance.get() != null)
                    {
                        Tracing.instance.stopSession();
                        stopSessionSucceeded.set(true);
                        // Check if TraceState is null after stopSession in this thread
                        traceStateNullAfterStop.set(Tracing.instance.get() == null);
                    }
                }
            }
            finally
            {
                latch.countDown();
            }
        });

        assertTrue("Callback should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
        assertTrue("stopSession should have been called successfully", stopSessionSucceeded.get());
        assertTrue("TraceState should be null after stopSession in async thread", traceStateNullAfterStop.get());
        
        // Clean up the main thread's TraceState if it still exists
        if (Tracing.instance.get() != null)
        {
            Tracing.instance.stopSession();
        }
    }
}
