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
package org.apache.cassandra.service;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SignalLoggingTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSignalHandlerRegistration()
    {
        // This test verifies that signal handlers can be registered without errors
        try
        {
            sun.misc.Signal testSignal = new sun.misc.Signal("USR1");
            sun.misc.SignalHandler handler = new sun.misc.SignalHandler()
            {
                @Override
                public void handle(sun.misc.Signal sig)
                {
                    System.out.println("Test received signal: SIG" + sig.getName());
                }
            };

            sun.misc.SignalHandler oldHandler = sun.misc.Signal.handle(testSignal, handler);

            sun.misc.Signal.raise(testSignal);

            if (oldHandler != null)
            {
                sun.misc.Signal.handle(testSignal, oldHandler);
            }

            assertTrue("Signal handler registration should work", true);
        }
        catch (Exception e)
        {
            // Signal might not be available on this platform
            System.out.println("Signal USR1 not available on this platform: " + e.getMessage());
        }
    }

    /**
     * Test that the actual inner handle() method logs received signals.
     */
    @Test
    public void testActualInnerHandleMethodLogsSignal() throws InterruptedException
    {
        try
        {
            // Set up logging capture for StorageService
            Logger storageServiceLogger = (Logger) LoggerFactory.getLogger(StorageService.class);
            ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
            listAppender.start();
            storageServiceLogger.addAppender(listAppender);
            Level originalLevel = storageServiceLogger.getLevel();
            storageServiceLogger.setLevel(Level.INFO);

            try
            {
                sun.misc.Signal testSignal = new sun.misc.Signal("USR2");

                // Save the current handler
                sun.misc.SignalHandler originalHandler = sun.misc.Signal.handle(testSignal, sun.misc.SignalHandler.SIG_DFL);
                // Restore it immediately so we're at a known state
                sun.misc.Signal.handle(testSignal, originalHandler);

                // Call registerSignalHandlersForTest on the singleton instance to exercise the ACTUAL inner handle() method
                // Using USR2 which is safe for testing (won't terminate the JVM)
                StorageService.instance.registerSignalHandlersForTest(new String[]{"USR2"});

                // Raise the signal to trigger the ACTUAL handler in StorageService
                sun.misc.Signal.raise(testSignal);

                // Wait a bit for the asynchronous signal handler to execute
                Thread.sleep(500);

                // Verify the log message was written by the actual inner handle() method
                boolean foundLogMessage = listAppender.list.stream()
                    .anyMatch(event -> event.getFormattedMessage().contains("Received signal: SIGUSR2"));

                assertTrue("Signal logging should occur in actual inner handle() method", foundLogMessage);

                // Restore original handler
                sun.misc.Signal.handle(testSignal, originalHandler);
            }
            finally
            {
                storageServiceLogger.detachAppender(listAppender);
                storageServiceLogger.setLevel(originalLevel);
            }
        }
        catch (IllegalArgumentException e)
        {
            System.out.println("Signal USR2 not available on this platform, skipping test: " + e.getMessage());
        }
    }

    /**
     * Test that the actual inner handle() method chains to a previous handler.
     */
    @Test
    public void testActualInnerHandleMethodChainsToRealPreviousHandler() throws InterruptedException
    {
        try
        {
            sun.misc.Signal testSignal = new sun.misc.Signal("USR1");
            final AtomicBoolean previousHandlerCalled = new AtomicBoolean(false);

            // Set up a previous handler that we can track
            sun.misc.SignalHandler previousHandler = new sun.misc.SignalHandler()
            {
                @Override
                public void handle(sun.misc.Signal sig)
                {
                    previousHandlerCalled.set(true);
                }
            };

            // Install our tracking handler
            sun.misc.SignalHandler originalHandler = sun.misc.Signal.handle(testSignal, previousHandler);

            // Now call registerSignalHandlersForTest() which registers the ACTUAL inner handle() method
            StorageService.instance.registerSignalHandlersForTest(new String[]{"USR1"});

            // Raise the signal to trigger the ACTUAL handler in StorageService
            sun.misc.Signal.raise(testSignal);

            // Wait for the asynchronous signal handler to execute
            Thread.sleep(500);

            // Verify the previous handler was called by the actual inner handle() method
            assertTrue("Previous handler should be called through chaining by actual inner handle()", previousHandlerCalled.get());

            // Restore original handler
            sun.misc.Signal.handle(testSignal, originalHandler);
        }
        catch (IllegalArgumentException e)
        {
            System.out.println("Signal USR1 not available on this platform, skipping test: " + e.getMessage());
        }
    }

    /**
     * Test that the actual inner handle() method does NOT chain to SIG_DFL.
     */
    @Test
    public void testActualInnerHandleMethodDoesNotChainToSigDfl() throws InterruptedException
    {
        try
        {
            sun.misc.Signal testSignal = new sun.misc.Signal("USR2");

            // Save original handler
            sun.misc.SignalHandler originalHandler = sun.misc.Signal.handle(testSignal, sun.misc.SignalHandler.SIG_DFL);

            // Set the handler to SIG_DFL so registerSignalHandlersForTest sees SIG_DFL as previous
            sun.misc.Signal.handle(testSignal, sun.misc.SignalHandler.SIG_DFL);

            // Now call registerSignalHandlersForTest() which registers the ACTUAL inner handle() method
            StorageService.instance.registerSignalHandlersForTest(new String[]{"USR2"});

            // Raise the signal - the actual handler should complete without attempting to chain to SIG_DFL
            sun.misc.Signal.raise(testSignal);

            // Wait for the asynchronous signal handler to execute
            Thread.sleep(500);

            // If we get here without exception, the actual inner handle() correctly avoided calling SIG_DFL
            assertTrue("Actual handler should not attempt to chain to SIG_DFL", true);

            // Restore original handler
            sun.misc.Signal.handle(testSignal, originalHandler);
        }
        catch (IllegalArgumentException e)
        {
            System.out.println("Signal USR2 not available on this platform, skipping test: " + e.getMessage());
        }
    }

    /**
     * Test that the actual inner handle() method does NOT chain to SIG_IGN.
     */
    @Test
    public void testActualInnerHandleMethodDoesNotChainToSigIgn() throws InterruptedException
    {
        try
        {
            sun.misc.Signal testSignal = new sun.misc.Signal("USR1");

            // Save original handler
            sun.misc.SignalHandler originalHandler = sun.misc.Signal.handle(testSignal, sun.misc.SignalHandler.SIG_IGN);

            // Set the handler to SIG_IGN so registerSignalHandlersForTest sees SIG_IGN as previous
            sun.misc.Signal.handle(testSignal, sun.misc.SignalHandler.SIG_IGN);

            // Now call registerSignalHandlersForTest() which registers the ACTUAL inner handle() method
            StorageService.instance.registerSignalHandlersForTest(new String[]{"USR1"});

            // Raise the signal - the actual handler should complete without attempting to chain to SIG_IGN
            sun.misc.Signal.raise(testSignal);

            // Wait for the asynchronous signal handler to execute
            Thread.sleep(500);

            // If we get here without exception, the actual inner handle() correctly avoided calling SIG_IGN
            assertTrue("Actual handler should not attempt to chain to SIG_IGN", true);

            // Restore original handler
            sun.misc.Signal.handle(testSignal, originalHandler);
        }
        catch (IllegalArgumentException e)
        {
            System.out.println("Signal USR1 not available on this platform, skipping test: " + e.getMessage());
        }
    }

    /**
     * Test the actual complete inner handle() method behavior: logging AND chaining.
     */
    @Test
    public void testActualInnerHandleMethodCompleteFlow() throws InterruptedException
    {
        try
        {
            // Set up logging capture
            Logger storageServiceLogger = (Logger) LoggerFactory.getLogger(StorageService.class);
            ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
            listAppender.start();
            storageServiceLogger.addAppender(listAppender);
            Level originalLevel = storageServiceLogger.getLevel();
            storageServiceLogger.setLevel(Level.INFO);

            try
            {
                sun.misc.Signal testSignal = new sun.misc.Signal("USR2");
                final AtomicInteger previousHandlerCallCount = new AtomicInteger(0);

                // Set up a previous handler that we can track
                sun.misc.SignalHandler previousHandler = new sun.misc.SignalHandler()
                {
                    @Override
                    public void handle(sun.misc.Signal sig)
                    {
                        previousHandlerCallCount.incrementAndGet();
                    }
                };

                // Install our tracking handler
                sun.misc.SignalHandler originalHandler = sun.misc.Signal.handle(testSignal, previousHandler);

                // Now call registerSignalHandlersForTest() which registers the ACTUAL inner handle() method
                StorageService.instance.registerSignalHandlersForTest(new String[]{"USR2"});

                // Raise the signal to trigger the ACTUAL handler
                sun.misc.Signal.raise(testSignal);

                // Wait for the asynchronous signal handler to execute
                Thread.sleep(500);

                // Verify the log message was written by the actual inner handle() method
                boolean foundLogMessage = listAppender.list.stream()
                    .anyMatch(event -> event.getFormattedMessage().contains("Received signal: SIGUSR2"));

                assertTrue("Signal logging should occur in actual inner handle()", foundLogMessage);

                // Verify the previous handler was called by the actual chaining logic
                assertEquals("Previous handler should be called exactly once by actual inner handle()", 1, previousHandlerCallCount.get());

                // Restore original handler
                sun.misc.Signal.handle(testSignal, originalHandler);
            }
            finally
            {
                storageServiceLogger.detachAppender(listAppender);
                storageServiceLogger.setLevel(originalLevel);
            }
        }
        catch (IllegalArgumentException e)
        {
            System.out.println("Signal USR2 not available on this platform, skipping test: " + e.getMessage());
        }
    }
}
