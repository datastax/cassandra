/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.service;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The daemon starts the executor liveness watchdog during setup, and a watchdog that fails to start, as on a
 * malformed property, does not fail the startup: the daemon logs a warning and goes on.
 */
public class CassandraDaemonExecutorLivenessWatchdogTest
{
    private static final String WATCHDOG_LOGGER = ExecutorLivenessWatchdog.class.getName();
    private static final String DAEMON_LOGGER = CassandraDaemon.class.getName();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void stopWatchdog()
    {
        ExecutorLivenessWatchdog.stop();
    }

    // captures what a logger logs until closed
    private static final class CapturedLog implements AutoCloseable
    {
        private final Logger logger;
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        CapturedLog(String loggerName)
        {
            logger = (Logger) LoggerFactory.getLogger(loggerName);
            appender.start();
            logger.addAppender(appender);
        }

        List<ILoggingEvent> events()
        {
            synchronized (appender)
            {
                return List.copyOf(appender.list);
            }
        }

        List<String> messages(Level level)
        {
            return events().stream()
                           .filter(event -> event.getLevel() == level)
                           .map(ILoggingEvent::getFormattedMessage)
                           .collect(Collectors.toList());
        }

        @Override
        public void close()
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    public void testStartsTheWatchdog()
    {
        try (CapturedLog watchdogLog = new CapturedLog(WATCHDOG_LOGGER);
             CapturedLog daemonLog = new CapturedLog(DAEMON_LOGGER))
        {
            CassandraDaemon.startExecutorLivenessWatchdog();

            List<String> started = watchdogLog.messages(Level.INFO);
            assertEquals(started.toString(), 1, started.size());
            assertTrue(started.get(0), started.get(0).startsWith("Executor liveness watchdog started: "));
            assertEquals(List.of(), daemonLog.messages(Level.WARN));
        }
    }

    @Test
    public void testFailureToStartDoesNotFailStartup()
    {
        // a value start() cannot parse, so it throws rather than refusing the configuration itself
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS, "ten");
             CapturedLog watchdogLog = new CapturedLog(WATCHDOG_LOGGER);
             CapturedLog daemonLog = new CapturedLog(DAEMON_LOGGER))
        {
            CassandraDaemon.startExecutorLivenessWatchdog();

            List<ILoggingEvent> warnings = daemonLog.events().stream()
                                                    .filter(event -> event.getLevel() == Level.WARN)
                                                    .collect(Collectors.toList());
            assertEquals(warnings.toString(), 1, warnings.size());
            ILoggingEvent warning = warnings.get(0);
            assertEquals("Unable to start the executor liveness watchdog", warning.getFormattedMessage());
            assertEquals(ConfigurationException.class.getName(), warning.getThrowableProxy().getClassName());
            // not started
            assertEquals(List.of(), watchdogLog.messages(Level.INFO));
        }
    }
}
