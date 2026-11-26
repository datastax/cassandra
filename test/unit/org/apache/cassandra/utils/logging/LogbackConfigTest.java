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

package org.apache.cassandra.utils.logging;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.status.Status;

import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the shipped {@code conf/logback.xml} stays loadable and functional across logback upgrades:
 * <ul>
 *   <li>it is parsed without any ERROR-level status (well-formed, all referenced classes are on the classpath),</li>
 *   <li>the SYSTEMLOG, STDOUT and ASYNCDEBUGLOG appenders are wired to the root logger and of the expected type,</li>
 *   <li>log events are routed according to the configured thresholds: INFO and above to stdout and system.log,
 *       everything (including DEBUG) to debug.log.</li>
 * </ul>
 * The configuration is loaded into a fresh, detached {@link LoggerContext} so the logging of the test framework
 * is not affected.
 */
public class LogbackConfigTest
{
    private static final String CONFIG_FILE = "conf/logback.xml";

    private static final String INFO_SENTINEL = "logback-config-test-info-sentinel";
    private static final String DEBUG_SENTINEL = "logback-config-test-debug-sentinel";

    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;
    private Path logDir;
    private WithProperties properties;
    private LoggerContext ctx;

    @Before
    public void setUp() throws IOException
    {
        logDir = Files.createTempDirectory("logback-config-test-");
        properties = new WithProperties().set(LOG_DIR, logDir.toString());

        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut, true));
    }

    @After
    public void tearDown()
    {
        if (ctx != null)
            ctx.stop();

        System.setOut(originalOut);
        properties.close();
    }

    @Test
    public void configurationLoadsWithoutErrors() throws JoranException
    {
        ctx = loadConfig();

        int highest = ctx.getStatusManager().getCopyOfStatusList()
                         .stream()
                         .mapToInt(Status::getLevel)
                         .max()
                         .orElse(Status.INFO);
        assertTrue("conf/logback.xml produced an ERROR status: " + ctx.getStatusManager().getCopyOfStatusList(),
                   highest < Status.ERROR);
    }

    @Test
    public void rootLoggerHasExpectedAppenders() throws JoranException
    {
        ctx = loadConfig();
        Logger root = ctx.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);

        assertTrue("SYSTEMLOG should be a RollingFileAppender",
                   root.getAppender("SYSTEMLOG") instanceof RollingFileAppender);
        assertTrue("STDOUT should be a ConsoleAppender",
                   root.getAppender("STDOUT") instanceof ConsoleAppender);
        assertTrue("ASYNCDEBUGLOG should be an AsyncAppender",
                   root.getAppender("ASYNCDEBUGLOG") instanceof AsyncAppender);

        for (String name : new String[]{ "SYSTEMLOG", "STDOUT", "ASYNCDEBUGLOG" })
            assertTrue(name + " should have been started", root.getAppender(name).isStarted());
    }

    @Test
    public void eventsAreRoutedAccordingToConfiguredThresholds() throws JoranException, IOException
    {
        ctx = loadConfig();

        // org.apache.cassandra is configured at DEBUG, so both events pass the logger level check
        Logger logger = ctx.getLogger("org.apache.cassandra.utils.logging.LogbackConfigTest");
        logger.info(INFO_SENTINEL);
        logger.debug(DEBUG_SENTINEL);

        // stopping the context flushes the async debug.log appender and closes the files
        ctx.stop();

        String stdout = new String(capturedOut.toByteArray(), StandardCharsets.UTF_8);
        String systemLog = read("system.log");
        String debugLog = read("debug.log");

        assertTrue("INFO event should be written to stdout: " + stdout, stdout.contains(INFO_SENTINEL));
        assertFalse("DEBUG event must not be written to stdout: " + stdout, stdout.contains(DEBUG_SENTINEL));

        assertTrue("INFO event should be written to system.log: " + systemLog, systemLog.contains(INFO_SENTINEL));
        assertFalse("DEBUG event must not be written to system.log: " + systemLog, systemLog.contains(DEBUG_SENTINEL));

        assertTrue("INFO event should be written to debug.log: " + debugLog, debugLog.contains(INFO_SENTINEL));
        assertTrue("DEBUG event should be written to debug.log: " + debugLog, debugLog.contains(DEBUG_SENTINEL));
    }

    @Test
    public void loggerLevelsMatchConfiguration() throws JoranException
    {
        ctx = loadConfig();

        assertEquals(ch.qos.logback.classic.Level.INFO,
                     ctx.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).getLevel());
        assertEquals(ch.qos.logback.classic.Level.DEBUG,
                     ctx.getLogger("org.apache.cassandra").getLevel());
    }

    @Test
    public void configurationChangesAreDetected() throws Exception
    {
        // Work on a copy of the shipped config with a short scan period, so the test doesn't wait 60 seconds
        String shipped = new String(Files.readAllBytes(new File(CONFIG_FILE).toPath()), StandardCharsets.UTF_8);
        String original = shipped.replaceFirst("scanPeriod=\"[^\"]*\"", "scanPeriod=\"1 seconds\"");
        Path watched = logDir.resolve("logback-scan-test.xml");
        Files.write(watched, original.getBytes(StandardCharsets.UTF_8));
        ctx = loadConfig(watched.toFile());

        // File systems may have coarse timestamps and logback detects a change by comparing them,
        // so move the modification time into the future
        String changed = original.replace("<logger name=\"org.apache.cassandra\" level=\"DEBUG\"/>",
                                          "<logger name=\"org.apache.cassandra\" level=\"ERROR\"/>");
        Files.write(watched, changed.getBytes(StandardCharsets.UTF_8));
        Files.setLastModifiedTime(watched, FileTime.fromMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5)));

        // logback checks the file on its own scheduler thread, so the new level is applied asynchronously
        spinAssertEquals("logback should detect the changed config file",
                         ch.qos.logback.classic.Level.ERROR,
                         () -> ctx.getLogger("org.apache.cassandra").getLevel(),
                         10, TimeUnit.SECONDS);
    }

    private static LoggerContext loadConfig() throws JoranException
    {
        return loadConfig(new File(CONFIG_FILE));
    }

    private static LoggerContext loadConfig(File config) throws JoranException
    {
        assertTrue("Cannot find " + config.getAbsolutePath(), config.exists());

        // Fresh, detached context. A LoggerContext created by hand has no MDC adapter (the slf4j service
        // provider sets one on the real context), and without it every appender fails and nothing is logged.
        LoggerContext context = new LoggerContext();
        context.setName("LogbackConfigTest");
        context.setMDCAdapter(new LogbackMDCAdapter());
        context.start();

        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(config);
        return context;
    }

    private String read(String fileName) throws IOException
    {
        Path file = logDir.resolve(fileName);
        assertNotNull(file);
        assertTrue(fileName + " should have been created in " + logDir, Files.exists(file));
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }
}
