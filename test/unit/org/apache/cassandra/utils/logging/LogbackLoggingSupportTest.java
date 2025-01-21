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

import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import static org.junit.Assert.assertSame;

public class LogbackLoggingSupportTest
{
    @Test
    public void setLogLevelWithoutOptionsReloadsConfiguration() throws Exception
    {
        // given
        Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        assertSame("the default test log level is DEBUG", Level.DEBUG, rootLogger.getLevel());

        rootLogger.setLevel(Level.TRACE);
        assertSame("the log level should have been switched to TRACE", Level.TRACE, rootLogger.getLevel());

        LogbackLoggingSupport loggingSupport = new LogbackLoggingSupport();
        loggingSupport.onStartup();

        // when
        // empty class and level reset to the default configuration
        loggingSupport.setLoggingLevel("", "");

        // then
        assertSame("reset test log level should be DEBUG", Level.DEBUG, rootLogger.getLevel());
    }
}