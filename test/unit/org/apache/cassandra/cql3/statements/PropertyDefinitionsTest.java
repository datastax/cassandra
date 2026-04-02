/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql3.statements;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.codahale.metrics.Clock;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.exceptions.SyntaxException;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.cql3.statements.PropertyDefinitions.parseBoolean;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PropertyDefinitionsTest
{

    private ListAppender<ILoggingEvent> logAppender;
    private Logger logger;
    private Field lastLoggedTimeField;

    @Before
    public void setup() throws Exception
    {
        logger = (Logger) LoggerFactory.getLogger(PropertyDefinitions.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        logger.addAppender(logAppender);
        lastLoggedTimeField = PropertyDefinitions.class.getDeclaredField("OBSOLETE_PROPERTY_LAST_LOG_TIMES");
        lastLoggedTimeField.setAccessible(true);
    }

    @After
    public void cleanup() throws Exception
    {
        logger.detachAppender(logAppender);

        // Clear map
        Map<String, Long> lastLoggedTime = (Map<String, Long>) lastLoggedTimeField.get(null);
        lastLoggedTime.clear();
    }

    @Test
    public void testPostiveBooleanParsing()
    {
        assertTrue(parseBoolean("prop1", "1"));
        assertTrue(parseBoolean("prop2", "true"));
        assertTrue(parseBoolean("prop3", "True"));
        assertTrue(parseBoolean("prop4", "TrUe"));
        assertTrue(parseBoolean("prop5", "yes"));
        assertTrue(parseBoolean("prop6", "Yes"));
    }

    @Test
    public void testNegativeBooleanParsing()
    {
        assertFalse(parseBoolean("prop1", "0"));
        assertFalse(parseBoolean("prop2", "false"));
        assertFalse(parseBoolean("prop3", "False"));
        assertFalse(parseBoolean("prop4", "FaLse"));
        assertFalse(parseBoolean("prop6", "No"));
    }

    @Test
    public void testGetProperty()
    {
        String key = "k";
        String value = "v";
        PropertyDefinitions pd = new PropertyDefinitions();
        pd.addProperty(key, value);
        assertEquals(value, pd.getProperty(key).toString());
    }

    @Test(expected = SyntaxException.class)
    public void testGetMissingProperty()
    {
        PropertyDefinitions pd = new PropertyDefinitions();
        pd.getProperty("missing");
    }

    @Test(expected = SyntaxException.class)
    public void testInvalidPositiveBooleanParsing()
    {
        parseBoolean("cdc", "tru");
    }

    @Test(expected = SyntaxException.class)
    public void testInvalidNegativeBooleanParsing()
    {
        parseBoolean("cdc", "fals");
    }

    @Test
    public void testAddProperty()
    {
        // string overload
        testAddProperty("v1", "v2", (pd, v) -> pd.addProperty("k", v));

        // map overload
        testAddProperty(new HashMap<String, String>(){{put("k1", "v1");}},
                        new HashMap<String, String>(){{put("k2", "v2");}},
                        (pd, v) -> pd.addProperty("k", v));

        // set of QualifiedName overload
        testAddProperty(Collections.singleton(new QualifiedName("keyspace", "v1")),
                        Collections.singleton(new QualifiedName("keyspace", "v2")),
                        (pd, v) -> pd.addProperty("k", v));
    }

    private <V> void testAddProperty(V oldValue, V newValue, BiConsumer<PropertyDefinitions, V> adder)
    {
        String key = "k";
        PropertyDefinitions pd = new PropertyDefinitions();
        adder.accept(pd, oldValue);

        Assertions.assertThat(pd.getProperty(key)).isEqualTo(oldValue);
        Assertions.assertThatThrownBy(() -> adder.accept(pd, newValue))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining(String.format(PropertyDefinitions.MULTIPLE_DEFINITIONS_ERROR, key));
        Assertions.assertThat(pd.getProperty(key)).isEqualTo(oldValue);
    }

    @Test
    public void testObsoletePropertyWarningRateLimiting() throws Exception
    {
        String obsoleteProperty = "old_prop";
        TestClock testClock = new TestClock();

        // First call - should log
        testClock.setTime(0);
        PropertyDefinitions pd1 = new PropertyDefinitions(new TestClock());
        pd1.addProperty(obsoleteProperty, "value1");
        pd1.validate(Collections.emptySet(), Collections.singleton(obsoleteProperty));

        List<ILoggingEvent> logs1 = getWarningLogs(obsoleteProperty);
        assertEquals("First call should log warning", 1, logs1.size());
        assertTrue(logs1.get(0).getFormattedMessage().contains("Ignoring obsolete property"));

        logAppender.list.clear();

        // Second call immediately - should NOT log (within 30 seconds)
        testClock.setTime(100); // Only 100ms passed
        PropertyDefinitions pd2 = new PropertyDefinitions(testClock);
        pd2.addProperty(obsoleteProperty, "value2");
        pd2.validate(Collections.emptySet(), Collections.singleton(obsoleteProperty));

        List<ILoggingEvent> logs2 = getWarningLogs(obsoleteProperty);
        assertEquals("Second call within 100ms should not log", 0, logs2.size());

        logAppender.list.clear();

        // Advance time by 30 seconds and try again - should log
        testClock.setTime(30_100); // 30 seconds + 100ms from start

        PropertyDefinitions pd3 = new PropertyDefinitions(testClock);
        pd3.addProperty(obsoleteProperty, "value3");
        pd3.validate(Collections.emptySet(), Collections.singleton(obsoleteProperty));

        List<ILoggingEvent> logs3 = getWarningLogs(obsoleteProperty);
        assertEquals("Third call after 30s should log again", 1, logs3.size());
    }

    @Test
    public void testObsoletePropertyWarningPerProperty()
    {
        String obsoleteProperty1 = "old_prop1";
        String obsoleteProperty2 = "old_prop2";

        // First property - should log
        PropertyDefinitions pd1 = new PropertyDefinitions();
        pd1.addProperty(obsoleteProperty1, "value1");
        pd1.validate(Collections.emptySet(), Collections.singleton(obsoleteProperty1));

        List<ILoggingEvent> logs1 = getWarningLogs(obsoleteProperty1);
        assertEquals("First property should log", 1, logs1.size());

        logAppender.list.clear();

        // Second property immediately - should ALSO log (different property)
        PropertyDefinitions pd2 = new PropertyDefinitions();
        pd2.addProperty(obsoleteProperty2, "value2");
        pd2.validate(Collections.emptySet(), Collections.singleton(obsoleteProperty2));

        List<ILoggingEvent> logs2 = getWarningLogs(obsoleteProperty2);
        assertEquals("Different property should log independently", 1, logs2.size());
    }

    private List<ILoggingEvent> getWarningLogs(String propertyName)
    {
        return logAppender.list.stream()
                               .filter(event -> event.getLevel() == Level.WARN)
                               .filter(event -> event.getFormattedMessage().contains(propertyName))
                               .collect(Collectors.toList());
    }


    // Custom Clock implementation for testing
    private static class TestClock extends Clock
    {
        private long currentTime = 0;

        @Override
        public long getTick()
        {
            return currentTime * 1_000_000; // Convert ms to ns
        }

        @Override
        public long getTime()
        {
            return currentTime;
        }

        public void setTime(long timeMs)
        {
            this.currentTime = timeMs;
        }
    }
}
