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

package org.apache.cassandra.config;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_RELEVANT_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

// checkstyle: suppress below 'blockSystemPropertyUsage'

public class CassandraRelevantPropertiesTest
{
    @Before
    public void setup()
    {
        System.clearProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey());
    }

    @Test
    public void testSystemPropertyisSet() {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_RELEVANT_PROPERTIES, "test"))
        {
            assertThat(System.getProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey())).isEqualTo("test"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        }
    }

    @Test
    public void testString()
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_RELEVANT_PROPERTIES, "some-string"))
        {
            assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getString()).isEqualTo("some-string");
        }
    }

    @Test
    public void testString_null()
    {
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getString()).isNull();
    }

    @Test
    public void testString_override_default()
    {
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getString("other-string")).isEqualTo("other-string");
        TEST_CASSANDRA_RELEVANT_PROPERTIES.setString("this-string");
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getString("other-string")).isEqualTo("this-string");
    }

    @Test
    public void testBoolean()
    {
        System.setProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey(), "true");
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getBoolean()).isTrue();
        System.setProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey(), "false");
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getBoolean()).isFalse();
        System.setProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey(), "junk");
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getBoolean()).isFalse();
        System.setProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey(), "");
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getBoolean()).isFalse();
    }

    @Test
    public void testBoolean_null()
    {
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getBoolean()).isFalse();
    }

    @Test
    public void testBoolean_override_default()
    {
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getBoolean(true)).isTrue();
        TEST_CASSANDRA_RELEVANT_PROPERTIES.setBoolean(false);
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getBoolean(true)).isFalse();
    }

    @Test
    public void testDecimal()
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_RELEVANT_PROPERTIES, "123456789"))
        {
            assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getInt()).isEqualTo(123456789);
        }
    }

    @Test
    public void testHexadecimal()
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_RELEVANT_PROPERTIES, "0x1234567a"))
        {
            assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getInt()).isEqualTo(305419898);
        }
    }

    @Test
    public void testOctal()
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_RELEVANT_PROPERTIES, "01234567"))
        {
            assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getInt()).isEqualTo(342391);
        }
    }

    @Test(expected = ConfigurationException.class)
    public void testInteger_empty()
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_RELEVANT_PROPERTIES, ""))
        {
            assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getInt()).isEqualTo(342391);
        }
    }

    @Test(expected = ConfigurationException.class)
    public void testInteger_null()
    {
        try (WithProperties properties = new WithProperties())
        {
            TEST_CASSANDRA_RELEVANT_PROPERTIES.getInt();
        }
    }

    @Test
    public void testInteger_override_default()
    {
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getInt(2345)).isEqualTo(2345);
        TEST_CASSANDRA_RELEVANT_PROPERTIES.setInt(1234);
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getInt(2345)).isEqualTo(1234);
    }

    @Test
    public void testLong()
    {
        System.setProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey(), "1234");
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getLong()).isEqualTo(1234);
    }

    @Test(expected = ConfigurationException.class)
    public void testLong_empty()
    {
        System.setProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey(), "");
        TEST_CASSANDRA_RELEVANT_PROPERTIES.getLong();
        fail("Expected ConfigurationException");
    }

    @Test(expected = ConfigurationException.class)
    public void testLong_null()
    {
        TEST_CASSANDRA_RELEVANT_PROPERTIES.getLong();
        fail("Expected NullPointerException");
    }

    @Test
    public void testLong_override_default()
    {
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getLong(2345)).isEqualTo(2345);
        TEST_CASSANDRA_RELEVANT_PROPERTIES.setLong(1234);
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getLong(2345)).isEqualTo(1234);
    }

    @Test
    public void testDouble()
    {
        System.setProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey(), "1.567");
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getDouble()).isEqualTo(1.567);
    }

    @Test(expected = ConfigurationException.class)
    public void testDouble_empty()
    {
        System.setProperty(TEST_CASSANDRA_RELEVANT_PROPERTIES.getKey(), "");
        TEST_CASSANDRA_RELEVANT_PROPERTIES.getDouble();
        fail("Expected ConfigurationException");
    }

    @Test(expected = ConfigurationException.class)
    public void testDouble_null()
    {
        TEST_CASSANDRA_RELEVANT_PROPERTIES.getDouble();
        fail("Expected NullPointerException");
    }

    @Test
    public void testDouble_override_default()
    {
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getDouble(2.345)).isEqualTo(2.345);
        TEST_CASSANDRA_RELEVANT_PROPERTIES.setDouble(1.234);
        assertThat(TEST_CASSANDRA_RELEVANT_PROPERTIES.getDouble(2.345)).isEqualTo(1.234);
    }

    @Test
    public void testClearProperty()
    {
        assertNull(TEST_CASSANDRA_RELEVANT_PROPERTIES.getString());
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_RELEVANT_PROPERTIES, "test"))
        {
            assertEquals("test", TEST_CASSANDRA_RELEVANT_PROPERTIES.getString());
        }
        assertNull(TEST_CASSANDRA_RELEVANT_PROPERTIES.getString());
    }
}
