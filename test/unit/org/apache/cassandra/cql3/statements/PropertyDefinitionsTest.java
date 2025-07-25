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

import java.util.Collections;
import java.util.HashMap;
import java.util.function.BiConsumer;

import org.junit.Test;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.exceptions.SyntaxException;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.cql3.statements.PropertyDefinitions.parseBoolean;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PropertyDefinitionsTest
{
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
}
