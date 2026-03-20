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

package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.TrieMemtableFactory;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MemtableParamsTest
{
    static final ParameterizedClass DEFAULT = TrieMemtableFactory.CONFIGURATION;

    @Test
    public void testDefault()
    {
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions(ImmutableMap.of());
        assertEquals(ImmutableMap.of("default", DEFAULT), map);
    }

    @Test
    public void testDefaultRemapped()
    {
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("remap", new InheritingClass("default", null, null))
        );
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "remap", DEFAULT),
                     map);
    }

    @Test
    public void testOne()
    {
        final InheritingClass one = new InheritingClass(null, "SkipList", null);
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions(ImmutableMap.of("one", one));
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one),
                     map);
    }

    @Test
    public void testOrderingDoesNotMatter()
    {
        // linked hash map preserves insertion order
        Map<String, InheritingClass> config = new LinkedHashMap<>();

        config.put("default", new InheritingClass("trie", null, null));
        config.put("abc", new InheritingClass(null, "Abc", ImmutableMap.of("c", "d")));
        config.put("skiplist", new InheritingClass("skiplistOnSteroids", "SkipListMemtable", ImmutableMap.of("e", "f")));
        config.put("skiplistOnSteroids", new InheritingClass("abc", null, ImmutableMap.of("a", "b")));
        config.put("trie", new InheritingClass(null, "TrieMemtable", null));

        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions(config);
        assertEquals("TrieMemtable", map.get("default").class_name);

        // this inherits abc which has c as config parameter
        assertTrue(map.get("skiplistOnSteroids").parameters.containsKey("a"));
        assertTrue(map.get("skiplistOnSteroids").parameters.containsKey("c"));
        assertEquals(map.get("skiplistOnSteroids").class_name, config.get("abc").class_name);

        // this inherits from skiplistOnSteroids which so params are carried over as well
        assertTrue(map.get("skiplist").parameters.containsKey("a"));
        assertTrue(map.get("skiplist").parameters.containsKey("c"));
        assertTrue(map.get("skiplist").parameters.containsKey("e"));
        assertEquals(map.get("skiplist").class_name, "SkipListMemtable");

        Map<String, InheritingClass> config2 = new LinkedHashMap<>();
        config2.put("skiplist", new InheritingClass(null, "SkipListMemtable", null));
        config2.put("trie", new InheritingClass(null, "TrieMemtable", null));
        config2.put("default", new InheritingClass("trie", null, null));

        Map<String, ParameterizedClass> map2 = MemtableParams.expandDefinitions(config2);
        assertEquals("TrieMemtable", map2.get("default").class_name);
    }

    @Test
    public void testExtends()
    {
        final InheritingClass one = new InheritingClass(null, "SkipList", null);
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("one", one,
                            "two", new InheritingClass("one",
                                                       null,
                                                       ImmutableMap.of("extra", "value")))
        );

        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", new ParameterizedClass("SkipList",
                                                                   ImmutableMap.of("extra", "value"))),
                     map);
    }

    @Test
    public void testExtendsReplace()
    {
        final InheritingClass one = new InheritingClass(null,
                                                        "SkipList",
                                                        ImmutableMap.of("extra", "valueOne"));
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("one", one,
                            "two", new InheritingClass("one",
                                                       null,
                                                       ImmutableMap.of("extra", "value")))
        );
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", new ParameterizedClass("SkipList",
                                                                   ImmutableMap.of("extra", "value"))),
                     map);
    }

    @Test
    public void testDoubleExtends()
    {
        final InheritingClass one = new InheritingClass(null, "SkipList", null);
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("one", one,
                            "two", new InheritingClass("one",
                                                       null,
                                                       ImmutableMap.of("param", "valueTwo",
                                                                       "extra", "value")),
                            "three", new InheritingClass("two",
                                                         "OtherClass",
                                                         ImmutableMap.of("param", "valueThree",
                                                                         "extraThree", "three")))
        );
        assertEquals(ImmutableMap.of("default", DEFAULT,
                                     "one", one,
                                     "two", new ParameterizedClass("SkipList",
                                                                   ImmutableMap.of("param", "valueTwo",
                                                                                   "extra", "value")),
                                     "three", new ParameterizedClass("OtherClass",
                                                                     ImmutableMap.of("param", "valueThree",
                                                                                     "extra", "value",
                                                                                     "extraThree", "three"))),
                     map);
    }

    @Test
    public void testInvalidSelfExtends()
    {
        try
        {
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
                ImmutableMap.of("one", new InheritingClass("one",
                                                           null,
                                                           ImmutableMap.of("extra", "value")))
            );
            fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    @Test
    public void testReplaceDefault()
    {
        final InheritingClass one = new InheritingClass(null,
                                                        "SkipList",
                                                        ImmutableMap.of("extra", "valueOne"));
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions(ImmutableMap.of("default", one));
        assertEquals(ImmutableMap.of("default", one), map);
    }

    @Test
    public void testDefaultExtends()
    {
        final InheritingClass one = new InheritingClass(null,
                                                        "SkipList",
                                                        ImmutableMap.of("extra", "valueOne"));
        Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
        (
            ImmutableMap.of("one", one,
                            "default", new InheritingClass("one", null, ImmutableMap.of()))
        );
        assertEquals(ImmutableMap.of("one", one,
                                     "default", one),
                     map);
    }
    // Note: The factories constructed from these parameters are tested in the CreateTest and AlterTest.

    @Test
    public void testInheritsNonExistent()
    {
        try
        {
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
            ImmutableMap.of("one", new InheritingClass("two",
                                                       null,
                                                       ImmutableMap.of("extra", "value")),
                            "two", new InheritingClass("three",
                                                       null,
                                                       ImmutableMap.of("extra2", "value2")))
            );
            fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    @Test
    public void testInvalidLoops()
    {
        try
        {
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
            ImmutableMap.of("one", new InheritingClass("two",
                                                       null,
                                                       ImmutableMap.of("extra", "value")),
                            "two", new InheritingClass("one",
                                                       null,
                                                       ImmutableMap.of("extra2", "value2")))
            );
            fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }

        try
        {
            Map<String, ParameterizedClass> map = MemtableParams.expandDefinitions
            (
            ImmutableMap.of("one", new InheritingClass("two",
                                                       null,
                                                       ImmutableMap.of("extra", "value")),
                            "two", new InheritingClass("three",
                                                       null,
                                                       ImmutableMap.of("extra2", "value2")),
                            "three", new InheritingClass("one",
                                                         null,
                                                         ImmutableMap.of("extra3", "value3")))
            );
            fail("Expected exception.");
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    // ========================================================================
    // CC4 to CC5 Upgrade Compatibility Tests
    // ========================================================================

    /**
     * Helper method to create a row with CC4 binary map data.
     */
    private UntypedResultSet.Row createCC4MapRow(Map<String, String> cc4Map)
    {
        ByteBuffer serialized = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false)
                                       .decompose(cc4Map);
        Map<String, ByteBuffer> data = new HashMap<>();
        data.put("memtable", serialized);
        return new UntypedResultSet.Row(data);
    }

    /**
     * Helper method to create a row with CC5 text data.
     */
    private UntypedResultSet.Row createCC5TextRow(String textValue)
    {
        ByteBuffer serialized = UTF8Type.instance.decompose(textValue);
        Map<String, ByteBuffer> data = new HashMap<>();
        data.put("memtable", serialized);
        return new UntypedResultSet.Row(data);
    }

    /**
     * Helper method to assert memtable configuration key.
     */
    private void assertMemtableConfigKey(UntypedResultSet.Row row, String expectedKey)
    {
        MemtableParams params = MemtableParams.getWithCC4Fallback(row, "memtable");
        assertNotNull(params);
        assertEquals(expectedKey, params.configurationKey());
    }

    /**
     * Test CC4 empty map upgrade.
     * CC4 stored empty map as 4 null bytes (32-bit integer = 0 entries).
     */
    @Test
    public void testCC4EmptyMapUpgrade()
    {
        ByteBuffer emptyMap = ByteBuffer.wrap(new byte[]{ 0x00, 0x00, 0x00, 0x00});
        Map<String, ByteBuffer> data = new HashMap<>();
        data.put("memtable", emptyMap);
        UntypedResultSet.Row row = new UntypedResultSet.Row(data);

        MemtableParams params = MemtableParams.getWithCC4Fallback(row, "memtable");
        assertEquals(MemtableParams.DEFAULT, params);
    }

    /**
     * Test CC4 TrieMemtable configuration upgrade.
     * CC4 stored: {"class": "TrieMemtable"} → Should map to CC5 "trie"
     */
    @Test
    public void testCC4TrieMemtableUpgrade()
    {
        UntypedResultSet.Row row = createCC4MapRow(ImmutableMap.of("class", "TrieMemtable"));
        assertMemtableConfigKey(row, "trie");
    }

    /**
     * Test CC4 SkipListMemtable configuration upgrade.
     * CC4 stored: {"class": "SkipListMemtable"} → Should map to CC5 "skiplist"
     */
    @Test
    public void testCC4SkipListMemtableUpgrade()
    {
        UntypedResultSet.Row row = createCC4MapRow(ImmutableMap.of("class", "SkipListMemtable"));
        assertMemtableConfigKey(row, "skiplist");
    }

    /**
     * Test CC4 fully qualified class name upgrade.
     * CC4 stored: {"class": "org.apache.cassandra.db.memtable.TrieMemtable"}
     * Should extract short name and map to CC5 "trie"
     */
    @Test
    public void testCC4FullyQualifiedClassNameUpgrade()
    {
        UntypedResultSet.Row row = createCC4MapRow(
            ImmutableMap.of("class", "org.apache.cassandra.db.memtable.TrieMemtable")
        );
        assertMemtableConfigKey(row, "trie");
    }

    /**
     * Test CC4 map with additional parameters (should still work).
     * CC4 stored: {"class": "TrieMemtable", "extra_param": "value"}
     * Should extract class name and map to CC5 "trie"
     */
    @Test
    public void testCC4MapWithExtraParametersUpgrade()
    {
        UntypedResultSet.Row row = createCC4MapRow(
            ImmutableMap.of("class", "TrieMemtable", "extra_param", "some_value")
        );
        assertMemtableConfigKey(row, "trie");
    }

    /**
     * Test CC4 map without "class" key (should fall back to default).
     * CC4 corrupted data: {"other_key": "value"} → Should fall back to DEFAULT
     */
    @Test
    public void testCC4MapWithoutClassKeyUpgrade()
    {
        UntypedResultSet.Row row = createCC4MapRow(ImmutableMap.of("other_key", "value"));
        MemtableParams params = MemtableParams.getWithCC4Fallback(row, "memtable");
        assertEquals(MemtableParams.DEFAULT, params);
    }

    /**
     * Test CC5 text values (normal operation, should work unchanged).
     */
    @Test
    public void testCC5TextValueTrie()
    {
        assertMemtableConfigKey(createCC5TextRow("trie"), "trie");
    }

    @Test
    public void testCC5TextValueSkiplist()
    {
        assertMemtableConfigKey(createCC5TextRow("skiplist"), "skiplist");
    }

    @Test
    public void testCC5TextValueDefault()
    {
        assertMemtableConfigKey(createCC5TextRow("default"), "default");
    }

    /**
     * Test missing memtable column (should return DEFAULT).
     * This happens when reading old schema that predates the memtable column.
     */
    @Test
    public void testMissingMemtableColumn()
    {
        UntypedResultSet.Row row = new UntypedResultSet.Row(new HashMap<>());
        MemtableParams params = MemtableParams.getWithCC4Fallback(row, "memtable");
        assertEquals(MemtableParams.DEFAULT, params);
    }

    /**
     * Test null memtable value (should return DEFAULT).
     */
    @Test
    public void testNullMemtableValue()
    {
        Map<String, ByteBuffer> data = new HashMap<>();
        data.put("memtable", null);
        UntypedResultSet.Row row = new UntypedResultSet.Row(data);

        MemtableParams params = MemtableParams.getWithCC4Fallback(row, "memtable");
        assertEquals(MemtableParams.DEFAULT, params);
    }

    // ========================================================================
    // StorageCompatibilityMode Writing Tests
    // ========================================================================

    /**
     * Test that asSchemaValueMap() returns a Map in CC_4 and CASSANDRA_4 compatibility modes.
     * Both modes should write memtable as {@code frozen<map<text, text>>} for CC4 compatibility.
     * This ensures downgrade to CC4 is safe.
     */
    @Test
    public void testAsSchemaValueMapInCC4CompatibilityModes()
    {
        // Test both CC_4 and CASSANDRA_4 modes (they should behave identically)
        for (StorageCompatibilityMode mode : new StorageCompatibilityMode[]{StorageCompatibilityMode.CC_4,
                                                                             StorageCompatibilityMode.CASSANDRA_4})
        {
            // Test DEFAULT memtable - CC4 writes empty map {} for "default" configuration
            Map<String, String> defaultMap = MemtableParams.DEFAULT.asSchemaValueMap(mode);
            assertNotNull("Should return Map in " + mode + " mode", defaultMap);
            assertTrue("Default should be empty map in " + mode + " mode", defaultMap.isEmpty());
        }
    }

    /**
     * Test that asSchemaValueMap() throws exception when called in CC5 mode.
     */
    @Test(expected = IllegalStateException.class)
    public void testAsSchemaValueMapThrowsInCC5Mode()
    {
        MemtableParams.DEFAULT.asSchemaValueMap(StorageCompatibilityMode.NONE);
    }

    /**
     * Test that asSchemaValueMap() rejects incompatible configurations in CC_4 mode.
     * Tests both CC5-only types (sharded) and unknown configurations.
     */
    @Test
    public void testAsSchemaValueMapRejectsIncompatibleConfigurations()
    {
        // Test 1: Sharded memtables (CC5-only, don't exist in CC4)
        MemtableParams shardedParams = MemtableParams.forTesting(MemtableParams.DEFAULT.factory(), "sharded-skiplist");
        try
        {
            shardedParams.asSchemaValueMap(StorageCompatibilityMode.CC_4);
            fail("Should have thrown ConfigurationException for sharded memtable in CC_4 mode");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention CC4 incompatibility",
                      e.getMessage().contains("not compatible with CC4"));
            assertTrue("Error message should mention sharded types",
                      e.getMessage().contains("Sharded memtable types"));
        }

        // Test 2: Unknown configurations (might not exist in CC4)
        MemtableParams unknownParams = MemtableParams.forTesting(MemtableParams.DEFAULT.factory(), "unknown-memtable-type");
        try
        {
            unknownParams.asSchemaValueMap(StorageCompatibilityMode.CC_4);
            fail("Should have thrown ConfigurationException for unknown configuration in CC_4 mode");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention configuration not found",
                      e.getMessage().contains("not found in cassandra.yaml"));
            assertTrue("Error message should mention CC4 compatibility mode",
                      e.getMessage().contains("CC4 compatibility mode"));
        }
    }

    /**
     * Test that asSchemaValueText() returns a String in CC5 mode (NONE).
     * This is the normal CC5 operation.
     */
    @Test
    public void testAsSchemaValueTextInCC5Mode()
    {
        // Test DEFAULT memtable in CC5 mode
        String defaultValue = MemtableParams.DEFAULT.asSchemaValueText(StorageCompatibilityMode.NONE);
        assertEquals("default", defaultValue);
    }

    /**
     * Test that asSchemaValueText() throws exception when called in CC4 compatibility mode.
     */
    @Test(expected = IllegalStateException.class)
    public void testAsSchemaValueTextThrowsInCC4Mode()
    {
        MemtableParams.DEFAULT.asSchemaValueText(StorageCompatibilityMode.CC_4);
    }

}
