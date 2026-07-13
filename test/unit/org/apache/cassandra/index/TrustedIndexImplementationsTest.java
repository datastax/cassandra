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

package org.apache.cassandra.index;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.internal.CustomCassandraIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.IndexMetadata;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@code cassandra.trusted_index_implementations} system property, which allows referencing the listed
 * custom index implementations in {@code CREATE CUSTOM INDEX ... USING} by their simple class name.
 */
public class TrustedIndexImplementationsTest extends CQLTester
{
    @After
    public void restoreTrustedIndexImplementations()
    {
        IndexMetadata.loadTrustedIndexImplementations(null);
    }

    @Test
    public void shouldCreateTrustedIndexBySimpleName() throws Throwable
    {
        IndexMetadata.loadTrustedIndexImplementations(StubIndex.class.getName());

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StubIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'stubindex'");
        createIndex("CREATE CUSTOM INDEX idx3 ON %s(v3) USING 'STUBINDEX'");
        createIndex("CREATE CUSTOM INDEX idx4 ON %s(v4) USING 'org.apache.cassandra.index.StubIndex'");

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        for (String index : new String[]{ "idx1", "idx2", "idx3", "idx4" })
            assertTrue(indexManager.getIndexByName(index) instanceof StubIndex);
    }

    @Test
    public void shouldRejectSimpleNameWhenNotTrusted() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("Unable to find custom indexer class 'StubIndex'",
                                  ConfigurationException.class,
                                  "CREATE CUSTOM INDEX ON %s(v) USING 'StubIndex'");
    }

    @Test
    public void shouldParsePropertyValue()
    {
        // entries are trimmed, empty entries are skipped, aliases are case-insensitive
        IndexMetadata.loadTrustedIndexImplementations(format(" %s , , %s ", StubIndex.class.getName(), CustomCassandraIndex.class.getName()));
        assertTrue(IndexMetadata.isTrustedIndexImplementation("StubIndex"));
        assertTrue(IndexMetadata.isTrustedIndexImplementation("stubindex"));
        assertTrue(IndexMetadata.isTrustedIndexImplementation(StubIndex.class.getName()));
        assertTrue(IndexMetadata.isTrustedIndexImplementation("CustomCassandraIndex"));
        assertEquals(StubIndex.class.getName(), IndexMetadata.expandAliases("stubindex"));

        // reloading clears the previously registered implementations and aliases
        IndexMetadata.loadTrustedIndexImplementations(CustomCassandraIndex.class.getName());
        assertFalse(IndexMetadata.isTrustedIndexImplementation(StubIndex.class.getName()));
        assertEquals("stubindex", IndexMetadata.expandAliases("stubindex"));
        assertTrue(IndexMetadata.isTrustedIndexImplementation("CustomCassandraIndex"));

        // entries without a package name are ignored
        IndexMetadata.loadTrustedIndexImplementations("StubIndex");
        assertFalse(IndexMetadata.isTrustedIndexImplementation("StubIndex"));

        for (String value : new String[]{ null, "", " , " })
        {
            IndexMetadata.loadTrustedIndexImplementations(value);
            assertFalse(IndexMetadata.isTrustedIndexImplementation(StubIndex.class.getName()));
            assertFalse(IndexMetadata.isTrustedIndexImplementation(CustomCassandraIndex.class.getName()));
        }

        // the built-in StorageAttachedIndex aliases are unaffected, but SAI is not implicitly trusted
        assertEquals(StorageAttachedIndex.class.getName(), IndexMetadata.expandAliases("sai"));
        assertEquals(StorageAttachedIndex.class.getName(), IndexMetadata.expandAliases("StorageAttachedIndex"));
        assertFalse(IndexMetadata.isTrustedIndexImplementation(StorageAttachedIndex.class.getName()));
        assertFalse(IndexMetadata.isTrustedIndexImplementation("sai"));
    }
}
