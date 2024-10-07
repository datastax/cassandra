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
package org.apache.cassandra.index.sai.disk.format;

import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SSTableIndexComponentsStateTest
{
    @Test
    public void indexWasUpdatedTest()
    {
        // Note: the `indexWasUpdated` method is not used in C* at the time of this writing, but it is in CNDB. Those
        // tests both make sure it works as expected, but also avoid the method be marked "unused" by code editors.

        var base = SSTableIndexComponentsState.builder()
                                               .addPerSSTable(Version.DB, 0)
                                               .addPerIndex("index1", Version.DB, 0)
                                               .addPerIndex("index2", Version.DB, 0);

        // Additions are changes.
        assertTrue(base.copy().build().indexWasUpdated(SSTableIndexComponentsState.EMPTY, "index1"));
        assertTrue(base.copy().build().indexWasUpdated(SSTableIndexComponentsState.EMPTY, "index2"));

        // Modifying the per-sstable component is an update of all indexes.
        // version change:
        assertIndexUpdated(base, base.copy().addPerSSTable(Version.EB, 0), "index1");
        assertIndexUpdated(base, base.copy().addPerSSTable(Version.EB, 0), "index2");
        // generation change:
        assertIndexUpdated(base, base.copy().addPerSSTable(Version.DB, 1), "index1");
        assertIndexUpdated(base, base.copy().addPerSSTable(Version.DB, 1), "index2");

        // Modifying a per-index component only count as an update of that index.
        // version change:
        assertIndexUpdated(base, base.copy().addPerIndex("index1", Version.EB, 0), "index1");
        assertIndexNotUpdated(base, base.copy().addPerIndex("index1", Version.EB, 0), "index2");
        // generation change:
        assertIndexUpdated(base, base.copy().addPerIndex("index1", Version.DB, 1), "index1");
        assertIndexNotUpdated(base, base.copy().addPerIndex("index1", Version.DB, 1), "index2");

        // Same state means no change
        assertIndexNotUpdated(base, base, "index1");
        assertIndexNotUpdated(base, base, "index2");
    }

    private void assertIndexUpdated(SSTableIndexComponentsState.Builder before, SSTableIndexComponentsState.Builder after, String indexName)
    {
        assertTrue(after.copy().build().indexWasUpdated(before.copy().build(), indexName));
    }

    private void assertIndexNotUpdated(SSTableIndexComponentsState.Builder before, SSTableIndexComponentsState.Builder after, String indexName)
    {
        assertFalse(after.copy().build().indexWasUpdated(before.copy().build(), indexName));
    }

    @Test
    public void includedIndexTest()
    {
        assertEquals(SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.DB, 0)
                                                .addPerIndex("index1", Version.DB, 0)
                                                .addPerIndex("index2", Version.DB, 0)
                     .build()
                     .includedIndexes(),
                     Set.of("index1", "index2"));
    }
}