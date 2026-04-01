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

package org.apache.cassandra.index.sai;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SSTableContextManagerTest extends SAITester
{
    @Test
    public void shouldRefreshDescriptorWhenRequestedIndexContextsChanged()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, v1 int, v2 int)");

        String indexOnV1 = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String indexOnV2 = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        execute("INSERT INTO %s (id, v1, v2) VALUES ('k1', 1, 2)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull(group);

        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        Map<String, StorageAttachedIndex> indexesByName = group.getIndexes()
                                                             .stream()
                                                             .collect(Collectors.toMap(i -> i.getIndexMetadata().name, i -> i));

        StorageAttachedIndex v1 = indexesByName.get(indexOnV1);
        StorageAttachedIndex v2 = indexesByName.get(indexOnV2);

        assertNotNull(v1);
        assertNotNull(v2);

        assertTrue(IndexDescriptor.isIndexBuildCompleteOnDisk(sstable, v1.getIndexContext()));
        assertTrue(IndexDescriptor.isIndexBuildCompleteOnDisk(sstable, v2.getIndexContext()));

        // simulate startup
        SSTableContextManager manager = group.sstableContextManager();
        manager.clear();

        // register first index
        IndexDescriptor first = manager.getOrLoadIndexDescriptor(sstable, Set.of(v1));
        // register second index
        IndexDescriptor second = manager.getOrLoadIndexDescriptor(sstable, Set.of(v1, v2));

        assertSame(first, second); // re-used cached IndexDescriptor
        assertTrue(second.perIndexComponents(v1.getIndexContext()).isComplete());
        assertTrue(second.perIndexComponents(v2.getIndexContext()).isComplete()); // FAILED
    }
}
