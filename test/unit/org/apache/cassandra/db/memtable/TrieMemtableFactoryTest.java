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
package org.apache.cassandra.db.memtable;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdateStage2;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertSame;

public class TrieMemtableFactoryTest
{
    private final StorageCompatibilityMode originalMode = DatabaseDescriptor.getStorageCompatibilityMode();

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(originalMode);
    }

    /// Trie partition updates are only written in the trie encoding from VERSION_DS_21 on, which the storage
    /// compatibility modes that keep the storage version at VERSION_40 do not use.
    @Test
    public void testPartitionUpdateFactoryFollowsStorageVersion()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        assertSame(TriePartitionUpdateStage2.FACTORY, TrieMemtableFactory.INSTANCE.partitionUpdateFactory());

        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_4);
        assertSame(TriePartitionUpdateStage2.FACTORY, TrieMemtableFactory.INSTANCE.partitionUpdateFactory());

        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        assertSame(TriePartitionUpdate.FACTORY, TrieMemtableFactory.INSTANCE.partitionUpdateFactory());
    }
}
