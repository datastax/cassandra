/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.io.sstable;

import java.util.Set;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class StorageHandlerTest
{
    private static final String KS = "StorageHandlerTest";
    private static final String TABLE = "testTable";

    private static ColumnFamilyStore store;

    private static volatile OnOpeningWrittenSSTableFailure onOpeningWrittenSSTableFailureInterceptor;

    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.REMOTE_STORAGE_HANDLER_FACTORY.setString(TestStorageHandlerFactory.class.getName());

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KS,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS, TABLE));

        store = Keyspace.open(KS).getColumnFamilyStore(TABLE);
    }

    @After
    public void afterTest()
    {
        store.truncateBlocking();
    }

    private void addDataAndFlush(ColumnFamilyStore cfs, int numKeys)
    {
        for (int i = 0; i < numKeys; i ++) {
            new RowUpdateBuilder(cfs.metadata(), i, String.valueOf(i))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
    }

    @Test
    @BMRules(rules = {
        @BMRule(name = "Fail opening reader",
                targetClass = "org.apache.cassandra.io.sstable.format.SSTableWriter$TransactionalProxy",
                targetMethod = "openResultInternal",
                targetLocation = "AT INVOKE openFinal",
                action = "throw new RuntimeException(\"Problem reading\")")
        }
    )
    public void testOnOpeningWrittenSSTableFailure()
    {
        int numKeys = 10;

        onOpeningWrittenSSTableFailureInterceptor = (reason, descriptor, components, compressedSize, uncompressedSize, stats, firstKey, lastKey, estimatedKeys, throwable) -> {
            assertEquals(SSTableReader.OpenReason.NORMAL, reason);
            assertEquals(TABLE, descriptor.cfname);
            assertEquals("Problem reading", throwable.getMessage());
            assertTrue(compressedSize > 0);
            assertTrue(uncompressedSize > 0);
            // Note that the update uses the iteration count sa timestamp.
            assertEquals(0, stats.minTimestamp);
            assertEquals(numKeys - 1, stats.maxTimestamp);
            // Note that tests uses the byter-order partitioner.
            assertEquals("0", ByteBufferUtil.string(firstKey.getKey()));
            assertEquals("9", ByteBufferUtil.string(lastKey.getKey()));
        };

        assertThrows("Problem reading", RuntimeException.class, () -> addDataAndFlush(store, numKeys));
    }

    private static class TestStorageHandler extends DefaultStorageHandler
    {
        private TestStorageHandler(SSTable.Owner owner, TableMetadataRef metadata, Directories directories, Tracker dataTracker)
        {
            super(owner, metadata, directories, dataTracker);
        }

        @Override
        public SSTableReader onOpeningWrittenSSTableFailure(SSTableReader.OpenReason reason, Descriptor descriptor, Set<Component> components, long compressedSize, long uncompressedSize, StatsMetadata stats, DecoratedKey firstKey, DecoratedKey lastKey, long estimatedKeys, Throwable throwable)
        {
            if (onOpeningWrittenSSTableFailureInterceptor != null)
            {
                try
                {
                    onOpeningWrittenSSTableFailureInterceptor.call(reason, descriptor, components, compressedSize, uncompressedSize, stats, firstKey, lastKey, estimatedKeys, throwable);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
            return super.onOpeningWrittenSSTableFailure(reason, descriptor, components, compressedSize, uncompressedSize, stats, firstKey, lastKey, estimatedKeys, throwable);
        }
    }

    @FunctionalInterface
    private interface OnOpeningWrittenSSTableFailure
    {
        void call(SSTableReader.OpenReason reason, Descriptor descriptor, Set<Component> components, long compressedSize, long uncompressedSize, StatsMetadata stats, DecoratedKey firstKey, DecoratedKey lastKey, long estimatedKeys, Throwable throwable) throws Exception;
    }

    public static class TestStorageHandlerFactory implements StorageHandlerFactory
    {
        @Override
        public StorageHandler create(SSTable.Owner owner, TableMetadataRef metadata, Directories directories, Tracker dataTracker)
        {
            return new TestStorageHandler(owner, metadata, directories, dataTracker);
        }
    }
}
