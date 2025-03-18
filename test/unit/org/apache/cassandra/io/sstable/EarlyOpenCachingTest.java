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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddingNotification;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class EarlyOpenCachingTest extends CQLTester
{
    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object> generateParameters()
    {
        return Arrays.stream(SSTableFormat.Type.values()).collect(Collectors.toList());
    }

    @Parameterized.Parameter
    public SSTableFormat.Type format;

    @BeforeClass
    public static void setUpClass() // override CQLTester's setUpClass
    {
        DatabaseDescriptor.setDiskAccessMode(Config.DiskAccessMode.standard);
        CQLTester.setUpClass();
    }

    @Test
    public void testFinalOpenRetainsCachedData() throws InterruptedException
    {
        System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, format.name);
        createTable("CREATE TABLE %s (pkey text, ckey text, val blob, PRIMARY KEY (pkey, ckey))");

        for (int i = 0; i < 800; i++)
        {
            String pkey = getRandom().nextAsciiString(10, 10);
            for (int j = 0; j < 100; j++)
                execute("INSERT INTO %s (pkey, ckey, val) VALUES (?, ?, ?)", pkey, "" + j, ByteBuffer.allocate(1000));
        }
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        AtomicInteger opened = new AtomicInteger(0);
        AtomicBoolean completed = new AtomicBoolean(false);
        Phaser phaser = new Phaser(1);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader source = cfs.getLiveSSTables().iterator().next();

        INotificationConsumer consumer = (notification, sender) -> {
            System.out.println("Received notification: " + notification);
            if (!(notification instanceof SSTableAddingNotification) || completed.get())
                return;
            SSTableAddingNotification n = (SSTableAddingNotification) notification;
            SSTableReader s = n.adding.iterator().next();
            readAllAndVerifyKeySpan(s);
            assertTrue("Chunk cache is not used",
                       ChunkCache.instance.sizeOfFile(s.getDataFile()) > 0);
            phaser.register();
            opened.incrementAndGet();
            s.runOnClose(phaser::arriveAndDeregister);
        };

        SSTableReader finalReader;
        cfs.getTracker().subscribe(consumer);
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(source, OperationType.COMPACTION);
             SSTableRewriter writer = new SSTableRewriter(txn, 1000, 100L << 10, false))
        {
            writer.switchWriter(SSTableWriterTestBase.getWriter(format, cfs, cfs.getDirectories().getDirectoryForNewSSTables(), txn));
            var iter = source.getScanner();
            while (iter.hasNext())
            {
                var next = iter.next();
                writer.append(next);
            }
            completed.set(true);
            finalReader = writer.finish().iterator().next();
        }
        phaser.arriveAndAwaitAdvance();
        assertTrue("No early opening occured", opened.get() > 0);

        assertTrue("Chunk cache is not retained for early open sstable",
                   ChunkCache.instance.sizeOfFile(finalReader.getDataFile()) > 0);
        assertEquals(Sets.newHashSet(finalReader), cfs.getLiveSSTables());
        readAllAndVerifyKeySpan(finalReader);
    }

    private static void readAllAndVerifyKeySpan(SSTableReader s)
    {
        DecoratedKey firstKey = null;
        DecoratedKey lastKey = null;
        for (var iter = s.getScanner(); iter.hasNext(); )
        {
            var partition = iter.next();
            // consume all rows, so that the data is cached
            partition.forEachRemaining(column -> {
                // consume all columns
            });
            if (firstKey == null)
                firstKey = partition.partitionKey();
            lastKey = partition.partitionKey();
        }
        assertEquals("Simple scanner does not iterate all content", firstKey, s.first);
        assertEquals("Simple scanner does not iterate all content", lastKey, s.last);
    }
}
